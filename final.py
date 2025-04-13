import requests
import sqlite3
import os
import json
from bs4 import BeautifulSoup
import re

# poverty census link: https://www.census.gov/data/developers/data-sets/Poverty-Statistics.html
# health insurance link: https://www.census.gov/data/developers/data-sets/Health-Insurance-Statistics.html

API_KEY = "2ccfec65f3d7e712756b848688b689eacd4e0282"
##Covid DB------------------------------------------##

def get_covid_data():
    covid_url = "https://disease.sh/v3/covid-19/states"
    try:
        response = requests.get(covid_url)
        
        data = response.json()
        with open("covid_data.json", "w") as json_file:
            json.dump(data, json_file, indent=4)
        return data, response.url
        
    except:
        return None



def set_up_covid_database(db_name):
    path = os.path.dirname(os.path.abspath(__file__))
    conn = sqlite3.connect(os.path.join(path, db_name))
    cur = conn.cursor()
    return cur, conn


get_covid_data()#creates json file

set_up_covid_database("covid_db.db")

def create_covid_table(data, cur, conn):

    cur.execute('''
        CREATE TABLE IF NOT EXISTS covid_data (
            state TEXT,
            cases INTEGER,
            deaths INTEGER,
            population INTEGER,
            tests INTEGER
        )
    ''')
    for state in data:
        state_name = state['state']
        cases = state['cases']
        deaths = state['deaths']
        population = state['population']
        tests = state['tests']
        cur.execute('''
            INSERT INTO covid_data (state, cases, deaths, population, tests)
            VALUES (?, ?, ?, ?, ?)
        ''', (state_name, cases, deaths, population, tests))
    conn.commit()


def load_data_and_insert_into_db():
    data = get_covid_data()
    if data:
        cur, conn = set_up_covid_database("covid_db.db")
        create_covid_table(data, cur, conn)
        conn.close()
        #print("It worked") #debugging

load_data_and_insert_into_db()#puts data into db

##Poverty DB----------------------------------------##

def get_poverty_data():
    url = "https://api.census.gov/data/2021/acs/acs1"
    params = {
        "get": "NAME,B17001_002E,B17001_001E,B19013_001E,B15003_001E,B15003_017E,B15003_022E",
        "for": "state:*",
        "key": API_KEY
    }
    try:
        response = requests.get(url, params=params)
        
        data = response.json()
        with open("poverty_data.json", "w") as f:
            json.dump(data, f, indent=4)
            print("done")
            
        return data
        
    except requests.RequestException as e:
        print("Request exception:", e)
        return None
    

    
get_poverty_data()

def setuppovertydatabase(db_name): #same as covid one. 
    path = os.path.dirname(os.path.abspath(__file__))
    conn = sqlite3.connect(os.path.join(path, db_name))
    cur = conn.cursor()
    return cur, conn


setuppovertydatabase("poverty_db.db")


def createpovertytable(cur, conn):
    #split, easier with no dictionary.
    # creates the table.  
    #change column names at some point(remeber). 
    cur.execute('''
        CREATE TABLE IF NOT EXISTS poverty_data (
            state_name TEXT,
            B17001_002E INTEGER, 
            B17001_001E INTEGER,
            B19013_001E INTEGER,
            B15003_001E INTEGER,
            B15003_017E INTEGER,
            B15003_022E INTEGER,
            state_code TEXT
        )
    ''')
    conn.commit()

cur, conn = setuppovertydatabase("poverty_db.db")
createpovertytable(cur, conn)

def loadpovertydata():
    #load the data from the json file.
    with open("poverty_data.json", "r") as file:
        data = json.load(file)
       
    for row in data[1:]: #to skip the first one. 
        cur.execute('''
                INSERT INTO poverty_data (
                    state_name, 
                    B17001_002E,
                    B17001_001E,
                    B19013_001E,
                    B15003_001E,
                    B15003_017E,
                    B15003_022E,
                    state_code
                ) 
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ''', ( 
                row[0], 
                row[1], 
                row[2], 
                row[3],
                row[4], 
                row[5], 
                row[6], 
                row[7]
            ))
        
    conn.commit()
    conn.close()

loadpovertydata()

def get_state_election_results():
   url = "https://en.wikipedia.org/wiki/2020_United_States_presidential_election"
   response = requests.get(url)
   soup = BeautifulSoup(response.content, "html.parser")


   # Find the main results table (it's the one with the classes below)
   state_party = {}
   target_div = soup.find_all("div", attrs={"style": "overflow:auto"})
   tbody = target_div[0].find("tbody")
   state_color = tbody
   election_list_r = tbody.find_all("tr", style = "background-color:#FFB6B6")
   election_list_d = tbody.find_all("tr", style = "color:black;background-color:#B0CEFF")
   for row in election_list_r + election_list_d:
    cells = row.find_all("td")
    if len(cells) > 0:
        state = re.sub(r"\[.*?\]|\W+$", "", cells[0].get_text(strip=True))
        biden_votes = int(cells[1].get_text(strip=True).replace(",", ""))
        trump_votes = int(cells[4].get_text(strip=True).replace(",", ""))
        
        if biden_votes > trump_votes:
            state_party[state] = "Democratic"
        elif trump_votes > biden_votes:
            state_party[state] = "Republican"
        else:
            state_party[state] = "Neither"

   return state_party

def setup_party_database(db_name):
    path = os.path.dirname(os.path.abspath(__file__))
    conn = sqlite3.connect(os.path.join(path, db_name))
    cur = conn.cursor()
    
    cur.execute('''
        CREATE TABLE IF NOT EXISTS state_parties (
            state TEXT PRIMARY KEY,
            party TEXT
        )
    ''')
    
    return cur, conn
def insert_state_parties(state_party, cur, conn):
    for state, party in state_party.items():
        cur.execute('''
            INSERT OR REPLACE INTO state_parties (state, party)
            VALUES (?, ?)
        ''', (state, party))
    conn.commit()

state_party = get_state_election_results()

cur, conn = setup_party_database("covid_db.db")
insert_state_parties(state_party, cur, conn)
conn.close()
print("Election data successfully added to the database.")



# NAME
# B17001_002E: People BELOW poverty
# B17001_001E: Total people in poverty universe
# B19013_001E: Median household income
# B15003_001E: Total population age 25+
# B15003_017E: High school grads (including equivalency)
# B15003_022E: Bachelor's degree holders
# for=state:06 (California's FIPS code)