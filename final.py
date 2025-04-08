import requests
import sqlite3
import os
import json

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
    data, _ = get_covid_data()
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


# NAME
# B17001_002E: People BELOW poverty
# B17001_001E: Total people in poverty universe
# B19013_001E: Median household income
# B15003_001E: Total population age 25+
# B15003_017E: High school grads (including equivalency)
# B15003_022E: Bachelor's degree holders
# for=state:06 (California's FIPS code)