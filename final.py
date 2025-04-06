import requests
import sqlite3
import os
import json

# poverty census link: https://www.census.gov/data/developers/data-sets/Poverty-Statistics.html
# health insurance link: https://www.census.gov/data/developers/data-sets/Health-Insurance-Statistics.html

API_KEY = "fc91d6eb0800a5c0ce59040f6df63647f2929dde"
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
    try:
        response = requests.get("https://api.census.gov/data/2023/cps/asec/mar/variables.json", params = {'apikey': API_KEY})
       
        
        if response.status_code == 200: 
            thedata = response.json()

            with open("poverty_data.json", "w") as json_file:
                    json.dump(thedata, json_file, indent=4)

            return (thedata, response.url)
        else:   
            return None 
    
    except requests.RequestException:
        return None
    
get_poverty_data()


