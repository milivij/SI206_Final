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
    try:
        with open("covid_data.json", "r") as json_file:
            data = json.load(json_file)
            print("Loaded COVID data from local file.")
            return data
    except:
        print("FAILED")
        return None

def get_poverty_data():
    url = "https://api.census.gov/data/2021/acs/acs1"
    params = {
        "get": "NAME,B17001_002E,B17001_001E,B19013_001E,B15003_001E,B15003_017E,B15003_022E",
        "for": "state:*",
        "key": API_KEY
    }
    
    response = requests.get(url, params=params)
    
    data = response.json()
    with open("poverty_data.json", "w") as f:
        json.dump(data, f, indent=4)
        print("done")
        
    return data
        
    
    
def get_state_election_results():
   url = "https://en.wikipedia.org/wiki/2020_United_States_presidential_election"
   response = requests.get(url)
   soup = BeautifulSoup(response.content, "html.parser")

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

def convert_poverty_to_dict():
    with open("poverty_data.json", "r") as file:
        data = json.load(file)[1:]
        result = {}
        for row in data:
            state = row[0]
            result[state] = {
                'poverty_population': int(row[1]),
                'poverty_universe': int(row[2]),
                'median_income': int(row[3]),
                'total_25plus': int(row[4]),
                'hs_grads': int(row[5]),
                'bachelors': int(row[6])
            }
        return result
    
def set_up_covid_database(db_name):
    path = os.path.dirname(os.path.abspath(__file__))
    conn = sqlite3.connect(os.path.join(path, db_name))
    cur = conn.cursor()
    return cur, conn

    
def create_combined_table(cur, conn):
    cur.execute('''
        CREATE TABLE IF NOT EXISTS state_data (
            state TEXT PRIMARY KEY,
            cases INTEGER,
            deaths INTEGER,
            population INTEGER,
            tests INTEGER,
            party_id INTEGER,
            FOREIGN KEY (party_id) REFERENCES parties(party_id)
        )
    ''')
    conn.commit()

def create_parties_table(cur, conn):
    cur.execute('''
        CREATE TABLE IF NOT EXISTS parties (
            party_id INTEGER PRIMARY KEY AUTOINCREMENT,
            party_name TEXT UNIQUE
        )
    ''')
    conn.commit()

def get_or_create_party_id(party_name, cur):
    cur.execute("SELECT party_id FROM parties WHERE party_name = ?", (party_name,))
    result = cur.fetchone()
    if result:
        return result[0]
    else:
        cur.execute("INSERT INTO parties (party_name) VALUES (?)", (party_name,))
        return cur.lastrowid




def insert_combined_data(covid_data, election_dict, cur, conn):
    cur.execute("SELECT state FROM state_data")
    inserted_states = set(row[0] for row in cur.fetchall())

    inserted_count = 0
    for entry in covid_data:
        if inserted_count >= 25:
            break

        state = entry['state'].strip()
        party_name = election_dict.get(state)

        if party_name and state not in inserted_states:
            party_id = get_or_create_party_id(party_name, cur)
            cur.execute('''
                INSERT OR IGNORE INTO state_data (
                    state, cases, deaths, population, tests, party_id
                ) VALUES (?, ?, ?, ?, ?, ?)
            ''', (
                state, entry['cases'], entry['deaths'], entry['population'], entry['tests'], party_id
            ))
            inserted_count += 1

    conn.commit()


def create_split_tables(cur, conn):
    cur.execute('''
        CREATE TABLE IF NOT EXISTS states (
            state_code INTEGER PRIMARY KEY,
            state_name TEXT
        )
    ''')

    cur.execute('''
        CREATE TABLE IF NOT EXISTS poverty_stats (
            state_code INTEGER PRIMARY KEY,
            poverty_population INTEGER,
            poverty_universe INTEGER,
            median_income INTEGER,
            total_25plus INTEGER,
            hs_grads INTEGER,
            bachelors INTEGER,
            FOREIGN KEY (state_code) REFERENCES states(state_code)
        )
    ''')

    conn.commit()

def insert_split_poverty_data(cur, conn):
    with open("poverty_data.json", "r") as file:
        data = json.load(file)

    for row in data[1:]:  # Skip header
        state_name = row[0]
        try:
            state_code = int(row[7])  # FIPS code
            poverty_population = int(row[1])
            poverty_universe = int(row[2])
            median_income = int(row[3])
            total_25plus = int(row[4])
            hs_grads = int(row[5])
            bachelors = int(row[6])
        except ValueError:
            continue  # Skip rows with invalid data

        # Insert into `states` table
        cur.execute('''
            INSERT OR IGNORE INTO states (state_code, state_name)
            VALUES (?, ?)
        ''', (state_code, state_name))

        # Insert into `poverty_stats` table
        cur.execute('''
            INSERT OR REPLACE INTO poverty_stats (
                state_code, poverty_population, poverty_universe, median_income,
                total_25plus, hs_grads, bachelors
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
        ''', (
            state_code, poverty_population, poverty_universe,
            median_income, total_25plus, hs_grads, bachelors
        ))

    conn.commit()







# NAME
# B17001_002E: People BELOW poverty
# B17001_001E: Total people in poverty universe
# B19013_001E: Median household income
# B15003_001E: Total population age 25+
# B15003_017E: High school grads (including equivalency)
# B15003_022E: Bachelor's degree holders
# for=state:06 (California's FIPS code)

def main():
    covid_data = get_covid_data()
    get_poverty_data()
    poverty_dict = convert_poverty_to_dict()
    election_dict = get_state_election_results()
    cur, conn = set_up_covid_database("covid_db.db")
    create_parties_table(cur, conn) 
    create_combined_table(cur, conn)
    create_split_tables(cur, conn)
    insert_split_poverty_data(cur, conn)
    if covid_data:
        insert_combined_data(covid_data, election_dict, cur, conn)
        print("All data successfully added to combined table.")
    else:
        print("Failed to load COVID data.")
    conn.close()
if __name__ == "__main__":
    main()