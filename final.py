import requests
import sqlite3
import os
import json
from bs4 import BeautifulSoup
import re
import matplotlib.pyplot as plt


# poverty census link: https://www.census.gov/data/developers/data-sets/Poverty-Statistics.html
# health insurance link: https://www.census.gov/data/developers/data-sets/Health-Insurance-Statistics.html

API_KEY = "2ccfec65f3d7e712756b848688b689eacd4e0282"
##Covid DB------------------------------------------##

def get_covid_data():
    covid_url = "https://disease.sh/v3/covid-19/states"

    try:
        response = requests.get(covid_url)
        data = response.json()

        return data  
    except:
        print("FAILED to fetch live COVID data.")
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
                state, entry['cases'], entry['deaths'], entry['population'], entry.get('tests', 0), party_id
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

    # Get already inserted state codes
    cur.execute("SELECT state_code FROM states")
    inserted_codes = set(row[0] for row in cur.fetchall())

    inserted_count = 0
    for row in data[1:]:  # Skip header
        if inserted_count >= 25:
            break

        try:
            state_code = int(row[7])  # FIPS code
            if state_code in inserted_codes:
                continue

            state_name = row[0]
            poverty_population = int(row[1])
            poverty_universe = int(row[2])
            median_income = int(row[3])
            total_25plus = int(row[4])
            hs_grads = int(row[5])
            bachelors = int(row[6])
        except ValueError:
            continue

        # Insert into states
        cur.execute('''
            INSERT OR IGNORE INTO states (state_code, state_name)
            VALUES (?, ?)
        ''', (state_code, state_name))

        # Insert into poverty_stats
        cur.execute('''
            INSERT OR REPLACE INTO poverty_stats (
                state_code, poverty_population, poverty_universe, median_income,
                total_25plus, hs_grads, bachelors
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
        ''', (
            state_code, poverty_population, poverty_universe,
            median_income, total_25plus, hs_grads, bachelors
        ))

        inserted_codes.add(state_code)  # Track that it’s now inserted
        inserted_count += 1

    conn.commit()


def get_joined_data(cur):
    query = '''
        SELECT 
            s.state_name, 
            d.cases, 
            d.deaths, 
            p.median_income,
            pr.party_name
        FROM state_data d
        JOIN states s ON d.state = s.state_name
        JOIN poverty_stats p ON s.state_code = p.state_code
        JOIN parties pr ON d.party_id = pr.party_id
    '''
    cur.execute(query)
    return cur.fetchall()

def write_results_to_file(joined_data):
    total_cases = 0
    total_states = 0
    with open("results.txt", "w") as f:
        for row in joined_data:
            state, cases, deaths, income, party = row
            f.write(f"{state}: {cases} cases, {deaths} deaths, ${income} median income, {party}\n")
            total_cases += cases
            total_states += 1

        if total_states > 0:
            avg = total_cases / total_states
            f.write(f"\n\nAverage COVID cases across states: {avg:.2f}")


def plot_death_rate_by_party(cur):
    
    cur.execute('''
        SELECT parties.party_name, SUM(state_data.deaths), SUM(state_data.population)
        FROM state_data
        JOIN parties ON state_data.party_id = parties.party_id
        GROUP BY parties.party_name
    ''')
    data = cur.fetchall()

    parties = []
    death_rates = []

    for row in data:
        party = row[0]
        total_deaths = row[1]
        total_population = row[2]
        death_rate = (total_deaths / total_population) * 100000  # per 100k

        parties.append(party)
        death_rates.append(death_rate)

    plt.figure(figsize=(8, 6))
    plt.barh(parties, death_rates, color='pink')
    plt.title("COVID Death Rate by Party (per 100k)")
    plt.ylabel("Political Party")  # Now on the y-axis
    plt.xlabel("Deaths per 100,000 People")
    plt.tight_layout()
    plt.savefig("covid_death_rate_by_party.png")
    plt.close()



def plot_avg_case_rate_by_party(cur):
    
    cur.execute('''
        SELECT parties.party_name, state_data.cases, state_data.population
        FROM state_data
        JOIN parties ON state_data.party_id = parties.party_id
    ''')
    data = cur.fetchall()
    

    party_totals = {}
    for party, cases, pop in data:
        if party not in party_totals:
            party_totals[party] = {"cases": 0, "pop": 0}
        party_totals[party]["cases"] += cases
        party_totals[party]["pop"] += pop

    parties = []
    case_rates = []
    for party, values in party_totals.items():
        rate = (values["cases"] / values["pop"]) * 100000
        parties.append(party)
        case_rates.append(rate)

    plt.figure(figsize=(8, 6))
    plt.bar(parties, case_rates, color='pink')
    plt.title("Average COVID Case Rate by Party (per 100k)")
    plt.xlabel("Political Party")
    plt.ylabel("Cases per 100,000 People")
    plt.tight_layout()
    plt.savefig("avg_case_rate_by_party.png")
    plt.close()



def plot_income_vs_case_rate(cur):
    query = '''
        SELECT ps.median_income, sd.cases, sd.population
        FROM state_data sd
        JOIN states s ON sd.state = s.state_name
        JOIN poverty_stats ps ON s.state_code = ps.state_code
    '''
    cur.execute(query)
    data = cur.fetchall()

    incomes = []
    case_rates = []
    for row in data:
        income, cases, pop = row
        if pop > 0:
            incomes.append(income)
            case_rates.append((cases / pop) * 100000)

    plt.scatter(incomes, case_rates, color='purple')
    plt.title("Median Income vs. COVID Case Rate (per 100k)")
    plt.xlabel("Median Income ($)")
    plt.ylabel("COVID Cases per 100,000 People")
    plt.tight_layout()
    plt.savefig("income_vs_case_rate.png")
    plt.close()

def plot_education_vs_case_rate(cur):
    query = '''
        SELECT 
            ps.bachelors * 1.0 / ps.total_25plus AS bachelors_rate,
            sd.cases * 100000.0 / sd.population AS case_rate
        FROM state_data sd
        JOIN states s ON sd.state = s.state_name
        JOIN poverty_stats ps ON s.state_code = ps.state_code
        WHERE ps.total_25plus > 0 AND sd.population > 0
    '''
    cur.execute(query)
    data = cur.fetchall()

    bachelors_percent = []
    case_rate = []

    for row in data:
        bachelors_rate = row[0] * 100  # Convert to percentage
        covid_case_rate = row[1]       # Already per 100k

        bachelors_percent.append(bachelors_rate)
        case_rate.append(covid_case_rate)

    plt.figure(figsize=(9, 6))
    plt.scatter(bachelors_percent, case_rate, alpha=0.7, color='black')
    plt.title("% Bachelor's Degree vs. COVID Case Rate")
    plt.xlabel("% of Adults with Bachelor's Degree")
    plt.ylabel("COVID Cases per 100,000")
    plt.grid(True)
    plt.tight_layout()
    plt.savefig("education_vs_case_rate.png")
    plt.close()










# NAME
# B17001_002E: People BELOW poverty
# B17001_001E: Total people in poverty universe
# B19013_001E: Median household income
# B15003_001E: Total population age 25+
# B15003_017E: High school grads (including equivalency)
# B15003_022E: Bachelor's degree holders
# for=state:06 (California's FIPS code)

def main():
    db_name = "covid_db2.db"
    covid_data = get_covid_data()
    get_poverty_data()
    poverty_dict = convert_poverty_to_dict()
    election_dict = get_state_election_results()
    cur, conn = set_up_covid_database(db_name)
    create_parties_table(cur, conn) 
    create_combined_table(cur, conn)
    create_split_tables(cur, conn)
    insert_split_poverty_data(cur, conn)
    if covid_data:
        insert_combined_data(covid_data, election_dict, cur, conn)
        print("All data successfully added to combined table.")
    else:
        print("Failed to load COVID data.")

    joined_data = get_joined_data(cur)
    write_results_to_file(joined_data)
    plot_death_rate_by_party(cur)
    plot_avg_case_rate_by_party(cur)
    plot_income_vs_case_rate(cur)
    plot_education_vs_case_rate(cur)
    
    
    
    conn.close()
if __name__ == "__main__":
    main()