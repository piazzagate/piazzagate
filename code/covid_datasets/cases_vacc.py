from datetime import datetime
from pathlib import Path
import pandas as pd 
import sqlite3
from sodapy import Socrata


# ---------------------------------------------------------------------- #
#                        COVID CASES BY COUNTY                           #
# ---------------------------------------------------------------------- #


# Read csv file from nytimes dataset, number of cases are ordered by date
cases_data = pd.read_csv('https://raw.githubusercontent.com/nytimes/covid-19-data/master/us-counties.csv')

# Get total number of cases and deaths up to July 2nd
total_cases_data = cases_data[cases_data["date"] == "2021-07-02"].drop("date", 1)


DATA_DIR = Path(__file__).parent.parent.parent / 'data'

# Create connection to database
conn = sqlite3.connect(DATA_DIR / 'processed' / 'covid_data.db')
c = conn.cursor()


# Delete tables if they exist
c.execute('DROP TABLE IF EXISTS "cases_by_date";')
c.execute('DROP TABLE IF EXISTS "total_cases";')


cases_by_date = """
    CREATE TABLE cases_by_date (
        date TEXT, 
        county TEXT, 
        state TEXT, 
        fips INT, 
        cases INT, 
        deaths INT
    );
"""

total_cases = """
    CREATE TABLE total_cases (  
        county TEXT, 
        state TEXT, 
        fips INT, 
        cases INT, 
        deaths INT
    );
"""


c.execute(cases_by_date)
c.execute(total_cases)

conn.commit()

# Convert dataframe to sql table
cases_data.to_sql('cases_by_date', conn, if_exists='append', index=False)
total_cases_data.to_sql('total_cases', conn, if_exists='append', index=False)

conn.commit()



# ---------------------------------------------------------------------- #
#                     COVID VACCINATIONS BY COUNTY                       #
# ---------------------------------------------------------------------- #


# Access the CDC's dataset using Socrata Open Data API
client = Socrata("data.cdc.gov",
                 'IIeauhR40yPl8zyK53tOPvlwu')

# Results returned as JSON from API / converted to Python list of dictionaries by sodapy.
results = client.get("8xkx-amqh", limit = 700000)

# Convert to pandas DataFrame, and select needed columns
vaccinations_data = pd.DataFrame.from_records(results)[["date", "fips", "recip_county", "recip_state", "series_complete_yes", "series_complete_pop_pct"]]

# Remove time from date
vaccinations_data["date"] = vaccinations_data["date"].apply(lambda x: x[:10])

# Get total vaccinations up to July 2nd
total_vaccinations_data = vaccinations_data[vaccinations_data["date"] == "2021-07-02"].drop("date", 1)


# Delete table if it already exists
c.execute('DROP TABLE IF EXISTS "county_vaccinations";')
c.execute('DROP TABLE IF EXISTS "total_vaccinations";')


county_vaccinations = """
    CREATE TABLE county_vaccinations (
        date TEXT, 
        fips INT, 
        recip_county TEXT, 
        recip_state TEXT, 
        series_complete_yes INT,
        series_complete_pop_pct FLOAT
    );
"""

total_vaccinations = """
    CREATE TABLE total_vaccinations (
        fips INT, 
        recip_county TEXT, 
        recip_state TEXT, 
        series_complete_yes INT,
        series_complete_pop_pct FLOAT
    );
"""

c.execute(county_vaccinations)
c.execute(total_vaccinations)

conn.commit()

vaccinations_data.to_sql('county_vaccinations', conn, if_exists='append', index=False)
total_vaccinations_data.to_sql('total_vaccinations', conn, if_exists='append', index=False)

conn.commit()


