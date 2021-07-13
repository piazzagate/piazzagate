from pathlib import Path
import pandas as pd
import sqlite3
from sodapy import Socrata


# ---------------------------------------------------------------------- #
#                        COVID CASES BY COUNTY                           #
# ---------------------------------------------------------------------- #


# Read csv file from nytimes dataset, number of cases are ordered by date
cases_data = pd.read_csv(
    'https://raw.githubusercontent.com/nytimes/covid-19-data/master/us-counties.csv')

# Get total number of cases and deaths up to July 2nd

DATA_DIR = Path(__file__).parent.parent.parent / 'data'

# Create connection to database
conn = sqlite3.connect(DATA_DIR / 'processed' / 'covid_data.db')
c = conn.cursor()


# Delete table if it already exists
c.execute('DROP TABLE IF EXISTS "cases";')


cases_table = """
    CREATE TABLE cases (
        date TEXT, 
        county TEXT, 
        state TEXT, 
        fips INT, 
        cases INT, 
        deaths INT
    );
"""

c.execute(cases_table)
conn.commit()

# Convert dataframe to sql table
cases_data.to_sql('cases', conn, if_exists='append', index=False)
conn.commit()


# ---------------------------------------------------------------------- #
#                     COVID VACCINATIONS BY COUNTY                       #
# ---------------------------------------------------------------------- #


# Access the CDC's dataset using Socrata Open Data API
client = Socrata("data.cdc.gov",
                 'IIeauhR40yPl8zyK53tOPvlwu')

# Results returned as JSON from API / converted to Python list of dictionaries by sodapy.
results = client.get("8xkx-amqh", limit=700000)

# Convert to pandas DataFrame, and select needed columns
vaccinations_data = pd.DataFrame.from_records(results)[
    ["date", "fips", "recip_county", "recip_state", "series_complete_yes", "series_complete_pop_pct"]]

# Remove time from date
vaccinations_data["date"] = vaccinations_data["date"].apply(lambda x: x[:10])


# Delete table if it already exists
c.execute('DROP TABLE IF EXISTS "vaccinations";')


vaccinations_table = """
    CREATE TABLE vaccinations (
        date TEXT, 
        fips INT NOT NULL, 
        recip_county TEXT, 
        recip_state TEXT, 
        series_complete_yes INT,
        series_complete_pop_pct FLOAT
    );
"""

c.execute(vaccinations_table)
conn.commit()

vaccinations_data.to_sql('vaccinations', conn, if_exists='append', index=False)
conn.commit()

c.execute('DROP TABLE IF EXISTS "covid";')

# Combine cases and vaccinations
covid_table = """
    CREATE TABLE covid AS
        SELECT c.date, c.fips, c.cases, c.deaths, v.series_complete_yes, v.series_complete_pop_pct
        FROM cases AS c
        LEFT OUTER JOIN vaccinations AS v
        ON c.date = v.date AND c.fips = v.fips 
        WHERE c.fips IS NOT NULL;"""
c.execute(covid_table)
conn.commit()


# Replace vaccinations' null values with 0
replace_nulls = """
    UPDATE covid 
        SET
            series_complete_yes = COALESCE(series_complete_yes, 0),
            series_complete_pop_pct = COALESCE(series_complete_pop_pct, 0)"""

c.execute(replace_nulls)
c.execute('DROP TABLE IF EXISTS "cases";')
c.execute('DROP TABLE IF EXISTS "vaccinations";')

conn.commit()
conn.close()
