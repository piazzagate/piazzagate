from pathlib import Path
import pandas as pd
import tqdm
import json
import sqlite3

DATA_DIR = Path(__file__).parent.parent.parent / 'data'

conn = sqlite3.connect(DATA_DIR / 'processed' / 'covid_tweets.db')
c = conn.cursor()
c.execute('DROP TABLE IF EXISTS "counties";')
c.execute('DROP TABLE IF EXISTS "geopoints";')
c.execute('DROP TABLE IF EXISTS "boundaries";')

counties_table_cmd = """
    CREATE TABLE counties (
        fips INT PRIMARY KEY NOT NULL,
        statefp INT,
        countyfp INT,
        countyns INT,
        namelsad TEXT
    );
"""
geopoints_table_cmd = """
    CREATE TABLE geopoints (
        fips INT NOT NULL,
        lat FLOAT,
        long FLOAT,
        FOREIGN KEY(fips) REFERENCES counties(fips)
    );
"""
boundaries_table_cmd = """
    CREATE TABLE boundaries (
        fips INT NOT NULL,
        lat FLOAT,
        long FLOAT,
        FOREIGN KEY(fips) REFERENCES counties(fips)
    );
"""
c.execute(counties_table_cmd)
c.execute(geopoints_table_cmd)
c.execute(boundaries_table_cmd)
conn.commit()

# data downloaded from https://public.opendatasoft.com/explore/dataset/us-county-boundaries/export/?disjunctive.statefp&disjunctive.countyfp&disjunctive.name&disjunctive.namelsad&disjunctive.stusab&disjunctive.state_name&dataChart=eyJxdWVyaWVzIjpbeyJjb25maWciOnsiZGF0YXNldCI6InVzLWNvdW50eS1ib3VuZGFyaWVzIiwib3B0aW9ucyI6eyJkaXNqdW5jdGl2ZS5zdGF0ZWZwIjp0cnVlLCJkaXNqdW5jdGl2ZS5jb3VudHlmcCI6dHJ1ZSwiZGlzanVuY3RpdmUubmFtZSI6dHJ1ZSwiZGlzanVuY3RpdmUubmFtZWxzYWQiOnRydWUsImRpc2p1bmN0aXZlLnN0dXNhYiI6dHJ1ZSwiZGlzanVuY3RpdmUuc3RhdGVfbmFtZSI6dHJ1ZX19LCJjaGFydHMiOlt7ImFsaWduTW9udGgiOnRydWUsInR5cGUiOiJjb2x1bW4iLCJmdW5jIjoiQVZHIiwieUF4aXMiOiJhbGFuZCIsInNjaWVudGlmaWNEaXNwbGF5Ijp0cnVlLCJjb2xvciI6IiNGRjUxNUEifV0sInhBeGlzIjoic3RhdGVmcCIsIm1heHBvaW50cyI6NTAsInNvcnQiOiIifV0sInRpbWVzY2FsZSI6IiIsImRpc3BsYXlMZWdlbmQiOnRydWUsImFsaWduTW9udGgiOnRydWV9&location=2,40.71396,40.07813&basemap=jawg.streets
# geoid and countyns are unique identifiers
county_coords = pd.read_csv(DATA_DIR / 'raw' / 'counties' / 'us-county-boundaries.csv', sep=';')

for i, county in tqdm.tqdm(county_coords.iterrows()):
    fips = county['GEOID']
    statefp = county['STATEFP']
    countyfp = county['COUNTYFP']
    countyns = county['COUNTYNS']
    namelsad = county['NAMELSAD']

    coords = json.loads('[' + county['Geo Point'] + ']')
    lat = coords[0]
    long = coords[1]

    boundary = json.loads(county['Geo Shape'])['coordinates'][0]

    c.execute('INSERT INTO counties VALUES (?, ?, ?, ?, ?);', (fips, statefp, countyfp, countyns, namelsad))

    c.execute('INSERT INTO geopoints VALUES (?, ?, ?);', (fips, lat, long))
    
    for coords in boundary:
        if type(coords[0]) == list:
            for coords_ in coords:
                c.execute('INSERT INTO boundaries VALUES (?, ?, ?);', (fips, coords_[0], coords_[1]))
            continue
        c.execute('INSERT INTO boundaries VALUES (?, ?, ?);', (fips, coords[0], coords[1]))
    
    conn.commit()