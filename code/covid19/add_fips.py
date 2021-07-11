from pathlib import Path
import pandas as pd 
import sqlite3


DATA_DIR = Path(__file__).parent.parent.parent / 'data'

df = pd.read_csv(DATA_DIR / 'raw' / 'covid19' / 'ZIP-COUNTY-FIPS_2018-03.csv')[['STCOUNTYFP','CITY','STATE']].drop_duplicates()
# Dataset downloaded from here: https://data.world/niccolley/us-zipcode-to-county-state/workspace/file?filename=ZIP-COUNTY-FIPS_2018-03.csv
# You will need to sign up before accessing the dataset


# Dictionary that maps state abbreviation to full name
states = {
        'AK': 'Alaska',
        'AL': 'Alabama',
        'AR': 'Arkansas',
        'AS': 'American Samoa',
        'AZ': 'Arizona',
        'CA': 'California',
        'CO': 'Colorado',
        'CT': 'Connecticut',
        'DC': 'District of Columbia',
        'DE': 'Delaware',
        'FL': 'Florida',
        'GA': 'Georgia',
        'GU': 'Guam',
        'HI': 'Hawaii',
        'IA': 'Iowa',
        'ID': 'Idaho',
        'IL': 'Illinois',
        'IN': 'Indiana',
        'KS': 'Kansas',
        'KY': 'Kentucky',
        'LA': 'Louisiana',
        'MA': 'Massachusetts',
        'MD': 'Maryland',
        'ME': 'Maine',
        'MI': 'Michigan',
        'MN': 'Minnesota',
        'MO': 'Missouri',
        'MP': 'Northern Mariana Islands',
        'MS': 'Mississippi',
        'MT': 'Montana',
        'NA': 'National',
        'NC': 'North Carolina',
        'ND': 'North Dakota',
        'NE': 'Nebraska',
        'NH': 'New Hampshire',
        'NJ': 'New Jersey',
        'NM': 'New Mexico',
        'NV': 'Nevada',
        'NY': 'New York',
        'OH': 'Ohio',
        'OK': 'Oklahoma',
        'OR': 'Oregon',
        'PA': 'Pennsylvania',
        'PR': 'Puerto Rico',
        'RI': 'Rhode Island',
        'SC': 'South Carolina',
        'SD': 'South Dakota',
        'TN': 'Tennessee',
        'TX': 'Texas',
        'UT': 'Utah',
        'VA': 'Virginia',
        'VI': 'Virgin Islands',
        'VT': 'Vermont',
        'WA': 'Washington',
        'WI': 'Wisconsin',
        'WV': 'West Virginia',
        'WY': 'Wyoming'
}


# Convert state from abbreviation to full name
df["STATE"] = df["STATE"].apply(lambda x: states[x])

conn = sqlite3.connect(DATA_DIR / 'processed' / 'covid19_sample.db')
c = conn.cursor()


county_fips = """
    CREATE TABLE IF NOT EXISTS county_fips (
        STCOUNTYFP INT,
        CITY STRING, 
        STATE STRING
    );
"""

c.execute(county_fips)
conn.commit()

# Convert dataframe to sql table
df.to_sql('county_fips', conn, if_exists='append', index=False)

# Remove new line from states in tweets table
remove_new_line = """UPDATE tweets 
SET state = REPLACE(state, '\n', '');"""

change_col_name = """ALTER TABLE county_fips RENAME COLUMN STCOUNTYFP to FIPS;"""

c.execute(remove_new_line)
c.execute(change_col_name)
conn.commit()


join_cmd = """CREATE TABLE IF NOT EXISTS tweets_with_fips AS 
SELECT id, tweets.state, tweets.city, user_id, tweets.text, created_at, is_retweet,
         original_tweet_id, retweet_count, favorite_count, lang, county_fips.FIPS
FROM tweets 
JOIN
county_fips
ON county_fips.STATE = tweets.state AND county_fips.CITY = tweets.city;"""


c.execute(join_cmd)
conn.commit()


# Remove cities with multiple counties
find_dupl_cities = """CREATE TABLE IF NOT EXISTS unique_tweets_fips AS
SELECT *, ROW_NUMBER() OVER (PARTITION BY id) AS RN
FROM tweets_with_fips
;"""
c.execute(find_dupl_cities)
conn.commit()

remove_dupl_cities = """DELETE FROM unique_tweets_fips WHERE RN<>1"""
c.execute(remove_dupl_cities)
c.execute('DROP TABLE IF EXISTS "tweets";')
c.execute('DROP TABLE IF EXISTS "tweets_with_fips";')
c.execute('DROP TABLE IF EXISTS "county_fips";')
conn.commit()


final_table = """ALTER TABLE unique_tweets_fips
RENAME TO tweets;"""
c.execute(final_table)
c.execute('DROP TABLE IF EXISTS "unique_tweets_fips";')

conn.commit()
conn.close()
