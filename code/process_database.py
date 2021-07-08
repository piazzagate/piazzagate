from pathlib import Path
import sqlite3
import shutil
import pandas as pd
from datetime import datetime

DATA_DIR = Path(__file__).parent.parent / 'data'
TWEETS_FILE = 'covid19_sample.db'
DB_FILE = 'processed.db'

# ---------------------------------------------------------------------- #
#                       PREPROCESS SCRAPED TWEETS                        #
# ---------------------------------------------------------------------- #

# Create a connection to the db of scraped tweets with tables [tweets, users, hashtags]
shutil.copy(DATA_DIR / 'processed' / TWEETS_FILE, DATA_DIR / 'processed' / DB_FILE)
conn = sqlite3.connect(DATA_DIR / 'processed' / DB_FILE)
c = conn.cursor()

# Remove new line from tweets(states)
remove_new_line = """
    UPDATE tweets 
    SET state = REPLACE(state, '\n', '');
"""
c.execute(remove_new_line)

# Lowercase hashtags(hashtag)
lower_hashtags = """
    UPDATE hashtags 
    SET hashtag = LOWER(hashtag);
"""
c.execute(lower_hashtags)

conn.commit()


# ---------------------------------------------------------------------- #
#                         CREATE COUNTIES TABLE                          #
# ---------------------------------------------------------------------- #

county_fips = pd.read_csv(DATA_DIR / 'raw' / 'counties' / 'ZIP-COUNTY-FIPS_2018-03.csv')[['STCOUNTYFP','CITY','STATE']].drop_duplicates()
# Dataset downloaded from here: https://data.world/niccolley/us-zipcode-to-county-state/workspace/file?filename=ZIP-COUNTY-FIPS_2018-03.csv
# You will need to sign up before accessing the dataset

# Dictionary that maps state abbreviation to full name
states = {
        'AK': 'Alaska', 'AL': 'Alabama', 'AR': 'Arkansas', 'AS': 'American Samoa', 'AZ': 'Arizona', 'CA': 'California', 'CO': 'Colorado',
        'CT': 'Connecticut', 'DC': 'District of Columbia', 'DE': 'Delaware', 'FL': 'Florida', 'GA': 'Georgia', 'GU': 'Guam', 'HI': 'Hawaii',
        'IA': 'Iowa', 'ID': 'Idaho', 'IL': 'Illinois', 'IN': 'Indiana', 'KS': 'Kansas', 'KY': 'Kentucky', 'LA': 'Louisiana', 'MA': 'Massachusetts',
        'MD': 'Maryland', 'ME': 'Maine', 'MI': 'Michigan', 'MN': 'Minnesota', 'MO': 'Missouri', 'MP': 'Northern Mariana Islands', 'MS': 'Mississippi',
        'MT': 'Montana', 'NA': 'National', 'NC': 'North Carolina', 'ND': 'North Dakota', 'NE': 'Nebraska', 'NH': 'New Hampshire', 'NJ': 'New Jersey',
        'NM': 'New Mexico', 'NV': 'Nevada', 'NY': 'New York', 'OH': 'Ohio', 'OK': 'Oklahoma', 'OR': 'Oregon', 'PA': 'Pennsylvania', 'PR': 'Puerto Rico',
        'RI': 'Rhode Island', 'SC': 'South Carolina', 'SD': 'South Dakota', 'TN': 'Tennessee', 'TX': 'Texas', 'UT': 'Utah', 'VA': 'Virginia',
        'VI': 'Virgin Islands', 'VT': 'Vermont', 'WA': 'Washington', 'WI': 'Wisconsin', 'WV': 'West Virginia', 'WY': 'Wyoming'
}

# Convert state from abbreviation to full name
county_fips["STATE"] = county_fips["STATE"].apply(lambda x: states[x])

c.execute('DROP TABLE IF EXISTS "counties";')
counties_table_cmd = """
    CREATE TABLE IF NOT EXISTS counties (
        STCOUNTYFP INT NOT NULL,
        CITY STRING, 
        STATE STRING
    );
"""
c.execute(counties_table_cmd)
conn.commit()

# Convert dataframe to sql table
county_fips.to_sql('counties', conn, if_exists='append', index=False)
c.execute("ALTER TABLE counties RENAME COLUMN STCOUNTYFP TO fips;")
c.execute("ALTER TABLE counties RENAME COLUMN CITY TO city;")
c.execute("ALTER TABLE counties RENAME COLUMN STATE TO state;")
conn.commit()

# ---------------------------------------------------------------------- #
#                          ADD FIPS TO TWEETS                            #
# ---------------------------------------------------------------------- #

# Add fips column to tweets by joining tweets and counties table
c.execute('DROP TABLE IF EXISTS "tweets_with_fips";')
join_tweets_counties = """
    CREATE TABLE IF NOT EXISTS tweets_with_fips AS 
    SELECT id, c.fips, user_id, t.text, created_at, is_retweet, original_tweet_id, retweet_count, favorite_count, lang
    FROM tweets AS t
    JOIN counties AS c
    ON c.state = t.state AND LOWER(c.city) = LOWER(t.city);
"""
c.execute(join_tweets_counties)
conn.commit()

# Remove cities with multiple counties
c.execute('DROP TABLE IF EXISTS "unique_tweets_fips";')
find_dupl_cities = """
    CREATE TABLE IF NOT EXISTS unique_tweets_fips AS
    SELECT *, ROW_NUMBER() OVER (PARTITION BY id) AS RN
    FROM tweets_with_fips;
"""
c.execute(find_dupl_cities)
conn.commit()
c.execute("DELETE FROM unique_tweets_fips WHERE RN<>1")
conn.commit()

# Clean up intermediary tables
c.execute('DROP TABLE IF EXISTS "tweets";')
c.execute('DROP TABLE IF EXISTS "tweets_with_fips";')
c.execute('DROP TABLE IF EXISTS "counties";')
conn.commit()

# Rename final table to 'tweets'
c.execute("ALTER TABLE unique_tweets_fips RENAME TO tweets;")
c.execute("DROP TABLE IF EXISTS 'unique_tweets_fips';")
conn.commit()


# ---------------------------------------------------------------------- #
#                       UPDATE CREATED_AT COLUMN                         #
# ---------------------------------------------------------------------- #

c.execute('DROP TABLE IF EXISTS "updated_tweets";')
updated_tweets_table = """
    CREATE TABLE IF NOT EXISTS updated_tweets (
        id INT PRIMARY KEY NOT NULL,
        fips INT,
        user_id INT,
        text TEXT,
        created_at TEXT,
        is_retweet INT,
        original_tweet_id INT,
        retweet_count INT,
        favorite_count INT,
        lang TEXT,
        FOREIGN KEY(user_id) REFERENCES users(id),
        FOREIGN KEY(fips) REFERENCES counties(fips)
    );
"""
c.execute(updated_tweets_table)
conn.commit()

# Get tweets(created_at) and reformat to 'XXXX-XX-XX'
# Store reformatted dates in `created_at`
c.execute("SELECT created_at FROM tweets;")
created_at = []
for row in c:
    # get year
    y = int(row[0][-4:])
    # get month
    month_name = row[0][4:7]
    dt = datetime.strptime(month_name, "%b")
    m = int(dt.month)
    # get day
    d = int(row[0][8:10])
    created_at.append(f"{y:04}-{m:02}-{d:02}")

# Insert entries from the tweets table into the
# updated_tweets table with the updated date
c2 = conn.cursor()
c.execute("SELECT * FROM tweets")
i = 0
for row in c:
    #print(row[0], row[1], row[2], row[3], created_at[i], row[5], row[6], row[7], row[8], row[9])
    c2.execute('INSERT INTO updated_tweets VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?);', 
                (row[0], row[1], row[2], row[3], created_at[i], row[5], row[6], row[7], row[8], row[9]))
    conn.commit()
    i += 1
    

# Clean up intermediary tables
c.execute('DROP TABLE IF EXISTS "tweets";')
conn.commit()

# Rename final table to 'tweets'
c.execute("ALTER TABLE updated_tweets RENAME TO tweets;")
c.execute("DROP TABLE IF EXISTS 'updated_tweets';")
conn.commit()

conn.close()