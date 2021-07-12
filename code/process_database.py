from pathlib import Path
import sqlite3
import shutil
import pandas as pd
from datetime import datetime

DATA_DIR = Path(__file__).parent.parent / 'data'
TWEETS_FILE = 'covid19_sample.db'
DEMOGRAPHICS_FILE = 'counties.db'
COVID_FILE = 'covid_data.db'
DB_FILE = 'processed.db'

# ---------------------------------------------------------------------- #
#                       PREPROCESS SCRAPED TWEETS                        #
# ---------------------------------------------------------------------- #

# Create a connection to the db of scraped tweets with tables [tweets, users, hashtags]
shutil.copy(DATA_DIR / 'processed' / TWEETS_FILE,
            DATA_DIR / 'processed' / DB_FILE)

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

c.execute("ALTER TABLE users RENAME COLUMN follower_cnt TO follower_count;")
c.execute("ALTER TABLE users RENAME COLUMN list_cnt TO list_count;")
c.execute("ALTER TABLE users RENAME COLUMN tweet_cnt TO tweet_count;")
conn.commit()


# ---------------------------------------------------------------------- #
#                         CREATE COUNTIES TABLE                          #
# ---------------------------------------------------------------------- #

county_fips = pd.read_csv(DATA_DIR / 'raw' / 'counties' / 'ZIP-COUNTY-FIPS_2018-03.csv')[
    ['STCOUNTYFP', 'CITY', 'STATE', 'COUNTYNAME']].drop_duplicates()
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
    CREATE TABLE counties (
        STCOUNTYFP INT NOT NULL,
        COUNTYNAME STRING,
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
c.execute("ALTER TABLE counties RENAME COLUMN COUNTYNAME TO county;")
conn.commit()


# -------------------------------------------------------------------------------- #
#                              ADD DEMOGRAPHICS TABLE                              #
# -------------------------------------------------------------------------------- #


demographics_dir = DATA_DIR / 'processed' / DEMOGRAPHICS_FILE

# Attach demographics database to processed.db
attach_demog_db = 'ATTACH DATABASE ' + '\'' + \
    str(demographics_dir) + '\'' + ' AS demog_db;'
c.execute(attach_demog_db)
conn.commit()

# Create demographics table in processed.db
create_demographics_table = """CREATE TABLE IF NOT EXISTS demographics(
            fips INT PRIMARY KEY NOT NULL,
            county TEXT,
            state TEXT,
            total_population INT,
            percent_male REAL,
            percent_female REAL,
            percent_age_10_to_14 REAL,
            percent_age_15_to_19 REAL,
            percent_age_20_to_24 REAL,
            percent_age_25_to_34 REAL,
            percent_age_35_to_44 REAL,
            percent_age_45_to_54 REAL,
            percent_age_55_to_59 REAL,
            percent_age_60_to_64 REAL,
            percent_age_65_to_74 REAL,
            percent_age_75_to_84 REAL,
            percent_age_85_and_older REAL,
            median_age REAL,
            percent_hispanic_or_latino REAL,
            percent_white REAL,
            percent_black_or_african_american REAL,
            percent_american_indian_and_alaska_native REAL,
            percent_asian REAL,
            percent_native_hawaiian_and_other_pacific_islander REAL,
            percent_other_race REAL,
            percent_two_or_more_races REAL,
            total_housing_units INT,
            unemployment_rate REAL,
            median_household_income INT,
            mean_household_income INT,
            per_capita_income INT,
            percent_health_insurance REAL,
            percent_private_health_insurance REAL,
            percent_public_coverage REAL,
            percent_no_health_insurance REAL,
            percent_families_income_below_poverty_line REAL,
            percent_people_income_below_poverty_line REAL,
            percent_married_couple_family REAL,
            percent_cohabiting_couple REAL,
            percent_male_householder REAL,
            percent_female_householder REAL,
            avg_household_size REAL,
            avg_family_size REAL,
            percent_males_never_married REAL,
            percent_males_now_married_separated REAL,
            percent_males_separated REAL,
            percent_males_widowed REAL,
            percent_males_divorced REAL,
            percent_females_never_married REAL,
            percent_females_now_married_separated REAL,
            percent_females_separated REAL,
            percent_females_widowed REAL,
            percent_females_divorced REAL,
            percent_education_less_than_9th_grade REAL,
            percent_9th_to_12th_grade_no_diploma REAL,
            percent_high_school_graduate REAL,
            percent_some_college_no_degree REAL,
            percent_associates_degree REAL,
            percent_bachelors_degree REAL,
            percent_graduate_or_professional_degree REAL,
            percent_high_school_graduate_or_higher REAL,
            percent_bachelors_degree_or_higher REAL,
            percent_civilian_veteran REAL,
            percent_with_disability REAL,
            percent_native_born REAL,
            percent_born_in_US REAL,
            percent_foreign_born REAL,
            percent_naturalized_US_citizen REAL,
            percent_not_US_citizen REAL,
            percent_households_with_computer REAL,
            percent_households_with_Internet REAL,
            percent_votes_democrat REAL,
            percent_votes_republican REAL,
            percent_votes_libertarian REAL,
            percent_votes_green REAL,
            percent_votes_other REAL);"""
c.execute(create_demographics_table)
conn.commit()

populate_demographics = """INSERT INTO demographics SELECT * FROM demog_db.county_data"""
c.execute(populate_demographics)
conn.commit()


# Combine each county with its total_population
join_county_demographics = """
    CREATE TABLE counties_with_population AS
    SELECT c.fips, c.county, c.city, c.state, d.total_population
    FROM counties as c
    LEFT OUTER JOIN demographics as d
    ON c.fips = d.fips
"""
c.execute(join_county_demographics)
conn.commit()


# Replace null values in total_population with a 0
replace_nulls = """
    UPDATE counties_with_population
        SET
            total_population = COALESCE(total_population, 0);"""
c.execute(replace_nulls)
conn.commit()

# Find cities with multiple counties, ordered by their total population in descending order
find_dupl_cities = """
    CREATE TABLE unique_cities AS
    SELECT *, ROW_NUMBER() OVER (PARTITION BY city, state ORDER BY total_population DESC) AS RN
    FROM counties_with_population;
"""

c.execute(find_dupl_cities)
conn.commit()

# For each city spanning multiple counties, keep only the county with the highest population
c.execute("DELETE FROM unique_cities WHERE RN<>1")
conn.commit()


# -------------------------------------------------------------------------------- #
#                               ADD FIPS TO TWEETS                                 #
# -------------------------------------------------------------------------------- #

# Add fips column to tweets by joining tweets and counties table
c.execute('DROP TABLE IF EXISTS "tweets_with_fips";')
join_tweets_counties = """
    CREATE TABLE tweets_with_fips AS 
    SELECT id, c.fips, t.city, user_id, t.text, created_at, is_retweet, original_tweet_id, retweet_count, favorite_count, lang
    FROM tweets AS t
    JOIN unique_cities AS c
    ON c.state = t.state AND LOWER(c.city) = LOWER(t.city)
    ORDER BY id;
"""
c.execute(join_tweets_counties)
conn.commit()

# Clean up intermediary tables
c.execute('DROP TABLE IF EXISTS "tweets";')
c.execute("DROP TABLE IF EXISTS 'unique_cities';")
c.execute("DROP TABLE IF EXISTS 'counties_with_population';")
c.execute('DROP TABLE IF EXISTS "counties";')
conn.commit()

# Rename final table to 'tweets'
c.execute("ALTER TABLE tweets_with_fips RENAME TO tweets;")
c.execute("DROP TABLE IF EXISTS 'tweets_with_fips';")
conn.commit()


# -------------------------------------------------------------------------------- #
#                            UPDATE CREATED_AT COLUMN                              #
# -------------------------------------------------------------------------------- #

c.execute('DROP TABLE IF EXISTS "updated_tweets";')
updated_tweets_table = """
    CREATE TABLE IF NOT EXISTS updated_tweets (
        id INT PRIMARY KEY NOT NULL,
        fips INT,
        city TEXT,
        user_id INT,
        text TEXT,
        created_at TEXT,
        is_retweet INT,
        original_tweet_id INT,
        retweet_count INT,
        favorite_count INT,
        lang TEXT,
        FOREIGN KEY(user_id) REFERENCES users(id),
        FOREIGN KEY(fips) REFERENCES demographics(fips)
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
    c2.execute('INSERT INTO updated_tweets VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);',
               (row[0], row[1], row[2], row[3], row[4], created_at[i], row[6], row[7], row[8], row[9], row[10]))
    conn.commit()
    i += 1


# Clean up intermediary tables
c.execute('DROP TABLE IF EXISTS "tweets";')
conn.commit()

# Rename final table to 'tweets'
c.execute("ALTER TABLE updated_tweets RENAME TO tweets;")
c.execute("DROP TABLE IF EXISTS 'updated_tweets';")
conn.commit()


# -------------------------------------------------------------------------------- #
#                         ADD PROCESSED COVID TABLE                                #
# -------------------------------------------------------------------------------- #

COVID_DIR = DATA_DIR / 'processed' / COVID_FILE

attach_covid_db = 'ATTACH DATABASE ' + '\'' + \
    str(COVID_DIR) + '\'' + ' AS covid_db;'
c.execute(attach_covid_db)
conn.commit()

create_covid_table = """
    CREATE TABLE IF NOT EXISTS covid(
        date TEXT, 
        fips INT, 
        cases INT, 
        deaths INT, 
        vaccinated_count INT, 
        vaccinated_percent REAL,
        PRIMARY KEY (date, fips),
        FOREIGN KEY(fips) REFERENCES demographics(fips)
);"""
c.execute(create_covid_table)
conn.commit()

populate_covid = """INSERT INTO covid SELECT * FROM covid_db.covid"""
c.execute(populate_covid)
conn.commit()

conn.close()
