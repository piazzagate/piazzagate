from tqdm import tqdm
import pandas as pd
import subprocess
from twarc import Twarc
from pathlib import Path
import sqlite3
import subprocess

DATA_DIR = Path(__file__).parent.parent.parent / 'data'

# ---------------------------------------------------------------------- #
#                           DATABASE SETUP                               #
# ---------------------------------------------------------------------- #

# setting up database
print("Setting up database")
# Create connection to database
conn = sqlite3.connect(DATA_DIR / 'processed' / 'data.db')
c = conn.cursor()
c.execute('DROP TABLE IF EXISTS "tweets";')
c.execute('DROP TABLE IF EXISTS "users";')
c.execute('DROP TABLE IF EXISTS "hashtags";')

tweet_table_cmd = """
    CREATE TABLE tweets (
        id INT PRIMARY KEY NOT NULL,
        user_id INT,
        text TEXT,
        created_at TEXT,
        place_id TEXT,
        lon REAL,
        lat REAL,
        in_reply_to_status_id INT,
        in_reply_to_user_id INT,
        is_quote_status INT,
        retweet_count INT,
        favorite_count INT,
        lang TEXT,
        FOREIGN KEY(user_id) REFERENCES users(id)
    );
"""
user_table_cmd = """
    CREATE TABLE users (
        id INT PRIMARY KEY NOT NULL,
        location TEXT,
        verified INT,
        follower_cnt INT,
        list_cnt INT,
        tweet_cnt INT
    );
"""
hashtag_table_cmd = """
    CREATE TABLE hashtags (
        tweet_id INT NOT NULL,
        hashtag TEXT,
        FOREIGN KEY(tweet_id) REFERENCES tweets(id)
    );
"""
c.execute(tweet_table_cmd)
c.execute(user_table_cmd)
c.execute(hashtag_table_cmd)
conn.commit()


# ---------------------------------------------------------------------- #
#                             EXTRACT IDS                                #
# ---------------------------------------------------------------------- #

print("Getting IDs to hydrate")


def count_lines(filename):
    out = subprocess.Popen(['wc', '-l', str(filename.absolute())],
                           stdout=subprocess.PIPE,
                           stderr=subprocess.STDOUT
                           ).communicate()[0]
    return int(out.decode("utf-8").strip().split(' ')[0])


def get_ids(filename):
    with open(filename) as f:
        for line in f.readlines():
            yield line


tweet_file = DATA_DIR / 'processed' / 'tweets_ids.txt'
total_count = count_lines(tweet_file)

# ---------------------------------------------------------------------- #
#                          HYDRATE TWEET IDS                             #
# ---------------------------------------------------------------------- #

print("Beginning hydration")
t = Twarc()
i = 0
with open(tweet_file) as f:
    for tweet in tqdm(t.hydrate(f), total=total_count):
        # tweet info
        id = tweet['id']
        text = tweet['full_text']
        created_at = tweet['created_at']
        if tweet['coordinates']:
            lon, lat = tweet['coordinates']['coordinates']
        else:
            # print(f"Skipping {i}")
            continue
        place_id = None
        if tweet['place']:
            place_id = tweet['place']['id']
        in_reply_to_status_id = tweet['in_reply_to_status_id']
        in_reply_to_user_id = tweet['in_reply_to_user_id']
        is_quote_status = tweet['is_quote_status']
        retweet_count = tweet['retweet_count']
        favorite_count = tweet['favorite_count']
        lang = tweet['lang']
        hashtags = tweet['entities']['hashtags']

        # tweet author info
        user = tweet['user']
        user_id = user['id']
        user_location = user['location']
        user_verified = user['verified']
        user_follower_cnt = user['followers_count']
        user_list_cnt = user['listed_count']
        user_tweet_cnt = user['statuses_count']

        # Do not insert tweet whose id already exists
        cmd = f"SELECT COUNT(1) FROM tweets WHERE id = {id};"
        c.execute(cmd)
        tweet_exists = False
        for row in c:
            if row[0] == 1:
                tweet_exists = True

        if tweet_exists:
            continue

        c.execute('INSERT INTO tweets VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);',
                  (id, user_id, text, created_at, place_id, lon, lat, in_reply_to_status_id, in_reply_to_user_id,
                   is_quote_status, retweet_count, favorite_count, lang))

        # SQL query to check if user exists in the users table
        cmd = f"SELECT COUNT(1) FROM users WHERE id = {user_id};"
        c.execute(cmd)
        user_exists = False
        for row in c:
            if row[0] == 1:
                user_exists = True

        if not(user_exists):
            c.execute('INSERT INTO users VALUES (?, ?, ?, ?, ?, ?);',
                      (user_id, user_location, user_verified, user_follower_cnt, user_list_cnt, user_tweet_cnt))

        for h in hashtags:
            c.execute('INSERT INTO hashtags VALUES (?, ?);', (id, h['text']))

        i += 1

        if i % 1000 == 0:
            conn.commit()
