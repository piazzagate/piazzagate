from pathlib import Path
from tqdm import tqdm
import pandas as pd
import subprocess
from twarc import Twarc
from pathlib import Path
import sqlite3
import subprocess

DATA_DIR = Path(__file__).parent.parent / 'data'
PORTION = 1 / 50  # only extract a fraction of the data due to rate limits

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
        has_coords INT,
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


def get_ids(tweet_files):
    for tweet_file in tweet_files:
        df = pd.read_csv(tweet_file, header=None)
        ids = list(df.iloc[:, 0])
        ids = ids[:int(len(ids) * PORTION)]

        for id in ids:
            yield id


def count_lines(filename):
    out = subprocess.Popen(['wc', '-l', str(filename.absolute())],
                           stdout=subprocess.PIPE,
                           stderr=subprocess.STDOUT
                           ).communicate()[0]
    return int(int(out.decode("utf-8").strip().split(' ')[0]) * PORTION)


def get_total_count(tweet_files):
    total = 0
    for tweet_file in tweet_files:
        total += count_lines(tweet_file)

    return total

print("Getting IDs to hydrate")
tweet_files = list((DATA_DIR / 'raw' / 'tweets').glob('*.csv'))
ids = get_ids(tweet_files)
total_count = get_total_count(tweet_files)

# ---------------------------------------------------------------------- #
#                          HYDRATE TWEET IDS                             #
# ---------------------------------------------------------------------- #

t = Twarc()
i = 0
for tweet in tqdm(t.hydrate(ids), total=total_count):
    # tweet info
    id = tweet['id']
    text = tweet['full_text']
    created_at = tweet['created_at']
    has_coords = 0
    if tweet['coordinates']: has_coords = 1
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
        if row[0] == 1: tweet_exists = True
    
    if tweet_exists:
        continue
        
    c.execute('INSERT INTO tweets VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);',
              (id, user_id, text, created_at, place_id, has_coords, in_reply_to_status_id, in_reply_to_user_id,
               is_quote_status, retweet_count, favorite_count, lang))

    # SQL query to check if user exists in the users table
    cmd = f"SELECT COUNT(1) FROM users WHERE id = {user_id};"
    c.execute(cmd)
    user_exists = False
    for row in c:
        if row[0] == 1: user_exists = True
    
    if not(user_exists):
        c.execute('INSERT INTO users VALUES (?, ?, ?, ?, ?, ?);',
                (user_id, user_location, user_verified, user_follower_cnt, user_list_cnt, user_tweet_cnt))
    
    for h in hashtags:
        c.execute('INSERT INTO hashtags VALUES (?, ?);', (id, h['text']))
    
    i += 1
    
    if i % 1000 == 0:
        conn.commit()
