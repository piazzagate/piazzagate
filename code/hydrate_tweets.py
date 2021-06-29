from pathlib import Path
import pandas as pd
from tqdm import tqdm
import subprocess
from twarc import Twarc
import sqlite3

DATA_DIR = Path(__file__).parent.parent / 'data'
PORTION = 1 / 50  # only extract a fraction of the data due to rate limits

# setting up database


t = Twarc()

print("Setting up database")
# Create connection to database
conn = sqlite3.connect(DATA_DIR / 'data.db')
c = conn.cursor()

# Delete tables if they exist
c.execute('DROP TABLE IF EXISTS "tweets";')
c.execute('DROP TABLE IF EXISTS "authors";')
c.execute('DROP TABLE IF EXISTS "hashtags";')

tweet_table_cmd = """
    CREATE TABLE tweets (
        id INT PRIMARY KEY NOT NULL,
        user_id INT FOREIGN KEY,
        text TEXT, 
        created_at TEXT,
        place_id TEXT,
        in_reply_to_status_id INT,
        in_reply_to_user_id INT,
        quoted_status_id INT,
        retweeted_status INT,
        quote_count INT,
        reply_count INT,
        retweet_count INT,
        favorite_count INT,
        lang TEXT,
    );
"""
author_table_cmd = """
    CREATE TABLE authors (
        author_id INT PRIMARY KEY NOT NULL,
        location TEXT,
        verified INT,
        follower_cnt INT,
        list_cnt INT,
        tweet_cnt INT
    );
"""

hashtag_table_cmd = """
    CREATE TABLE hashtags (
        tweet_id INT FOREIGN KEY,
        hashtag TEXT
    );
"""

c.execute(tweet_table_cmd)
c.execute(author_table_cmd)
conn.commit()


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


tweet_files = (DATA_DIR / 'raw' / 'tweets').glob('*.csv')
print("Counting total number of IDs to hydrate")
total_count = get_total_count(tweet_files)

print("Beginning hydration")
ids = get_ids(tweet_files)

for tweet in tqdm(t.hydrate(ids), total=total_count):
    id = tweet['id']
    text = tweet['text']
    created_at = tweet['created_at']
    place_id = None
    if tweet['place']:
        place_id = tweet['place']['id']
    # original tweet's id
    in_reply_to_status_id = tweet['in_reply_to_status_id']
    in_reply_to_user_id = tweet['in_reply_to_user_id']  # original user's id
    quoted_status_id = tweet['quoted_status_id']
    retweeted_status = tweet['retweeted_status']
    quote_count = tweet['quote_count']
    reply_count = tweet['reply_count']
    retweet_count = tweet['retweet_count']
    favorite_count = tweet['favorite_count']
    lang = tweet['lang']
    hashtags = tweet['entities']['hashtags']

    user = tweet['user']
    user_id = user['id']
    user_location = user['location']
    user_verified = user['verified']
    user_follower_cnt = user['followers_count']
    user_list_cnt = user['listed_count']
    user_tweet_cnt = user['statuses_count']

    c.execute('INSERT INTO tweets VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);',
              (id, user_id, text, created_at, place_id, in_reply_to_status_id, in_reply_to_user_id,
               quoted_status_id, retweeted_status, quote_count, reply_count, retweet_count, favorite_count, lang,))

    c.execute('INSERT INTO authors VALUES (?, ?, ?, ?, ?, ?);',
              (user_id, user_location, user_verified, user_follower_cnt, user_list_cnt, user_tweet_cnt))

    for h in hashtags:
        c.execute('INSERT INTO hashtags VALUES (?, ?);', (id, h))

conn.commit()
