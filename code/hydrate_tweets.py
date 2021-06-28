from twarc import Twarc
import sqlite3
import pandas

consumer_key=""
consumer_secret=""
access_token=""
access_token_secret=""

t = Twarc(consumer_key, consumer_secret, access_token, access_token_secret)

# Create connection to database
conn = sqlite3.connect('data.db')
c = conn.cursor()

# Delete tables if they exist
c.execute('DROP TABLE IF EXISTS "tweets";')
c.execute('DROP TABLE IF EXISTS "authors";')
c.execute('DROP TABLE IF EXISTS "hashtags";')

#TODO: Create tables in the database and add data to it. REMEMBER TO COMMIT
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

for tweet in t.hydrate(open('tweet_ids.csv')): # EDIT TO POINT TO LIST OF TWEET IDS
    id = tweet['id']
    text = tweet['text']
    created_at = tweet['created_at']
    place_id = None
    if tweet['place']: place_id = tweet['place']['id']
    in_reply_to_status_id = tweet['in_reply_to_status_id'] # original tweet's id
    in_reply_to_user_id = tweet['in_reply_to_user_id'] # original user's id
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
        (id,user_id,text,created_at,place_id,in_reply_to_status_id,in_reply_to_user_id,
        quoted_status_id,retweeted_status,quote_count,reply_count,retweet_count,favorite_count,lang,))
    
    c.execute('INSERT INTO authors VALUES (?, ?, ?, ?, ?, ?);', 
        (user_id,user_location,user_verified,user_follower_cnt,user_list_cnt,user_tweet_cnt))
    
    for h in hashtags:
        c.execute('INSERT INTO hashtags VALUES (?, ?);', (id,h))

conn.commit()