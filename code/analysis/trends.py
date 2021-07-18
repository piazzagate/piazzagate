import sqlite3
from pathlib import Path
import pandas as pd

DATA_DIR = Path(__file__).parent.parent.parent / 'data'
DATASET = 'processed_random_train'

conn = sqlite3.connect(DATA_DIR / 'processed' / f'{DATASET}.db')


def tweets_by_county(conn):
    cmd = '''SELECT tweets.fips, demographics.state, demographics.county, demographics.total_population, COUNT(*) AS num_tweets FROM tweets 
    JOIN demographics ON tweets.fips = demographics.fips 
    GROUP BY tweets.fips
    ORDER BY COUNT(*) DESC'''

    df = pd.read_sql_query(cmd, conn)
    df.to_csv(DATA_DIR / 'analysis' / 'tweets_by_county.csv', index=False)


def tweets_over_time(conn):
    cmd = '''SELECT created_at, COUNT(*) AS num_tweets, SUM(favorite_count) AS total_likes FROM tweets 
    GROUP BY created_at
    ORDER BY created_at ASC'''

    df = pd.read_sql_query(cmd, conn)
    df.to_csv(DATA_DIR / 'analysis' / 'tweets_over_time.csv', index=False)


tweets_by_county(conn)
tweets_over_time(conn)
