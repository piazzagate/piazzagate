import pandas as pd
import numpy as np
import sqlite3
from pathlib import Path
import matplotlib.pyplot as plt


DATA_DIR = Path(__file__).parent.parent.parent / 'data'
DATASET = 'processed_random_train'

conn = sqlite3.connect(DATA_DIR / 'processed' / f'{DATASET}.db')
c = conn.cursor()

######## Number of tweets on a day in a county
cmd = """SELECT *, COUNT(created_at) AS num_tweets FROM tweets AS t
GROUP BY created_at, fips"""
c.execute(cmd)
conn.commit()

df = pd.read_sql_query(cmd, conn)

# Convert date (string) into datetime format
df['created_at'] = pd.to_datetime(df['created_at'])


cmd = """SELECT * FROM covid"""
ds = pd.read_sql_query(cmd, conn)
ds['date'] = pd.to_datetime(ds['date'])


ds['cases_on_day'] = (
    ds.groupby(['fips'])['cases']
    .transform(lambda s: s.sub(s.shift().fillna(0)).abs())
)

ds['vaccinated_on_day'] = (
    ds.groupby(['fips'])['vaccinated_count']
    .transform(lambda s: s.sub(s.shift().fillna(0)).abs())
)

ds['deaths_on_day'] = (
    ds.groupby(['fips'])['deaths']
    .transform(lambda s: s.sub(s.shift().fillna(0)).abs())
)

df = pd.merge(df, ds, how='left', left_on = ['fips', 'created_at'], right_on = ['fips', 'date']).fillna(0).drop('date', axis=1)


### Calculate correlations
correlation1 = df['cases_on_day'].corr(df['num_tweets'])
correlation2 = df['vaccinated_on_day'].corr(df['num_tweets'])
correlation3 = df['deaths_on_day'].corr(df['num_tweets'])

print(f"Pearson correlation cases vs num_tweets: {correlation1}")
print(f"Pearson correlation vaccinations vs num_tweets: {correlation2}")
print(f"Pearson correlation deaths vs num_tweets: {correlation3}")


df.plot(x = 'created_at', xlabel = 'Date', y = ['cases_on_day', 'num_tweets'], kind = 'line', figsize = (20,5), title = 'No. of cases vs no. of tweets per day')
plt.savefig(DATA_DIR / 'analysis' / 'cases_vs_tweets.png')
df.plot(x = 'created_at', xlabel = 'Date', y = ['vaccinated_on_day', 'num_tweets'], kind = 'line', figsize = (20,5), title = 'No. of vaccinations vs no. of tweets per day')
plt.savefig(DATA_DIR / 'analysis' / 'vaccines_vs_tweets.png')
df.plot(x = 'created_at', xlabel = 'Date', y = ['deaths_on_day', 'num_tweets'], kind = 'line', figsize = (20,5), title = 'No. of deaths vs no. of tweets per day')
plt.savefig(DATA_DIR / 'analysis' / 'deaths_vs_tweets.png')

# df.plot(x = 'cases_on_day', y = ['num_tweets'], kind = 'scatter', figsize = (30,5), xlabel='No. of cases', ylabel='No. of tweets')
# plt.show()
