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
cmd = """CREATE TABLE IF NOT EXISTS num_tweets AS SELECT *, COUNT(created_at) AS num_tweets FROM tweets AS t
GROUP BY created_at, fips"""
c.execute(cmd)
conn.commit()


# Join covid and tweets table
cmd = """SELECT t.fips, t.city, c.date, c.cases, c.deaths, c.vaccinated_count, t.num_tweets
FROM num_tweets AS t
JOIN covid AS c
ON t.fips = c.fips AND t.created_at = c.date"""
c.execute(cmd)
conn.commit()

df = pd.read_sql_query(cmd, conn)

# Convert date (string) into datetime format
df['date'] = pd.to_datetime(df['date'])

correlation = df['cases'].corr(df['num_tweets'])
print(f"Pearson correlation: {correlation}")
#correlation = 0.1002511113405427  

cmd = """SELECT * FROM covid"""
ds = pd.read_sql_query(cmd, conn)
ds['date'] = pd.to_datetime(ds['date'])

# Copy the cases column into cases_on_day
ds['cases_on_day'] = ds['cases']


############ this is the slow loop
for i, row in ds.iterrows():
    val = ds[(ds['date'] == row['date'] - pd.Timedelta(days=1)) & (ds['fips'] == row['fips'])]['cases']
    if len(val) > 0:
        ds.at[i, 'cases_on_day'] = ds.at[i, 'cases_on_day'] - val


df.plot(x = 'date', y = ['cases', 'num_tweets'], kind = 'line', figsize = (30,5))
# df.plot(x = 'cases', y = ['num_tweets'], kind = 'scatter', figsize = (30,5), xlabel='No. of cases', ylabel='No. of tweets')
# plt.show()
plt.savefig(DATA_DIR / 'analysis' / 'cases_vs_tweets.png')