import sqlite3
from pathlib import Path
import numpy as np
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
from datetime import datetime
from urllib.request import urlopen
import json
import plotly.express as px
from langcodes import Language
from sklearn.linear_model import LinearRegression

with urlopen('https://raw.githubusercontent.com/plotly/datasets/master/geojson-counties-fips.json') as response:
    counties = json.load(response)

sns.set_theme(style='darkgrid')

DATA_DIR = Path(__file__).parent.parent.parent / 'data'
DATASET = 'processed_random_train'

conn = sqlite3.connect(DATA_DIR / 'processed' / f'{DATASET}.db')

us_state_abbrev = {
    'Alabama': 'AL',
    'Alaska': 'AK',
    'American Samoa': 'AS',
    'Arizona': 'AZ',
    'Arkansas': 'AR',
    'California': 'CA',
    'Colorado': 'CO',
    'Connecticut': 'CT',
    'Delaware': 'DE',
    'District of Columbia': 'DC',
    'Florida': 'FL',
    'Georgia': 'GA',
    'Guam': 'GU',
    'Hawaii': 'HI',
    'Idaho': 'ID',
    'Illinois': 'IL',
    'Indiana': 'IN',
    'Iowa': 'IA',
    'Kansas': 'KS',
    'Kentucky': 'KY',
    'Louisiana': 'LA',
    'Maine': 'ME',
    'Maryland': 'MD',
    'Massachusetts': 'MA',
    'Michigan': 'MI',
    'Minnesota': 'MN',
    'Mississippi': 'MS',
    'Missouri': 'MO',
    'Montana': 'MT',
    'Nebraska': 'NE',
    'Nevada': 'NV',
    'New Hampshire': 'NH',
    'New Jersey': 'NJ',
    'New Mexico': 'NM',
    'New York': 'NY',
    'North Carolina': 'NC',
    'North Dakota': 'ND',
    'Northern Mariana Islands': 'MP',
    'Ohio': 'OH',
    'Oklahoma': 'OK',
    'Oregon': 'OR',
    'Pennsylvania': 'PA',
    'Puerto Rico': 'PR',
    'Rhode Island': 'RI',
    'South Carolina': 'SC',
    'South Dakota': 'SD',
    'Tennessee': 'TN',
    'Texas': 'TX',
    'Utah': 'UT',
    'Vermont': 'VT',
    'Virgin Islands': 'VI',
    'Virginia': 'VA',
    'Washington': 'WA',
    'West Virginia': 'WV',
    'Wisconsin': 'WI',
    'Wyoming': 'WY'
}


def tweets_by_county(conn):
    cmd = '''SELECT demographics.fips, demographics.state, demographics.county, demographics.total_population, COUNT(tweets.id) AS num_tweets FROM demographics 
    LEFT JOIN tweets ON tweets.fips = demographics.fips
    GROUP BY demographics.fips
    ORDER BY COUNT(tweets.id) DESC'''

    df = pd.read_sql_query(cmd, conn)
    df['fips'] = df['fips'].map(lambda fip: f'{fip:05d}')
    fig = px.choropleth(df, geojson=counties, locations='fips',
                        color='num_tweets', scope='usa', labels={'num_tweets': '# Tweets'}, color_continuous_scale='Blues')
    fig.write_image(DATA_DIR / 'analysis' /
                    'tweets_by_county.png', width=20*300, height=10*300)
    df.to_csv(DATA_DIR / 'analysis' / 'tweets_by_county.csv', index=False)


def tweets_rate(conn):
    cmd = '''SELECT demographics.fips, demographics.state, demographics.county, demographics.total_population, COUNT(tweets.id) AS num_tweets, demographics.percent_votes_democrat, demographics.percent_votes_republican FROM demographics 
    JOIN tweets ON tweets.fips = demographics.fips
    GROUP BY demographics.fips
    ORDER BY COUNT(tweets.id) DESC'''

    df = pd.read_sql_query(cmd, conn)
    df['tweet_rate'] = df['num_tweets'] * 1000000 / df['total_population']
    df = df.dropna()
    df = df.sort_values('tweet_rate', ascending=False)
    df.to_csv(DATA_DIR / 'analysis' / 'tweets_rate.csv', index=False)

    fig, ax = plt.subplots(figsize=(20, 10))
    sns.scatterplot(data=df, x='percent_votes_democrat',
                    y='tweet_rate', alpha=0.5, ax=ax)
    ax.set(xlabel="% votes for Democrats in 2020 election",
           ylabel="# misinformation tweets per million people")
    ax.set_title(
        '# misinformation tweets per million people based on political leaning for each county')
    fig.savefig(DATA_DIR / 'analysis' / 'tweets_rate_by_party_democrat.png')

    fig, ax = plt.subplots(figsize=(20, 10))
    sns.scatterplot(data=df, x='percent_votes_republican',
                    y='tweet_rate', alpha=0.5, ax=ax)
    ax.set(xlabel="% votes for Republicans in 2020 election",
           ylabel="# misinformation tweets per million people")
    ax.set_title(
        '# misinformation tweets per million people based on political leaning for each county')

    X = np.array(df['percent_votes_republican'])[::, np.newaxis]
    y = np.array(df['tweet_rate'])
    reg = LinearRegression().fit(X, y)
    print('Coefficients: \n', reg.coef_)
    print('Coefficient of determination: %.2f'
          % reg.score(X, y))
    ax.plot(X, reg.predict(X), color='blue', linewidth=3)
    fig.savefig(DATA_DIR / 'analysis' / 'tweets_rate_by_party_republican.png')

    fig, ax = plt.subplots(figsize=(20, 10))
    sns.scatterplot(data=df, x='total_population',
                    y='tweet_rate', alpha=0.5, ax=ax)
    ax.set(xlabel="County population",
           ylabel="# misinformation tweets per million people")
    ax.set_title(
        '# misinformation tweets per million people based on county population')
    fig.savefig(DATA_DIR / 'analysis' / 'tweets_rate_by_population.png')


def tweets_by_state(conn):
    cmd = '''WITH tweets_with_state (id, state)
    AS (SELECT tweets.id, demographics.state FROM tweets JOIN demographics ON tweets.fips = demographics.fips),
    states (state, total_population) 
    AS (SELECT demographics.state, SUM(demographics.total_population) FROM demographics GROUP BY demographics.state)
    SELECT states.state, states.total_population, COUNT(tweets_with_state.id) AS num_tweets FROM states
    LEFT JOIN tweets_with_state ON tweets_with_state.state = states.state
    GROUP BY states.state
    ORDER BY num_tweets DESC'''

    df = pd.read_sql_query(cmd, conn)
    df['state'] = df['state'].map(lambda x: us_state_abbrev[x])
    fig = px.choropleth(locations=df['state'], locationmode='USA-states', color=df['num_tweets'],
                        scope='usa', labels={'color': '# Tweets'}, color_continuous_scale='Blues')
    fig.write_image(DATA_DIR / 'analysis' /
                    'tweets_by_state.png', width=20*300, height=10*300)
    df.to_csv(DATA_DIR / 'analysis' / 'tweets_by_state.csv', index=False)


def tweets_over_time(conn):
    cmd = '''SELECT created_at, COUNT(*) AS num_tweets, SUM(favorite_count) AS total_likes FROM tweets
    GROUP BY created_at
    ORDER BY created_at ASC'''

    df = pd.read_sql_query(cmd, conn)
    df['created_at'] = [datetime.strptime(
        d, '%Y-%m-%d').date() for d in df['created_at']]
    rolling_avg = df.rolling(7, min_periods=1).mean()
    df['num_tweets'], df['total_likes'] = rolling_avg['num_tweets'], rolling_avg['total_likes']
    df.to_csv(DATA_DIR / 'analysis' / 'tweets_over_time.csv', index=False)

    fig, ax = plt.subplots(figsize=(40, 10))
    sns.lineplot(data=df, x='created_at', y='num_tweets', ax=ax)
    ax.set(xlabel="Date", ylabel="# tweets (average over 7 days)")
    fig.savefig(DATA_DIR / 'analysis' / 'tweets_over_time_num.png')

    fig, ax = plt.subplots(figsize=(40, 10))
    sns.lineplot(data=df, x='created_at', y='total_likes', ax=ax)
    ax.set(xlabel="Date", ylabel="# likes (average over 7 days)")
    fig.savefig(DATA_DIR / 'analysis' / 'tweets_over_time_likes.png')


def language_spread(conn):
    cmd = '''SELECT lang, COUNT(*) AS num_tweets FROM tweets
    GROUP BY lang
    ORDER BY COUNT(*) DESC'''

    df = pd.read_sql_query(cmd, conn)
    df.to_csv(DATA_DIR / 'analysis' / 'language_spread.csv', index=False)

    english_count = df[df['lang'] == 'en'].iloc[0]['num_tweets']
    # und is unidentified
    # ca is Catalan, but Twitter is wrong about all of these Tweets are simply in English
    df = df.drop(df[df['lang'] == 'en'].index).drop(
        df[df['lang'] == 'und'].index).drop(df[df['lang'] == 'ca'].index)
    df['lang'] = df['lang'].map(
        lambda x: Language.make(language=x).display_name())
    fig, ax = plt.subplots(figsize=(40, 10))
    sns.barplot(data=df, x='lang', y='num_tweets')
    ax.set(
        xlabel=f'Non-English languages (English={english_count})', ylabel='# tweets')
    fig.savefig(DATA_DIR / 'analysis' / 'language_spread.png')


def users_tweets_spread(conn):
    cmd = '''SELECT users.id, COUNT(tweets.id) AS num_tweets FROM tweets
    JOIN users ON users.id = tweets.user_id
    GROUP BY users.id
    ORDER BY num_tweets DESC'''

    df = pd.read_sql_query(cmd, conn)
    from collections import defaultdict
    count = defaultdict(lambda: 0)
    for _, row in df.iterrows():
        count[row['num_tweets']] += 1

    data = {'num_tweets': [], 'count': []}
    for k, v in count.items():
        data['num_tweets'].append(k)
        data['count'].append(v)
    df = pd.DataFrame.from_dict(data)
    df.to_csv(DATA_DIR / 'analysis' / 'users_tweets_spread.csv', index=False)

    x = ['1', '>1']
    y = [df[df['num_tweets'] == 1]['count'].sum(), df[df['num_tweets'] > 1]
         ['count'].sum()]

    fig, ax = plt.subplots(figsize=(20, 10))
    sns.barplot(x=x, y=y, ax=ax)
    ax.set_title('# users posting a given # tweets')
    ax.set(xlabel="# tweets", ylabel="# users")
    fig.savefig(DATA_DIR / 'analysis' / 'users_tweets_spread.png')


# tweets_by_county(conn)
tweets_rate(conn)
# tweets_by_state(conn)
# tweets_over_time(conn)
# language_spread(conn)
# users_tweets_spread(conn)
