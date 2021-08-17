import sqlite3
from pathlib import Path
import pandas as pd
import numpy as np
from matplotlib import dates
import matplotlib.pyplot as plt
from datetime import datetime
import seaborn as sns
sns.set_theme(style='darkgrid')

DATA_DIR = Path(__file__).parent.parent.parent / 'data'
DATASET = 'processed_random_train'

conn = sqlite3.connect(DATA_DIR / 'processed' / f'{DATASET}.db')


def get_demographic_data(conn):
    cmd = '''SELECT * FROM demographics'''

    df = pd.read_sql_query(cmd, conn)
    return df

def get_covid_data(conn):
    cmd = '''SELECT * FROM covid'''

    df = pd.read_sql_query(cmd, conn)
    return df

def get_tweet_data(conn):
    cmd = '''SELECT tweets.retweet_count, tweets.favorite_count, users.verified, users.follower_count, users.list_count, users.tweet_count 
    FROM tweets JOIN users ON tweets.user_id = users.id'''

    df = pd.read_sql_query(cmd, conn)
    return df

def generate_correlation_matrix(data, title, outfile, figsize):
    fig, ax = plt.subplots(figsize=figsize)
    sns.heatmap(data.corr(), annot=True, cbar_kws={'orientation': 'horizontal'}, ax=ax)
    ax.set(title=f"Correlation matrix for {title} data")
    fig.savefig(DATA_DIR / 'analysis' / outfile)

covid_data = get_covid_data(conn)
demographic_data = get_demographic_data(conn)
#generate_correlation_matrix(demographic_data, "demographic", "demographic_correlations.png", figsize=(60,48))
#generate_correlation_matrix(covid_data, "covid-19 cases/deaths/vaccination", "covid_correlations.png", figsize=(15,10))
#generate_correlation_matrix(get_tweet_data(conn), "tweet/author statistics", "tweet_stats_correlations.png", figsize=(15,10))

# demographic vars
#ind_vars = ['total_population', 'per_capita_income', 'percent_bachelors_degree_or_higher', 'percent_households_with_Internet', 'percent_votes_democrat', 'percent_votes_republican']
#generate_correlation_matrix(demographic_data[ind_vars], "official demographic statistics", "official_demog_stats_correlations.png", figsize=(15,10))

#generate_correlation_matrix(covid_data[covid_data['vaccinated_count'] > 0], "covid-19 cases/deaths/nonzero vaccination", "remove_0_covid_correlations.png", figsize=(15,10))
