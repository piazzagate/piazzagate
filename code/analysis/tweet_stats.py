import sqlite3
from pathlib import Path
import pandas as pd
import numpy as np
from sklearn import preprocessing
import seaborn as sns
import matplotlib.pyplot as plt
from scipy.stats import ttest_ind
import util
sns.set_theme(style='darkgrid')

DATA_DIR = Path(__file__).parent.parent.parent / 'data'
GRAPH_DIR = DATA_DIR / 'analysis'
TRAIN_DATASET = 'processed_random_train'
TEST_DATASET = 'processed_random_test'

def get_data(dataset):
    conn = sqlite3.connect(DATA_DIR / 'processed' / f'{dataset}.db')
    cmd = '''
    SELECT tweets.fips, tweets.retweet_count, tweets.favorite_count, users.verified, users.follower_count, 
        demographics.total_population, demographics.median_age, demographics.unemployment_rate, demographics.per_capita_income,
        demographics.percent_high_school_graduate, demographics.percent_households_with_Internet, 
        demographics.percent_votes_democrat, demographics.percent_votes_republican
    FROM tweets JOIN users ON tweets.user_id = users.id JOIN demographics on tweets.fips = demographics.fips'''

    df = pd.read_sql_query(cmd, conn).fillna(0)

    return df

def multiple_regression(df, ind_vars, remove_0):
    # Remove tweets with a mean retweet count of 0
    if remove_0: df = df[df['retweet_count'] > 0]

    min_max_scaler = preprocessing.MinMaxScaler()

    data = pd.DataFrame(min_max_scaler.fit_transform(df.values), 
            columns=['fips', 'retweet_count', 'favorite_count', 'verified', 'follower_count', 'total_population', 
                'median_age', 'unemployment_rate', 'per_capita_income', 'percent_high_school_graduate', 
                'percent_households_with_Internet', 'percent_votes_democrat', 'percent_votes_republican'])
    
    dep_var = 'retweet_count'
    util.regression(data, ind_vars, dep_var)

def histograms(df):
    features = ['retweet_count', 'favorite_count', 'verified', 'follower_count']
    plt_idxs = [(0,0),(0,1),(1,0),(1,1)]

    fig, axs = plt.subplots(2, 2, figsize=(8,8), sharey=True)
    fig.suptitle('Histograms for tweet stats plotted on a log scale', fontsize=24)
    plt.ylabel('Count')
    for feature, (i, j) in zip(features, plt_idxs):
        ax = axs[i][j]
        sns.histplot(data=df, x=feature, bins=30, ax=ax).set_yscale('log')
        ax.set(xlabel=feature)

    fig.savefig(GRAPH_DIR / 'tweet_stat_histograms.png')

def tweet_stat_scatterplots(df):
    features = ['favorite_count', 'verified', 'follower_count']

    fig, axs = plt.subplots(1, 3, figsize=(15, 5), sharey=True)
    fig.suptitle('Retweet count vs. tweet statistics', fontsize=24)
    for i, feature in enumerate(features):
        ax = axs[i]
        sns.scatterplot(data=df, x=feature, y='retweet_count', alpha=0.3, ax=ax)
        ax.set(xlabel=feature.replace('_', ' '), ylabel='retweet count')

    fig.savefig(GRAPH_DIR / 'tweet_stat_scatterplots.png')

def retweet_cnt_demographic_scatterplots(df):
    per_county_df = df.groupby(['fips'], as_index=False).mean()

    # Remove counties with a mean retweet count of 0
    per_county_df = per_county_df[per_county_df['retweet_count'] > 10]

    # OLD 
    # features = ['total_population', 'median_age', 'unemployment_rate', 'per_capita_income', 'percent_high_school_graduate', 
    #             'percent_households_with_Internet', 'percent_votes_democrat', 'percent_votes_republican']
    # plt_idxs = [(0,0),(0,1),(0,2),(1,0),(1,1),(1,2),(2,0),(2,1)]

    # NEW
    features = ['total_population', 'per_capita_income', 'percent_high_school_graduate', 
                'percent_households_with_Internet', 'percent_votes_democrat', 'percent_votes_republican']
    plt_idxs = [(0,0),(0,1),(0,2),(1,0),(1,1),(1,2)]

    fig, axs = plt.subplots(2, 3, figsize=(10, 8), sharey=True)
    fig.suptitle('Avg. retweet count per county vs. county statistics', fontsize=24)
    for feature, (i,j) in zip(features, plt_idxs):
        ax = axs[i][j]
        sns.scatterplot(x=per_county_df[feature], y=per_county_df['retweet_count'], alpha=0.2, ax=ax)
        m, b = np.polyfit(per_county_df[feature], per_county_df['retweet_count'], 1)
        ax.plot(per_county_df[feature], m*per_county_df[feature] + b, label=f'{np.round(m, 3)}x + {int(b)}')
        ax.set(xlabel=feature.replace('_', ' '), ylabel='mean retweet count per county')
        ax.legend(loc='upper right')

    fig.savefig(GRAPH_DIR / 'remove_<10_demographic_scatterplots.png')

def ttest(df):
    # get different samples to compare
    per_county_df = df.groupby(['fips'], as_index=False).mean()
    dem_counties = per_county_df[per_county_df['percent_votes_democrat'] >= 50]
    rep_counties = per_county_df[per_county_df['percent_votes_republican'] > 50]

    # Remove counties with a mean retweet count less than or equal to cnt
    cnt = 1
    dem_counties = dem_counties[dem_counties['retweet_count'] > cnt]
    rep_counties = rep_counties[rep_counties['retweet_count'] > cnt]

    # perform a 2-sample unpaired ttest on the mean retweet count of democratic vs. republican counties
    tstats, pvalue = ttest_ind(dem_counties['retweet_count'], rep_counties['retweet_count'])

    print(tstats, pvalue)

df = get_data(TRAIN_DATASET)

#histograms(df)
#tweet_stat_scatterplots(df)
#retweet_cnt_demographic_scatterplots(df)
#ttest(df)

# INDEPENDENT VARIABLE OPTIONS FOR MULTIPLE REGRESSION

# demographic stats regression
#ind_vars = ['total_population', 'median_age', 'unemployment_rate', 'per_capita_income', 'percent_high_school_graduate', 'percent_households_with_Internet', 'percent_votes_democrat', 'percent_votes_republican']

# official demographic stats regression
#ind_vars = ['total_population', 'per_capita_income', 'percent_high_school_graduate', 'percent_households_with_Internet', 'percent_votes_democrat', 'percent_votes_republican']

# official demographic + tweet stats regression
#ind_vars = ['favorite_count', 'verified', 'percent_votes_republican', 'total_population', 'per_capita_income', 'percent_high_school_graduate', 'percent_households_with_Internet', 'percent_votes_democrat', 'percent_votes_republican']

# tweet stats regression
#ind_vars = ['favorite_count', 'verified', 'percent_votes_republican']

# tweet stats + 1 demographic stat regression
#ind_vars = ['favorite_count', 'verified', 'follower_count', 'percent_votes_democrat']

# simple regression
#ind_vars = 'percent_votes_republican'

remove_0 = True
multiple_regression(df, ind_vars, remove_0)