import sqlite3
from pathlib import Path
import pandas as pd
from sklearn import preprocessing
import seaborn as sns
import matplotlib.pyplot as plt
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

def tweet_stats_regression(df, outfile):
    min_max_scaler = preprocessing.MinMaxScaler()

    train_data = pd.DataFrame(min_max_scaler.fit_transform(df.values), 
                    columns=['fips', 'retweet_count', 'favorite_count', 'verified', 'follower_count', 'total_population', 
                        'median_age', 'unemployment_rate', 'per_capita_income', 'percent_high_school_graduate', 
                        'percent_households_with_Internet', 'percent_votes_democrat', 'percent_votes_republican'])
    test_data = pd.DataFrame(min_max_scaler.fit_transform(df.values), 
                    columns=['fips', 'retweet_count', 'favorite_count', 'verified', 'follower_count', 'total_population', 
                        'median_age', 'unemployment_rate', 'per_capita_income', 'percent_high_school_graduate', 
                        'percent_households_with_Internet', 'percent_votes_democrat', 'percent_votes_republican'])
    # independent variables for tweet and demographic stats regression
    #ind_vars = ['favorite_count', 'verified', 'follower_count', 'total_population', 'median_age', 'unemployment_rate', 
    #            'per_capita_income', 'percent_high_school_graduate', 'percent_households_with_Internet', 'percent_votes_democrat', 'percent_votes_republican']
    # independent variables for tweet stats regression
    ind_vars = ['favorite_count', 'verified', 'follower_count']
    dep_var = 'retweet_count'
    reg, mse_train, mse_test, rsquared_val = util.regression(train_data, test_data, ind_vars, dep_var)

    # f = open(outfile, 'w')
    # f.write(reg.summary().to_text())
    # f.close()

def histograms():
    features = ['retweet_count', 'favorite_count', 'verified', 'follower_count']
    plt_idxs = [(0,0),(0,1),(1,0),(1,1)]

    fig, axs = plt.subplots(2, 2, figsize=(8,8), sharey=True)
    fig.suptitle('Histograms for tweet stats plotted on a log scale')
    plt.ylabel('Count')
    for feature, (i, j) in zip(features, plt_idxs):
        ax = axs[i][j]
        sns.histplot(data=df, x=feature, bins=30, ax=ax).set_yscale('log')
        ax.set(xlabel=feature)

    fig.savefig(GRAPH_DIR / 'tweet_stat_histograms.png')

df = get_data(TRAIN_DATASET)

#tweet_stats_regression(df, GRAPH_DIR / 'tweet_stats_regression')
histograms()