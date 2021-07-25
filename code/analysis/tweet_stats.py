import sqlite3
from pathlib import Path
import pandas as pd
from sklearn import preprocessing
import seaborn as sns
import matplotlib.pyplot as plt
import util
sns.set_theme(style='darkgrid')

DATA_DIR = Path(__file__).parent.parent.parent / 'data'
TRAIN_DATASET = 'processed_random_train'
TEST_DATASET = 'processed_random_test'

def get_tweet_author_data(dataset):
    conn = sqlite3.connect(DATA_DIR / 'processed' / f'{dataset}.db')
    cmd = '''SELECT tweets.retweet_count, users.verified, users.follower_count, demographics.total_population, demographics.median_age, demographics.percent_votes_democrat, demographics.percent_votes_republican
    FROM tweets JOIN users ON tweets.user_id = users.id JOIN demographics on tweets.fips = demographics.fips'''

    df = pd.read_sql_query(cmd, conn).fillna(0)

    return df

def tweet_stats_regression():
    min_max_scaler = preprocessing.MinMaxScaler()

    train_data = pd.DataFrame(min_max_scaler.fit_transform(get_tweet_author_data(TRAIN_DATASET).values), 
                    columns=['retweet_count', 'verified', 'follower_count', 'total_population', 'median_age', 'percent_votes_democrat', 'percent_votes_republican'])
    test_data = pd.DataFrame(min_max_scaler.fit_transform(get_tweet_author_data(TEST_DATASET).values), 
                    columns=['retweet_count', 'verified', 'follower_count', 'total_population', 'median_age', 'percent_votes_democrat', 'percent_votes_republican'])

    ind_vars = ['verified', 'follower_count', 'median_age', 'percent_votes_democrat', 'percent_votes_republican']
    dep_var = 'retweet_count'
    mse_train, mse_test, rsquared_val = util.regression(train_data, test_data, ind_vars, dep_var)
    print(mse_train, mse_test, rsquared_val)

def histogram(feature):
    df = get_tweet_author_data(TRAIN_DATASET)

    fig, ax = plt.subplots()
    sns.histplot(data=df, x=feature, bins=30)
    ax.set(xlabel=feature)

    outfile = feature + '.png'
    fig.savefig(DATA_DIR / 'analysis' / outfile)


tweet_stats_regression()
#histogram('retweet_count')
#histogram('follower_count')
#histogram('tweet_count')