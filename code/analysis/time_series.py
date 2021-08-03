import pandas as pd
import numpy as np
import sqlite3
from pathlib import Path
import matplotlib.pyplot as plt
import seaborn as sns
import plotly_express as px
from sklearn import preprocessing
import util
import statsmodels.api as sm
from statsmodels.tsa.api import VAR




DATA_DIR = Path(__file__).parent.parent.parent / 'data'
DATASET = 'processed_time_train'
# DATASET = 'unfiltered_time_train'

def get_df(DATASET):

    conn = sqlite3.connect(DATA_DIR / 'processed' / f'{DATASET}.db')
    c = conn.cursor()

    # Get number of tweets and number of retweets on a day by county
    cmd = """SELECT *, COUNT(created_at) AS num_tweets, SUM(retweet_count) AS total_retweets FROM tweets AS t
    GROUP BY created_at, fips"""
    c.execute(cmd)
    conn.commit()

    df = pd.read_sql_query(cmd, conn)

    # Convert date (string) into datetime format
    df['created_at'] = pd.to_datetime(df['created_at'])

    cmd = """SELECT * FROM covid"""
    ds = pd.read_sql_query(cmd, conn)
    ds['date'] = pd.to_datetime(ds['date'])


    # Convert covid data from cumulative count to day-to-day count
    ds['cases_on_day'] = (
        ds.groupby(['fips'])['cases']
        .transform(lambda x: x.sub(x.shift().fillna(2*x)).abs()).fillna(0)
    )


    ds['vaccinated_on_day'] = (
        ds.groupby(['fips'])['vaccinated_count']
        .transform(lambda x: x.sub(x.shift().fillna(0)).abs())
    )

    ds['deaths_on_day'] = (
        ds.groupby(['fips'])['deaths']
        .transform(lambda x: x.sub(x.shift().fillna(0)).abs())
    )

    # Combine covid and tweets
    df = pd.merge(df, ds, how='left', left_on = ['fips', 'created_at'], right_on = ['fips', 'date']).fillna(0).drop('date', axis=1)
    return df

df=get_df(DATASET)

def histograms(df):
    features = ['cases_on_day', 'deaths_on_day', 'vaccinated_on_day']
    fig, axs = plt.subplots(1, 3, figsize=(20,5))
    fig.suptitle('Histograms for covid data plotted on a log scale', fontsize=24)
    plt.ylabel('Count')
    for i, feature in enumerate(features):
        ax = axs[i]
        sns.histplot(data=df, x=feature, bins=30, ax=ax).set_yscale('log')
        ax.set(xlabel=feature)

    fig.savefig(DATA_DIR / 'analysis' / 'covid_histograms.png')

# histograms(ds)

 
def line_plot(df, area):
    fig, ax_left = plt.subplots(figsize=(20,5))
    ax_right = ax_left.twinx()
    ax_right2= ax_left.twinx()
    ax_right3= ax_left.twinx()
    ax_right2.spines["right"].set_position(("axes", 1.2))
    rspine = ax_right2.spines['right']
    rspine.set_position(('axes', 1.15))
    ax_right2.set_frame_on(True)
    ax_right2.patch.set_visible(False)
    fig.subplots_adjust(right=0.7)
    ax_right3.spines["right"].set_position(("axes", 1.3))
    rspine = ax_right3.spines['right']
    rspine.set_position(('axes', 1.25))
    ax_right3.set_frame_on(True)
    ax_right3.patch.set_visible(False)
    fig.subplots_adjust(right=0.7)

    ax_left.plot(df['created_at'], df['cases_on_day'], color='green', label = 'cases', linestyle= '--')
    ax_right2.plot(df['created_at'], df['num_tweets'], color='cornflowerblue', label= 'tweets')
    ax_right2.lines[0].set_linewidth(3)
    ax_right3.plot(df['created_at'], df['deaths_on_day'], color='red', label= 'deaths', linestyle= '--')
    ax_right.plot(df['created_at'], df['vaccinated_on_day'], color='darkorange', label= 'vaccines')

    # if area != 'Texas':
    #     ax_right.plot(df['created_at'], df['vaccinated_on_day'], color='orchid', label= 'vaccines')
    #     l2, lb2 = ax_right.get_legend_handles_labels()
    #     ax_right.set_ylabel("No. of covid vaccinations")

    l1, lb1 = ax_left.get_legend_handles_labels()
    l3, lb3 = ax_right2.get_legend_handles_labels()
    l4, lb4 = ax_right3.get_legend_handles_labels()
    l2, lb2 = ax_right.get_legend_handles_labels()

    # if area != 'Texas':
    lines = l3 + l1 + l4 + l2
    labels = lb3 + lb1 + lb4 + lb2

    # else:
    #     lines = l1 + l3 + l4
    #     labels = lb1 + lb3 + lb4

    ax_left.legend(lines, labels, loc='upper left')
    ax_left.set_ylabel("No. of covid cases")
    ax_right2.set_ylabel("No. of covid tweets")
    ax_right3.set_ylabel("No. of covid-related deaths")
    ax_right.set_ylabel("No. of covid vaccinations")
    ax_left.plot()
    plt.title('Monthly covid cases, vaccinations, deaths and tweets in ' + area)
    outfile = 'covid_' + area.replace(' ', '_') + '.png'
    plt.savefig(DATA_DIR / 'analysis' / outfile)


# Get monthly data by county
dff = df.groupby(['fips', pd.Grouper(key='created_at', freq='M')]).sum().reset_index()

# Plot monthly data in LA and DC county
# line_plot(dff[dff['fips'] == 6037], 'LA county')
# line_plot(dff[dff['fips'] == 11001], 'DC county')



    # -------------------------------------------------------------------------------- #
    #                                  SCATTERPLOTS                                    #
    # -------------------------------------------------------------------------------- #

# Daily data, across the US
# fig = px.scatter(df.groupby('created_at', as_index=False).sum()
#                  , x='num_tweets'
#                  , y=['cases_on_day', 'deaths_on_day', 'vaccinated_on_day']
#                  , trendline='ols'
#                  )
# fig.write_image(DATA_DIR / 'analysis' / 'daily_covid_tweet_OLS.png')

def scatt_county():
    # Daily scatterplots for data by county
    df=get_df(DATASET)
    fig, axs = plt.subplots(3, 1, figsize=(8,8))
    ax=axs[0]
    df.plot(ax=ax, x = 'cases_on_day', y = 'num_tweets', kind = 'scatter', xlabel='No. of cases', ylabel='No. of tweets')

    ax=axs[1]
    df.plot(ax=ax, x = 'deaths_on_day', y = 'num_tweets', kind = 'scatter', xlabel='No. of deaths', ylabel='No. of tweets')

    ax=axs[2]
    df.plot(ax=ax, x = 'vaccinated_on_day', y = 'num_tweets', kind = 'scatter', xlabel='No. of vaccinations', ylabel='No. of tweets')

    fig.suptitle('Scatterplots for daily covid data by county')
    fig.tight_layout()
    fig.savefig(DATA_DIR / 'analysis' / 'daily_scatterplots_county.png')

# scatt_county()

def scatt_US():

    # Daily scatterplots for data across the US
    df=get_df(DATASET)
    d = df.groupby('created_at', as_index=False).sum()
    fig, axs = plt.subplots(3, 1, figsize=(8,8))
    ax=axs[0]
    d.plot(ax=ax, x = 'cases_on_day', y = 'num_tweets', kind = 'scatter', xlabel='No. of cases', ylabel='No. of tweets', alpha=0.3)

    ax=axs[1]
    d.plot(ax=ax, x = 'deaths_on_day', y = 'num_tweets', kind = 'scatter', xlabel='No. of deaths', ylabel='No. of tweets', alpha=0.3)

    ax=axs[2]
    d.plot(ax=ax, x = 'vaccinated_on_day', y = 'num_tweets', kind = 'scatter', xlabel='No. of vaccinations', ylabel='No. of tweets', alpha=0.3)

    fig.suptitle('Scatterplots for daily covid data across the US')
    fig.tight_layout()
    fig.savefig(DATA_DIR / 'analysis' / 'daily_scatterplots_US.png')

# scatt_US()

def rolling_avg():

    df=get_df(DATASET)
    d = df.groupby('created_at', as_index=False).sum()

    # Rolling data / 7-day average, across the US
    rolling_avg = d.rolling(7, min_periods=1).mean()
    d['tweets_rolling'] = rolling_avg['num_tweets']
    d['cases_rolling'] = rolling_avg['cases_on_day']
    d['vaccines_rolling'] = rolling_avg['vaccinated_on_day']
    d['deaths_rolling'] = rolling_avg['deaths_on_day']

    # d=d[d['vaccines_rolling'] != 0]
    # d=d[d['tweets_rolling'] != 0]
    # d=d[d['cases_rolling'] != 0]
    # d=d[d['deaths_rolling'] != 0]


    fig, axs = plt.subplots(3, 1, figsize=(8,8))
    ax=axs[0]
    d.plot(ax=ax, x = 'cases_rolling', y = 'tweets_rolling', kind = 'scatter', xlabel='No. of cases', ylabel='No. of tweets', alpha=0.3)
    m, b = np.polyfit(d['cases_rolling'], d['tweets_rolling'], 1)
    ax.plot(d['cases_rolling'], m*d['cases_rolling'] + b, label=f'{np.round(m, 3)}x + {int(b)}')
    ax.legend(loc='upper right')


    ax=axs[1]
    d.plot(ax=ax, x = 'deaths_rolling', y = 'tweets_rolling', kind = 'scatter', xlabel='No. of deaths', ylabel='No. of tweets', alpha=0.3)
    m, b = np.polyfit(d['deaths_rolling'], d['tweets_rolling'], 1)
    ax.plot(d['deaths_rolling'], m*d['deaths_rolling'] + b, label=f'{np.round(m, 3)}x + {int(b)}')
    ax.legend(loc='upper right')

    ax=axs[2]
    d.plot(ax=ax, x = 'vaccines_rolling', y = 'tweets_rolling', kind = 'scatter', xlabel='No. of vaccinations', ylabel='No. of tweets', alpha=0.3)
    m, b = np.polyfit(d['vaccines_rolling'], d['tweets_rolling'], 1)
    ax.plot(d['vaccines_rolling'], m*d['vaccines_rolling'] + b, label=f'{np.round(m, 3)}x + {int(b)}')
    ax.legend(loc='upper right')


    fig.suptitle('Scatterplots for 7-day rolling average of covid data across the US')
    fig.tight_layout()
    fig.savefig(DATA_DIR / 'analysis' / 'rolling_scatterplots_US.png')
    # line_plot(d, 'the US- rolling average')


    # Rolling data / 7-day average, by county
    rolling_avg = df.rolling(7, min_periods=1).mean()
    df['tweets_rolling'] = rolling_avg['num_tweets']
    df['cases_rolling'] = rolling_avg['cases_on_day']
    df['vaccines_rolling'] = rolling_avg['vaccinated_on_day']
    df['deaths_rolling'] = rolling_avg['deaths_on_day']


    fig, axs = plt.subplots(3, 1, figsize=(8,8))
    ax=axs[0]
    df.plot(ax=ax, x = 'cases_rolling', y = 'tweets_rolling', kind = 'scatter', xlabel='No. of cases', ylabel='No. of tweets', alpha=0.3)

    ax=axs[1]
    df.plot(ax=ax, x = 'deaths_rolling', y = 'tweets_rolling', kind = 'scatter', xlabel='No. of deaths', ylabel='No. of tweets', alpha=0.3)

    ax=axs[2]
    df.plot(ax=ax, x = 'vaccines_rolling', y = 'tweets_rolling', kind = 'scatter', xlabel='No. of vaccinations', ylabel='No. of tweets', alpha=0.3)

    fig.suptitle('Scatterplots for 7-day rolling average for covid data by county')
    fig.tight_layout()
    fig.savefig(DATA_DIR / 'analysis' / 'rolling_scatterplots_county.png')

rolling_avg()


    # -------------------------------------------------------------------------------- #
    #                                   DATA BY STATE                                  #
    # -------------------------------------------------------------------------------- #

def state_data():
    df=get_df(DATASET)

    # Get the state fips for each row
    df['state_fips'] = df['fips'].apply(lambda x: str(x)[:len(str(x))-3])


    df = df.drop(columns=['id', 'city', 'user_id', 'fips', 'is_retweet', 'text', 'original_tweet_id', 'lang', 'cases', 'deaths', 'vaccinated_count', 'vaccinated_percent'])
    df = df.groupby(['state_fips','created_at'], as_index=False).sum()


        # -------------------------------------------------------------------------------- #
        #                            DATA PER MONTH, BY STATE                              #
        # -------------------------------------------------------------------------------- #

    df = df.groupby(['state_fips', pd.Grouper(key='created_at', freq='M')]).sum().reset_index()

    line_plot(df.groupby('created_at', as_index=False).sum(), 'the US')
    # line_plot(df[df['state_fips'] == '6'], 'California')
    # line_plot(df[df['state_fips'] == '36'], 'New York')
    # line_plot(df[df['state_fips'] == '48'], 'Texas')
    # line_plot(df[df['state_fips'] == '12'], 'Florida')

    #  Scatter plot, monthly data, by state
    fig, axs = plt.subplots(3, 1, figsize=(8,8))
    ax=axs[0]
    df.plot(ax=ax, x = 'cases_on_day', y = 'num_tweets', kind = 'scatter', xlabel='No. of cases', ylabel='No. of tweets')

    ax=axs[1]
    df.plot(ax=ax, x = 'deaths_on_day', y = 'num_tweets', kind = 'scatter', xlabel='No. of deaths', ylabel='No. of tweets')

    ax=axs[2]
    df.plot(ax=ax, x = 'vaccinated_on_day', y = 'num_tweets', kind = 'scatter', xlabel='No. of vaccinations', ylabel='No. of tweets')

    fig.suptitle('Scatterplots for monthly covid data by state')
    fig.tight_layout()
    fig.savefig(DATA_DIR / 'analysis' / 'monthly_scatterplots_states.png')

    # Group data across the US
    df=df.groupby('created_at', as_index=False).sum()

    #  Scatter plot, monthly data
    fig, axs = plt.subplots(3, 1, figsize=(8,8))
    ax=axs[0]
    df.plot(ax=ax, x = 'cases_on_day', y = 'num_tweets', kind = 'scatter', xlabel='No. of cases', ylabel='No. of tweets', alpha=0.3)

    ax=axs[1]
    df.plot(ax=ax, x = 'deaths_on_day', y = 'num_tweets', kind = 'scatter', xlabel='No. of deaths', ylabel='No. of tweets',alpha=0.3)

    ax=axs[2]
    df.plot(ax=ax, x = 'vaccinated_on_day', y = 'num_tweets', kind = 'scatter', xlabel='No. of vaccinations', ylabel='No. of tweets',alpha=0.3)

    fig.suptitle('Scatterplots for monthly covid data across the US')
    fig.tight_layout()
    fig.savefig(DATA_DIR / 'analysis' / 'monthly_scatterplots_US.png')


    #monthly country-wide data
    # fig = px.scatter(df.groupby('created_at', as_index=False).sum()
    #                 , x='num_tweets'
    #                 , y=['cases_on_day', 'deaths_on_day', 'vaccinated_on_day']
    #                 , trendline='ols'
    #                 )
    # fig.write_image(DATA_DIR / 'analysis' / 'monthly_covid_tweet_OLS.png')

state_data()



    # -------------------------------------------------------------------------------- #
    #                                     COVID REGRESSION                             #
    # -------------------------------------------------------------------------------- #

TRAIN_DATASET = 'processed_random_train'
TEST_DATASET = 'processed_random_test'

def get_data(dataset):
    df=get_df(dataset)
    # df = df.groupby(['fips', pd.Grouper(key='created_at', freq='M')]).sum().reset_index()
    # df = df.groupby('created_at', as_index=False).sum()

    d = df.groupby('created_at', as_index=False).sum()

    # Rolling data / 7-day average, across the US
    rolling_avg = d.rolling(7, min_periods=1).mean()
    d['num_tweets'] = rolling_avg['num_tweets']
    d['cases_on_day'] = rolling_avg['cases_on_day']
    d['vaccinated_on_day'] = rolling_avg['vaccinated_on_day']
    d['deaths_on_day'] = rolling_avg['deaths_on_day']

    df=d

    # df = df[df['cases_on_day'] != 0]
    # df = df[df['deaths_on_day'] != 0]
    df = df[df['vaccinated_on_day'] != 0]
    # df = df[df['num_tweets'] != 0]

    return df[['cases_on_day', 'deaths_on_day', 'vaccinated_on_day','num_tweets']]
    

def covid_regression():
    min_max_scaler = preprocessing.MinMaxScaler()

    train_data = pd.DataFrame(min_max_scaler.fit_transform(get_data(TRAIN_DATASET).values), 
                    columns=['cases_on_day', 'deaths_on_day', 'vaccinated_on_day', 'num_tweets'])
    test_data = pd.DataFrame(min_max_scaler.fit_transform(get_data(TEST_DATASET).values), 
                    columns=['cases_on_day', 'deaths_on_day', 'vaccinated_on_day', 'num_tweets'])

    ind_vars = ['cases_on_day', 'deaths_on_day', 'vaccinated_on_day']
    dep_var = 'num_tweets'
    mse_train, mse_test, rsquared_val = util.regression(train_data, test_data, ind_vars, dep_var)
    print(mse_train, mse_test, rsquared_val)

# covid_regression()

df=get_data(DATASET)

# plt.show()

def var_model():
    model = VAR(df)
    # results_aic = []
    # for p in range(1,10):
    #     results = model.fit(p)
    #     results_aic.append(results.aic)
    # sns.set()
    # plt.plot(list(np.arange(1,10,1)), results_aic)
    # plt.xlabel("Order")
    # plt.ylabel("AIC")
    # plt.show()
    
    # 9 gives smallest AIC

    results=model.fit(9)
    print(results.summary())

var_model()

