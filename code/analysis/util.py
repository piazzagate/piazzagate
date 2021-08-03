import pandas as pd
import sqlite3
import statsmodels.api as sm
from sklearn.metrics import r2_score
from sklearn.preprocessing import MinMaxScaler
from statsmodels.tools import eval_measures

def get_demographic_data(data_dir, dataset):
    conn = sqlite3.connect(data_dir / 'processed' / f'{dataset}.db')

    cmd = '''SELECT
                *,
                (SELECT COUNT(t.fips) * 1000000 / d.total_population
                 FROM tweets AS t
                 WHERE t.fips = d.fips) AS tweet_rate
            FROM demographics AS d
            WHERE tweet_rate != 0'''

    # Drop NULL values
    df = pd.read_sql_query(cmd, conn).dropna()
    # Drop fips, county, state columns
    df.drop(columns=['fips', 'county', 'state'], inplace=True)

    return df

def get_normalized_demographic_data(data_dir, dataset):
    conn = sqlite3.connect(data_dir / 'processed' / f'{dataset}.db')

    cmd = '''SELECT
                *,
                (SELECT COUNT(t.fips) * 1000000 / d.total_population
                 FROM tweets AS t
                 WHERE t.fips = d.fips) AS tweet_rate
            FROM demographics AS d
            WHERE tweet_rate != 0'''

    # Drop NULL values
    df = pd.read_sql_query(cmd, conn).dropna()
    # Drop fips, county, state columns
    df.drop(columns=['fips', 'county', 'state'], inplace=True)

    # Normalize columns
    scaler = MinMaxScaler()
    norm = scaler.fit_transform(df.values)
    df = pd.DataFrame(norm, columns=df.columns)

    return df

def regression(df, ind_var_names, dep_var_name):
    X = df[ind_var_names].to_numpy()
    y = df[dep_var_name].to_numpy()
    
    X = sm.add_constant(X)

    mod = sm.OLS(y, X)
    res = mod.fit()

    print(ind_var_names)
    print(res.summary())
