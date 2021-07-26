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
                (SELECT COUNT(t.fips)
                 FROM tweets AS t
                 WHERE t.fips = d.fips) AS num_tweets
            FROM demographics AS d'''

    # Replace NULL values with 0
    df = pd.read_sql_query(cmd, conn).fillna(0)
    # Drop fips, county, state columns
    df.drop(columns=['fips', 'county', 'state'], inplace=True)

    return df

def get_normalized_demographic_data(data_dir, dataset):
    conn = sqlite3.connect(data_dir / 'processed' / f'{dataset}.db')

    cmd = '''SELECT
                *,
                (SELECT COUNT(t.fips)
                    FROM tweets AS t
                    WHERE t.fips = d.fips) AS num_tweets
            FROM demographics AS d'''

    # Replace NULL values with 0
    df = pd.read_sql_query(cmd, conn).fillna(0)
    # Drop fips, county, state columns
    df.drop(columns=['fips', 'county', 'state'], inplace=True)

    # Normalize columns
    scaler = MinMaxScaler()
    norm = scaler.fit_transform(df.values)
    df = pd.DataFrame(norm, columns=df.columns)

    return df

def regression(train_df, test_df, ind_var_names, dep_var_name):
    X_train = train_df[ind_var_names].to_numpy()
    X_test = test_df[ind_var_names].to_numpy()
    y_train = train_df[dep_var_name].to_numpy()
    y_test = test_df[dep_var_name].to_numpy()
    
    X_train = sm.add_constant(X_train)
    X_test = sm.add_constant(X_test)

    mod = sm.OLS(y_train, X_train)
    res = mod.fit()

    train_pred_vals = res.predict(X_train)
    mse_train = eval_measures.mse(y_train, train_pred_vals)

    test_pred_vals = res.predict(X_test)
    mse_test = eval_measures.mse(y_test, test_pred_vals)

    rsquared_val = r2_score(y_test, test_pred_vals)

    print(res.summary())
    
    return mse_train, mse_test, rsquared_val