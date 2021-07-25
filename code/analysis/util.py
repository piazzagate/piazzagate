import matplotlib.pyplot as plt
import pandas as pd
import sqlite3
from sklearn.linear_model import LinearRegression
from sklearn.metrics import mean_squared_error, r2_score
from sklearn.preprocessing import MinMaxScaler

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

    # Normalize columns
    scaler = MinMaxScaler()
    norm = scaler.fit_transform(df.values)
    df = pd.DataFrame(norm, columns=df.columns)

    return df

def regression(train_df, test_df, ind_var_names, dep_var_name):
    X_train = train_df[ind_var_names]
    X_test = test_df[ind_var_names]
    y_train = train_df[dep_var_name]
    y_test = test_df[dep_var_name]

    reg = LinearRegression().fit(X_train, y_train)

    train_pred_vals = reg.predict(X_train)
    mse_train = mean_squared_error(y_train, train_pred_vals)

    test_pred_vals = reg.predict(X_test)
    mse_test = mean_squared_error(y_test, test_pred_vals)

    rsquared_val = r2_score(y_test, test_pred_vals)

    # Plot outputs
    # plt.scatter(X_test, y_test,  color='black')
    # plt.plot(X_test, test_pred_vals, color='blue', linewidth=3)

    # plt.xticks(())
    # plt.yticks(())

    # plt.show()
    
    return mse_train, mse_test, rsquared_val