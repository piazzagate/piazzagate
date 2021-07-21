import pandas as pd
import sqlite3
from pathlib import Path
from util import regression

DATA_DIR = Path(__file__).parent.parent.parent.parent / 'data'

def get_demographic_data(dataset):
    conn = sqlite3.connect(DATA_DIR / 'processed' / f'{dataset}.db')

    cmd = '''SELECT
                *,
                (SELECT COUNT(t.fips)
                 FROM tweets AS t
                 WHERE t.fips = d.fips) AS num_tweets
            FROM demographics AS d'''

    # Replace NULL values with 0
    df = pd.read_sql_query(cmd, conn).fillna(0)
    # Drop fips, county, state, total_population columns
    df.drop(columns=['fips', 'county', 'state', 'total_population'], inplace=True)

    return df

def main():
    IND_VAR_NAMES = ['percent_hispanic_or_latino',
                    'percent_white',
                    'percent_black_or_african_american',
                    'percent_american_indian_and_alaska_native',
                    'percent_asian',
                    'percent_native_hawaiian_and_other_pacific_islander',
                    'percent_other_race',
                    'percent_two_or_more_races']

    DEP_VAR_NAME = "num_tweets"

    train_df = get_demographic_data('processed_random_train')
    test_df = get_demographic_data('processed_random_test')

    mse_train, mse_test, rsquared_val = regression(train_df, test_df, IND_VAR_NAMES, DEP_VAR_NAME)

    print('MSE (Train): ' + str(mse_train) + '\n' +
          'MSE (Test): ' + str(mse_test) + '\n' +
          'R-squared: ' + str(rsquared_val))

if __name__ == "__main__":
    main()
