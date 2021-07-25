from pathlib import Path
from util import get_demographic_data, regression

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

    data_dir = Path(__file__).parent.parent.parent.parent / 'data'

    train_df = get_demographic_data(data_dir, 'processed_random_train')
    test_df = get_demographic_data(data_dir, 'processed_random_test')

    mse_train, mse_test, rsquared_val = regression(train_df, test_df, IND_VAR_NAMES, DEP_VAR_NAME)

    print('MSE (Train): ' + str(mse_train) + '\n' +
          'MSE (Test): ' + str(mse_test) + '\n' +
          'R-squared: ' + str(rsquared_val))

if __name__ == "__main__":
    main()
