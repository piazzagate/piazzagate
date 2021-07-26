import sys
from pathlib import Path
sys.path.insert(1, 'code/analysis')
from util import get_normalized_demographic_data, regression

def main():
    ind_var_names = ['percent_age_10_to_14',
                    'percent_age_15_to_19',
                    'percent_age_20_to_24',
                    'percent_age_25_to_34',
                    'percent_age_35_to_44',
                    'percent_age_45_to_54',
                    'percent_age_55_to_59',
                    'percent_age_60_to_64',
                    'percent_age_65_to_74',
                    'percent_age_75_to_84',
                    'percent_age_85_and_older']

    dep_var_names = "num_tweets"

    data_dir = Path(__file__).parent.parent.parent.parent.parent / 'data'

    train_df = get_normalized_demographic_data(data_dir, 'processed_random_train')
    test_df = get_normalized_demographic_data(data_dir, 'processed_random_test')

    mse_train, mse_test, rsquared_val = regression(train_df, test_df, ind_var_names, dep_var_names)

    print('MSE (Train): ' + str(mse_train) + '\n' +
          'MSE (Test): ' + str(mse_test) + '\n' +
          'R-squared: ' + str(rsquared_val))

if __name__ == "__main__":
    main()
