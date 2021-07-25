import sys
from pathlib import Path
sys.path.insert(1, 'code/analysis')
from util import get_demographic_data, regression

def main():
    ind_var_names = ['percent_health_insurance',
                    'percent_private_health_insurance',
                    'percent_public_coverage',
                    'percent_no_health_insurance']

    dep_var_names = "num_tweets"

    data_dir = Path(__file__).parent.parent.parent.parent.parent / 'data'

    train_df = get_demographic_data(data_dir, 'processed_random_train')
    test_df = get_demographic_data(data_dir, 'processed_random_test')

    mse_train, mse_test, rsquared_val = regression(train_df, test_df, ind_var_names, dep_var_names)

    print('MSE (Train): ' + str(mse_train) + '\n' +
          'MSE (Test): ' + str(mse_test) + '\n' +
          'R-squared: ' + str(rsquared_val))

if __name__ == "__main__":
    main()
