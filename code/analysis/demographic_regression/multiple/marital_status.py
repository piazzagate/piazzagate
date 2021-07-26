import sys
from pathlib import Path
sys.path.insert(1, 'code/analysis')
from util import get_normalized_demographic_data, regression

def main():
    ind_var_names = ['percent_males_never_married',
                    'percent_males_now_married_separated',
                    'percent_males_separated',
                    'percent_males_widowed',
                    'percent_males_divorced',
                    'percent_females_never_married',
                    'percent_females_now_married_separated',
                    'percent_females_separated',
                    'percent_females_widowed',
                    'percent_females_divorced']

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
