from pathlib import Path
from util import get_demographic_data, regression

def main():
    IND_VAR_NAMES = ['percent_education_less_than_9th_grade',
                    'percent_9th_to_12th_grade_no_diploma',
                    'percent_high_school_graduate',
                    'percent_some_college_no_degree',
                    'percent_associates_degree',
                    'percent_bachelors_degree',
                    'percent_graduate_or_professional_degree',
                    'percent_high_school_graduate_or_higher',
                    'percent_bachelors_degree_or_higher']

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
