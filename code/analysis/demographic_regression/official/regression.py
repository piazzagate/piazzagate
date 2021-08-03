import sys
from pathlib import Path
sys.path.insert(1, 'code/analysis')
from util import get_normalized_demographic_data, regression

def main():
    ind_var_names = ['total_population',
                     'per_capita_income',
                     'percent_high_school_graduate',
                     'percent_households_with_Internet',
                     'percent_votes_democrat',
                     'percent_votes_republican']

    dep_var_names = "tweet_rate"

    data_dir = Path(__file__).parent.parent.parent.parent.parent / 'data'

    df = get_normalized_demographic_data(data_dir, 'processed_random_train')

    regression(df, ind_var_names, dep_var_names)

if __name__ == "__main__":
    main()
