import pandas as pd
import sqlite3
import sys
sys.path.insert(1, 'code/analysis')
from pathlib import Path
from sklearn.preprocessing import MinMaxScaler
from util import regression

DATA_DIR = Path(__file__).parent.parent.parent.parent.parent / 'data'

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
    # Drop fips, county, state, columns
    df.drop(columns=['fips', 'county', 'state'], inplace=True)

    # Remove correlated features
    corr_features = set()
    corr_matrix = df.corr()

    for i in range(len(corr_matrix.columns)):
        for j in range(i):
            # Threshold set to 0.8
            if abs(corr_matrix.iloc[i, j]) > 0.8:
                feat = corr_matrix.columns[i]
                corr_features.add(feat)

    df.drop(columns=corr_features, inplace=True)

    # Normalize columns
    scaler = MinMaxScaler()
    norm = scaler.fit_transform(df.values)
    df = pd.DataFrame(norm, columns=df.columns)

    return df

def main():
    ind_var_names = ['total_population',
                    'percent_male',
                    'percent_age_10_to_14',
                    'percent_age_15_to_19',
                    'percent_age_20_to_24',
                    'percent_age_25_to_34',
                    'percent_age_35_to_44',
                    'percent_age_45_to_54',
                    'percent_age_55_to_59',
                    'percent_age_60_to_64',
                    'percent_age_65_to_74',
                    'percent_age_75_to_84',
                    'percent_age_85_and_older',
                    'percent_hispanic_or_latino',
                    'percent_white',
                    'percent_black_or_african_american',
                    'percent_american_indian_and_alaska_native',
                    'percent_asian',
                    'percent_native_hawaiian_and_other_pacific_islander',
                    'percent_other_race',
                    'percent_two_or_more_races',
                    'unemployment_rate',
                    'median_household_income',
                    'percent_health_insurance',
                    'percent_private_health_insurance',
                    'percent_public_coverage',
                    'percent_married_couple_family',
                    'percent_cohabiting_couple',
                    'percent_male_householder',
                    'percent_female_householder',
                    'avg_household_size',
                    'percent_males_never_married',
                    'percent_males_separated',
                    'percent_males_widowed',
                    'percent_males_divorced',
                    'percent_females_separated',
                    'percent_females_widowed',
                    'percent_females_divorced',
                    'percent_education_less_than_9th_grade',
                    'percent_9th_to_12th_grade_no_diploma',
                    'percent_high_school_graduate',
                    'percent_some_college_no_degree',
                    'percent_associates_degree',
                    'percent_graduate_or_professional_degree',
                    'percent_civilian_veteran',
                    'percent_with_disability',
                    'percent_foreign_born',
                    'percent_naturalized_US_citizen',
                    'percent_not_US_citizen',
                    'percent_votes_democrat',
                    'percent_votes_republican',
                    'percent_votes_libertarian',
                    'percent_votes_green',
                    'percent_votes_other']

    dep_var_names = "num_tweets"

    train_df = get_demographic_data('processed_random_train')
    test_df = get_demographic_data('processed_random_test')

    mse_train, mse_test, rsquared_val = regression(train_df, test_df, ind_var_names, dep_var_names)

    print('MSE (Train): ' + str(mse_train) + '\n' +
          'MSE (Test): ' + str(mse_test) + '\n' +
          'R-squared: ' + str(rsquared_val))

if __name__ == "__main__":
    main()
