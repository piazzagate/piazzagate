import pandas as pd
import sqlite3
from pathlib import Path
from sklearn.preprocessing import MinMaxScaler
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
    # Drop fips, county, state columns
    df.drop(columns=['fips', 'county', 'state'], inplace=True)

    # Normalize columns
    scaler = MinMaxScaler()
    norm = scaler.fit_transform(df)
    df = pd.DataFrame(norm, columns=df.columns)

    return df

def main():
    IND_VAR_NAMES = ['total_population',
                    'percent_male',
                    'percent_female',
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
                    'median_age',
                    'percent_hispanic_or_latino',
                    'percent_white',
                    'percent_black_or_african_american',
                    'percent_american_indian_and_alaska_native',
                    'percent_asian',
                    'percent_native_hawaiian_and_other_pacific_islander',
                    'percent_other_race',
                    'percent_two_or_more_races',
                    'total_housing_units',
                    'unemployment_rate',
                    'median_household_income',
                    'mean_household_income',
                    'per_capita_income',
                    'percent_health_insurance',
                    'percent_private_health_insurance',
                    'percent_public_coverage',
                    'percent_no_health_insurance',
                    'percent_families_income_below_poverty_line',
                    'percent_people_income_below_poverty_line',
                    'percent_married_couple_family',
                    'percent_cohabiting_couple',
                    'percent_male_householder',
                    'percent_female_householder',
                    'avg_household_size',
                    'avg_family_size',
                    'percent_males_never_married',
                    'percent_males_now_married_separated',
                    'percent_males_separated',
                    'percent_males_widowed',
                    'percent_males_divorced',
                    'percent_females_never_married',
                    'percent_females_now_married_separated',
                    'percent_females_separated',
                    'percent_females_widowed',
                    'percent_females_divorced',
                    'percent_education_less_than_9th_grade',
                    'percent_9th_to_12th_grade_no_diploma',
                    'percent_high_school_graduate',
                    'percent_some_college_no_degree',
                    'percent_associates_degree',
                    'percent_bachelors_degree',
                    'percent_graduate_or_professional_degree',
                    'percent_high_school_graduate_or_higher',
                    'percent_bachelors_degree_or_higher',
                    'percent_civilian_veteran',
                    'percent_with_disability',
                    'percent_native_born',
                    'percent_born_in_US',
                    'percent_foreign_born',
                    'percent_naturalized_US_citizen',
                    'percent_not_US_citizen',
                    'percent_households_with_computer',
                    'percent_households_with_Internet',  
                    'percent_votes_democrat',
                    'percent_votes_republican',
                    'percent_votes_libertarian',
                    'percent_votes_green',
                    'percent_votes_other']

    DEP_VAR_NAME = "num_tweets"

    train_df = get_demographic_data('processed_random_train')
    test_df = get_demographic_data('processed_random_test')

    mse_train, mse_test, rsquared_val = regression(train_df, test_df, IND_VAR_NAMES, DEP_VAR_NAME)

    print('MSE (Train): ' + str(mse_train) + '\n' +
          'MSE (Test): ' + str(mse_test) + '\n' +
          'R-squared: ' + str(rsquared_val))

if __name__ == "__main__":
    main()
