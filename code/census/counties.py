import pandas as pd
from pathlib import Path
import sqlite3

DATA_DIR = Path(__file__).parent.parent.parent / 'data'
DATASET = 'counties'
COUNTY_DATA = {}
ALASKAN_DISTRICTS = {}

def setup_database(filename=f'{DATASET}.db'):
    # Create connection to database
    conn = sqlite3.connect(DATA_DIR / 'processed' / filename)
    c = conn.cursor()

    counties_table_cmd = """
        CREATE TABLE IF NOT EXISTS county_data (
            fips_code INT PRIMARY KEY NOT NULL,
            county TEXT,
            state TEXT,

            total_population INT,
            percent_male REAL,
            percent_female REAL,
            percent_age_10_to_14 REAL,
            percent_age_15_to_19 REAL,
            percent_age_20_to_24 REAL,
            percent_age_25_to_34 REAL,
            percent_age_35_to_44 REAL,
            percent_age_45_to_54 REAL,
            percent_age_55_to_59 REAL,
            percent_age_60_to_64 REAL,
            percent_age_65_to_74 REAL,
            percent_age_75_to_84 REAL,
            percent_age_85_and_older REAL,
            median_age REAL,
            percent_hispanic_or_latino REAL,
            percent_white REAL,
            percent_black_or_african_american REAL,
            percent_american_indian_and_alaska_native REAL,
            percent_asian REAL,
            percent_native_hawaiian_and_other_pacific_islander REAL,
            percent_other_race REAL,
            percent_two_or_more_races REAL,
            total_housing_units INT,

            unemployment_rate REAL,
            median_household_income INT,
            mean_household_income INT,
            per_capita_income INT,
            percent_health_insurance REAL,
            percent_private_health_insurance REAL,
            percent_public_coverage REAL,
            percent_no_health_insurance REAL,
            percent_families_income_below_poverty_line REAL,
            percent_people_income_below_poverty_line REAL,

            percent_married_couple_family REAL,
            percent_cohabiting_couple REAL,
            percent_male_householder REAL,
            percent_female_householder REAL,
            avg_household_size REAL,
            avg_family_size REAL,
            percent_males_never_married REAL,
            percent_males_now_married_separated REAL,
            percent_males_separated REAL,
            percent_males_widowed REAL,
            percent_males_divorced REAL,
            percent_females_never_married REAL,
            percent_females_now_married_separated REAL,
            percent_females_separated REAL,
            percent_females_widowed REAL,
            percent_females_divorced REAL,
            percent_education_less_than_9th_grade REAL,
            percent_9th_to_12th_grade_no_diploma REAL,
            percent_high_school_graduate REAL,
            percent_some_college_no_degree REAL,
            percent_associates_degree REAL,
            percent_bachelors_degree REAL,
            percent_graduate_or_professional_degree REAL,
            percent_high_school_graduate_or_higher REAL,
            percent_bachelors_degree_or_higher REAL,
            percent_civilian_veteran REAL,
            percent_with_disability REAL,
            percent_native_born REAL,
            percent_born_in_US REAL,
            percent_foreign_born REAL,
            percent_naturalized_US_citizen REAL,
            percent_not_US_citizen REAL,
            percent_households_with_computer REAL,
            percent_households_with_Internet REAL,

            percent_votes_democrat REAL,
            percent_votes_republican REAL,
            percent_votes_libertarian REAL,
            percent_votes_green REAL,
            percent_votes_other REAL
        );
    """
    c.execute(counties_table_cmd)
    conn.commit()

    return c, conn

def load_demographic_housing_data():
    path = DATA_DIR / 'raw' /  'census' / 'demographic_housing.csv'
    df = pd.read_csv(path)
    df = df.iloc[:,
        [0, 1, 2, 8, 12, 28, 32, 36, 40, 44, 48, 52, 56, 60, 64,
        68, 70, 284, 308, 312, 316, 320, 324, 328, 332, 342]]
    demographic_housing_header = [
            'county',
            'state',
            'total_population',
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
            'total_housing_units']

    for _, row in df.iloc[1:, :].iterrows():
        fips_code = int(row[0][-5:])

        county_state = row[1]
        demographic_housing_vals = county_state.split(', ')

        total_pop = int(row[2])
        demographic_housing_vals.append(total_pop)

        for datum in row[3:-1]:
            # Each datum is a percent
            demographic_housing_vals.append(float(datum))

        total_housing_units = int(row[-1])
        demographic_housing_vals.append(total_housing_units)

        COUNTY_DATA[fips_code] = dict(zip(
            demographic_housing_header,
            demographic_housing_vals))

def load_economic_characteristics_data():
    path = DATA_DIR / 'raw' /  'census' / 'economic_charact.csv'

    df = pd.read_csv(path)
    df = df.iloc[:, [0, 36, 246, 250, 350, 384, 388, 392, 396, 476, 512]]
    economic_charact_header = [
            'unemployment_rate',
            'median_household_income',
            'mean_household_income',
            'per_capita_income',
            'percent_health_insurance',
            'percent_private_health_insurance',
            'percent_public_coverage',
            'percent_no_health_insurance',
            'percent_families_income_below_poverty_line',
            'percent_people_income_below_poverty_line']

    for _, row in df.iloc[1:, :].iterrows():
        fips_code = int(row[0][-5:])
        economic_charact_vals = []

        for idx in range(1, len(row)):
            if idx in [2, 3, 4]:
                economic_charact_vals.append(int(row[idx]))
            else:
                economic_charact_vals.append(float(row[idx]))

        economic_charact_data = dict(zip(economic_charact_header, economic_charact_vals))

        COUNTY_DATA[fips_code].update(economic_charact_data)

def load_social_characteristics_data():
    path = DATA_DIR / 'raw' / 'census' / 'social_charact.csv'

    df = pd.read_csv(path)
    df = df.iloc[:,
        [0, 8, 16, 24, 40, 62, 66, 104, 108, 112, 116, 120, 128, 132, 136, 140, 144, 240, 244,
        248, 252, 256, 260, 264, 268, 272, 280, 288, 352, 356, 372, 380, 384, 608, 612]]

    social_charact_header = [
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
        'percent_households_with_Internet']

    for _, row in df.iloc[1:, :].iterrows():
        fips_code = int(row[0][-5:])
        social_charact_vals = []

        for datum in row[1:]:
            if datum == '-':
                social_charact_vals.append(0.0)
            else:
                social_charact_vals.append(float(datum))

        social_charact_data = dict(zip(social_charact_header, social_charact_vals))

        COUNTY_DATA[fips_code].update(social_charact_data)

def load_election_data():
    path = DATA_DIR / 'raw' / 'census' / 'pres_election.csv'

    df = pd.read_csv(path)
    # 2020 election
    df = df.loc[df['year'] == 2020]
    df = df.loc[:, ['county_fips', 'party', 'candidatevotes', 'totalvotes']]

    election_header = [
        'percent_votes_democrat',
        'percent_votes_republican',
        'percent_votes_libertarian',
        'percent_votes_green',
        'percent_votes_other']

    election_data = dict.fromkeys(election_header, 0.0)

    # Create fields for election data for each county
    for county in COUNTY_DATA.values():
        county.update(election_data)

    prev_fips = None
    prev_party = None
    candidate_votes = 0

    # Update each county
    for _, row in df.iterrows():
        # Fips code for District of Columbia is null in MIT dataset
        if pd.isna(row[0]):
            fips_code = 11001
        # Fips code for Oglala Lakota County is incorrect in MIT dataset
        elif row[0] == 46113:
            fips_code = 46102
        else:
            fips_code = int(row[0])

        party = row[1]
        total_votes = row[3]

        # District in Alaska
        if 2001 <= fips_code <= 2040:
            percent = round(row[2] / total_votes * 100, 2)
            if fips_code not in ALASKAN_DISTRICTS:
                ALASKAN_DISTRICTS[fips_code] = election_data

            if party == 'DEMOCRAT':
                ALASKAN_DISTRICTS[fips_code]['percent_votes_democrat'] = percent
            elif party == 'REPUBLICAN':
                ALASKAN_DISTRICTS[fips_code]['percent_votes_republican'] = percent
            elif party == 'LIBERTARIAN':
                ALASKAN_DISTRICTS[fips_code]['percent_votes_libertarian'] = percent
            elif party == 'GREEN':
                ALASKAN_DISTRICTS[fips_code]['percent_votes_green'] = percent
            elif party == 'OTHER':
                ALASKAN_DISTRICTS[fips_code]['percent_votes_other'] = percent

        # Ignore District 99 and Kansas City
        elif fips_code != 2099 and fips_code != 36000:
            if ((prev_fips is None and prev_party is None) or
                (prev_fips == fips_code and prev_party == party)):
                candidate_votes += row[2]

            elif prev_fips == fips_code and prev_party != party:
                percent = round(candidate_votes / total_votes * 100, 2)

                if prev_party == 'DEMOCRAT':
                    COUNTY_DATA[fips_code]['percent_votes_democrat'] = percent
                elif prev_party == 'REPUBLICAN':
                    COUNTY_DATA[fips_code]['percent_votes_republican'] = percent
                elif prev_party == 'LIBERTARIAN':
                    COUNTY_DATA[fips_code]['percent_votes_libertarian'] = percent
                elif prev_party == 'GREEN':
                    COUNTY_DATA[fips_code]['percent_votes_green'] = percent
                elif prev_party == 'OTHER':
                    COUNTY_DATA[fips_code]['percent_votes_other'] = percent

                candidate_votes = row[2]

            elif prev_fips != fips_code:
                percent = round(candidate_votes / prev_total * 100, 2)
    
                if prev_party == 'DEMOCRAT':
                    COUNTY_DATA[prev_fips]['percent_votes_democrat'] = percent
                elif prev_party == 'REPUBLICAN':
                    COUNTY_DATA[prev_fips]['percent_votes_republican'] = percent
                elif prev_party == 'LIBERTARIAN':
                    COUNTY_DATA[prev_fips]['percent_votes_libertarian'] = percent
                elif prev_party == 'GREEN':
                    COUNTY_DATA[prev_fips]['percent_votes_green'] = percent
                elif prev_party == 'OTHER':
                    COUNTY_DATA[prev_fips]['percent_votes_other'] = percent

                candidate_votes = row[2]

            prev_fips = fips_code
            prev_party = party
            prev_total = total_votes

    # Update last row
    percent = round(candidate_votes / prev_total * 100, 2)
    
    if prev_party == 'DEMOCRAT':
        COUNTY_DATA[prev_fips]['percent_votes_democrat'] = percent
    elif prev_party == 'REPUBLICAN':
        COUNTY_DATA[prev_fips]['percent_votes_republican'] = percent
    elif prev_party == 'LIBERTARIAN':
        COUNTY_DATA[prev_fips]['percent_votes_libertarian'] = percent
    elif prev_party == 'GREEN':
        COUNTY_DATA[prev_fips]['percent_votes_green'] = percent
    elif prev_party == 'OTHER':
        COUNTY_DATA[prev_fips]['percent_votes_other'] = percent

def populate_election_data_for_Alaskan_boroughs():
    boroughs = [2013, 2016, 2020, 2050, 2060, 2068, 2070, 2090, 2100, 2105, 
                2110, 2122, 2130, 2150, 2158, 2164, 2170, 2180, 2185, 2188, 
                2195, 2198, 2220, 2230, 2240, 2261, 2275, 2282, 2290]

    for borough in boroughs:
        if borough in [2013, 2016, 2060, 2070, 2164]:
            districts = [2037]
        elif borough == 2020:
            districts = list(range(2012, 2029))
        elif borough == 2050:
            districts = [2037, 2038]
        elif borough == 2068:
            districts = [2006]
        elif borough == 2090:
            districts = list(range(2001, 2007))
        elif borough in [2100, 2230]:
            districts = [2033]
        elif borough == 2105:
            districts = [2033, 2035]            
        elif borough == 2110:
            districts = [2033, 2034]
        elif borough == 2122:
            districts = list(range(2029, 2033))
        elif borough in [2130, 2275]:
            districts = [2036]
        elif borough in [2150, 2282]:
            districts = [2032]
        elif borough == 2158:
            districts = [2038, 2039]
        elif borough == 2170:
            districts = list(range(2007, 2013))
        elif borough == 2180:
            districts = [2039]
        elif borough in [2185, 2188]:
            districts = [2040]
        elif borough in [2195, 2220]:
            districts = [2035]
        elif borough == 2198:
            districts = [2035, 2036]
        elif borough in [2240, 2261]:
            districts = [2006, 2009]
        elif borough == 2290:
            districts = [2006, 2037, 2039, 2040]

        per_democrat = 0
        per_republican = 0
        per_libertarian = 0
        per_green = 0
        per_other = 0

        for district in districts:
            per_democrat += ALASKAN_DISTRICTS[district]['percent_votes_democrat']
            per_republican += ALASKAN_DISTRICTS[district]['percent_votes_republican']
            per_libertarian += ALASKAN_DISTRICTS[district]['percent_votes_libertarian']
            per_green += ALASKAN_DISTRICTS[district]['percent_votes_green']
            per_other += ALASKAN_DISTRICTS[district]['percent_votes_other']

        per_democrat = round(per_democrat / len(districts), 2)
        per_republican = round(per_republican / len(districts), 2)
        per_libertarian = round(per_libertarian / len(districts), 2)
        per_green = round(per_green / len(districts), 2)
        per_other = round(per_other / len(districts), 2)

        COUNTY_DATA[borough]['percent_votes_democrat'] = per_democrat
        COUNTY_DATA[borough]['percent_votes_republican'] = per_republican
        COUNTY_DATA[borough]['percent_votes_libertarian'] = per_libertarian
        COUNTY_DATA[borough]['percent_votes_green'] = per_green
        COUNTY_DATA[borough]['percent_votes_other'] = per_other

def populate_database():
    for county in COUNTY_DATA.items():
        fips_code = county[0]
        county_name = county[1]['county']
        state = county[1]['state']

        total_population = county[1]['total_population']
        percent_male = county[1]['percent_male']
        percent_female = county[1]['percent_female']
        percent_age_10_to_14 = county[1]['percent_age_10_to_14']
        percent_age_15_to_19 = county[1]['percent_age_15_to_19']
        percent_age_20_to_24 = county[1]['percent_age_20_to_24']
        percent_age_25_to_34 = county[1]['percent_age_25_to_34']
        percent_age_35_to_44 = county[1]['percent_age_35_to_44']
        percent_age_45_to_54 = county[1]['percent_age_45_to_54']
        percent_age_55_to_59 = county[1]['percent_age_55_to_59']
        percent_age_60_to_64 = county[1]['percent_age_60_to_64']
        percent_age_65_to_74 = county[1]['percent_age_65_to_74']
        percent_age_75_to_84 = county[1]['percent_age_75_to_84']
        percent_age_85_and_older = county[1]['percent_age_85_and_older']
        median_age = county[1]['median_age']
        percent_hispanic_or_latino = county[1]['percent_hispanic_or_latino']
        percent_white = county[1]['percent_white']
        percent_black_or_african_american = county[1]['percent_black_or_african_american']
        percent_american_indian_and_alaska_native = county[1]['percent_american_indian_and_alaska_native']
        percent_asian = county[1]['percent_asian']
        percent_native_hawaiian_and_other_pacific_islander = county[1]['percent_native_hawaiian_and_other_pacific_islander']
        percent_other_race = county[1]['percent_other_race']
        percent_two_or_more_races = county[1]['percent_two_or_more_races']
        total_housing_units = county[1]['total_housing_units']

        unemployment_rate = county[1]['unemployment_rate']
        median_household_income = county[1]['median_household_income']
        mean_household_income = county[1]['mean_household_income']
        per_capita_income = county[1]['per_capita_income']
        percent_health_insurance = county[1]['percent_health_insurance']
        percent_private_health_insurance = county[1]['percent_private_health_insurance']
        percent_public_coverage = county[1]['percent_public_coverage']
        percent_no_health_insurance = county[1]['percent_no_health_insurance']
        percent_families_income_below_poverty_line = county[1]['percent_families_income_below_poverty_line']
        percent_people_income_below_poverty_line = county[1]['percent_people_income_below_poverty_line']

        percent_married_couple_family = county[1]['percent_married_couple_family']
        percent_cohabiting_couple = county[1]['percent_cohabiting_couple']
        percent_male_householder = county[1]['percent_male_householder']
        percent_female_householder = county[1]['percent_female_householder']
        avg_household_size = county[1]['avg_household_size']
        avg_family_size = county[1]['avg_family_size']
        percent_males_never_married = county[1]['percent_males_never_married']
        percent_males_now_married_separated = county[1]['percent_males_now_married_separated']
        percent_males_separated = county[1]['percent_males_separated']
        percent_males_widowed = county[1]['percent_males_widowed']
        percent_males_divorced = county[1]['percent_males_divorced']
        percent_females_never_married = county[1]['percent_females_never_married']
        percent_females_now_married_separated = county[1]['percent_females_now_married_separated']
        percent_females_separated = county[1]['percent_females_separated']
        percent_females_widowed = county[1]['percent_females_widowed']
        percent_females_divorced = county[1]['percent_females_divorced']
        percent_education_less_than_9th_grade = county[1]['percent_education_less_than_9th_grade']
        percent_9th_to_12th_grade_no_diploma = county[1]['percent_9th_to_12th_grade_no_diploma']
        percent_high_school_graduate = county[1]['percent_high_school_graduate']
        percent_some_college_no_degree = county[1]['percent_some_college_no_degree']
        percent_associates_degree = county[1]['percent_associates_degree']
        percent_bachelors_degree = county[1]['percent_bachelors_degree']
        percent_graduate_or_professional_degree = county[1]['percent_graduate_or_professional_degree']
        percent_high_school_graduate_or_higher = county[1]['percent_high_school_graduate_or_higher']
        percent_bachelors_degree_or_higher = county[1]['percent_bachelors_degree_or_higher']
        percent_civilian_veteran = county[1]['percent_civilian_veteran']
        percent_with_disability = county[1]['percent_with_disability']
        percent_native_born = county[1]['percent_native_born']
        percent_born_in_US = county[1]['percent_born_in_US']
        percent_foreign_born = county[1]['percent_foreign_born']
        percent_naturalized_US_citizen = county[1]['percent_naturalized_US_citizen']
        percent_not_US_citizen = county[1]['percent_not_US_citizen']
        percent_households_with_computer = county[1]['percent_households_with_computer']
        percent_households_with_Internet = county[1]['percent_households_with_Internet']
        
        percent_votes_democrat = county[1]['percent_votes_democrat']
        percent_votes_republican = county[1]['percent_votes_republican']
        percent_votes_libertarian = county[1]['percent_votes_libertarian']
        percent_votes_green = county[1]['percent_votes_green']
        percent_votes_other = county[1]['percent_votes_other']

        c.execute('INSERT INTO county_data VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ' +
                                                  '?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ' +
                                                  '?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ' +
                                                  '?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ' +
                                                  '?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ' +
                                                  '?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ' +
                                                  '?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ' +
                                                  '?, ?, ?, ?, ?, ?);',
                                                  (fips_code,
                                                   county_name,
                                                   state,
                                                   total_population,
                                                   percent_male,
                                                   percent_female,
                                                   percent_age_10_to_14,
                                                   percent_age_15_to_19,
                                                   percent_age_20_to_24,
                                                   percent_age_25_to_34,
                                                   percent_age_35_to_44,
                                                   percent_age_45_to_54,
                                                   percent_age_55_to_59,
                                                   percent_age_60_to_64,
                                                   percent_age_65_to_74,
                                                   percent_age_75_to_84,
                                                   percent_age_85_and_older,
                                                   median_age,
                                                   percent_hispanic_or_latino,
                                                   percent_white,
                                                   percent_black_or_african_american,
                                                   percent_american_indian_and_alaska_native,
                                                   percent_asian,
                                                   percent_native_hawaiian_and_other_pacific_islander,
                                                   percent_other_race,
                                                   percent_two_or_more_races,
                                                   total_housing_units,
                                                   unemployment_rate,
                                                   median_household_income,
                                                   mean_household_income,
                                                   per_capita_income,
                                                   percent_health_insurance,
                                                   percent_private_health_insurance,
                                                   percent_public_coverage,
                                                   percent_no_health_insurance,
                                                   percent_families_income_below_poverty_line,
                                                   percent_people_income_below_poverty_line,
                                                   percent_married_couple_family,
                                                   percent_cohabiting_couple,
                                                   percent_male_householder,
                                                   percent_female_householder,
                                                   avg_household_size,
                                                   avg_family_size,
                                                   percent_males_never_married,
                                                   percent_males_now_married_separated,
                                                   percent_males_separated,
                                                   percent_males_widowed,
                                                   percent_males_divorced,
                                                   percent_females_never_married,
                                                   percent_females_now_married_separated,
                                                   percent_females_separated,
                                                   percent_females_widowed,
                                                   percent_females_divorced,
                                                   percent_education_less_than_9th_grade,
                                                   percent_9th_to_12th_grade_no_diploma,
                                                   percent_high_school_graduate,
                                                   percent_some_college_no_degree,
                                                   percent_associates_degree,
                                                   percent_bachelors_degree,
                                                   percent_graduate_or_professional_degree,
                                                   percent_high_school_graduate_or_higher,
                                                   percent_bachelors_degree_or_higher,
                                                   percent_civilian_veteran,
                                                   percent_with_disability,
                                                   percent_native_born,
                                                   percent_born_in_US,
                                                   percent_foreign_born,
                                                   percent_naturalized_US_citizen,
                                                   percent_not_US_citizen,
                                                   percent_households_with_computer,
                                                   percent_households_with_Internet,  
                                                   percent_votes_democrat,
                                                   percent_votes_republican,
                                                   percent_votes_libertarian,
                                                   percent_votes_green,
                                                   percent_votes_other))
    conn.commit()

# setting up database
print('Setting up database')
c, conn = setup_database()

print('Loading data')
load_demographic_housing_data()
load_economic_characteristics_data()
load_social_characteristics_data()
load_election_data()
populate_election_data_for_Alaskan_boroughs()

print('Populating database')
populate_database()
