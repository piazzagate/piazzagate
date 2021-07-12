from pathlib import Path
import sqlite3
import subprocess
import os
from datetime import date, datetime

DATA_DIR = Path(__file__).parent.parent / 'data'
DATASET = 'processed'

input_conn = sqlite3.connect(DATA_DIR / 'processed' / f'{DATASET}.db')
input_cursor = input_conn.cursor()


def setup_database(p):
    if p.exists():
        os.remove(p)
    conn = sqlite3.connect(p)
    c = conn.cursor()

    cmd = """
        CREATE TABLE IF NOT EXISTS tweets (
            id INT PRIMARY KEY NOT NULL,
            fips INT NOT NULL,
            city TEXT NOT NULL,
            user_id INT NOT NULL,
            text TEXT NOT NULL,
            created_at TEXT NOT NULL,
            is_retweet INT NOT NULL,
            original_tweet_id INT,
            retweet_count INT NOT NULL,
            favorite_count INT NOT NULL,
            lang TEXT NOT NULL,
            FOREIGN KEY(user_id) REFERENCES users(id),
            FOREIGN KEY(fips) REFERENCES demographics(fips)
        );
    """
    c.execute(cmd)
    cmd = """
        CREATE TABLE IF NOT EXISTS users (
            id INT PRIMARY KEY NOT NULL,
            location TEXT,
            verified INT NOT NULL,
            follower_count INT NOT NULL,
            list_count INT NOT NULL,
            tweet_count INT NOT NULL
        );
    """
    c.execute(cmd)
    cmd = """
        CREATE TABLE IF NOT EXISTS hashtags (
            tweet_id INT NOT NULL,
            hashtag TEXT NOT NULL,
            FOREIGN KEY(tweet_id) REFERENCES tweets(id)
        );
    """
    c.execute(cmd)
    cmd = """
        CREATE TABLE IF NOT EXISTS demographics (
            fips INT PRIMARY KEY NOT NULL,
            county TEXT NOT NULL,
            state TEXT NOT NULL,
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
    c.execute(cmd)
    cmd = """
        CREATE TABLE IF NOT EXISTS covid (
            date TEXT NOT NULL,
            fips INT NOT NULL,
            cases INT,
            deaths INT,
            vaccinated_count INT,
            vaccinated_percent REAL,
            PRIMARY KEY (date, fips),
            FOREIGN KEY(fips) REFERENCES demographics(fips)
        );
    """
    c.execute(cmd)
    conn.commit()

    return conn, c


def tweet_exists(c, tweet_id):
    c.execute(f'SELECT COUNT(1) FROM tweets WHERE id = {tweet_id}')
    for row in c:
        if row[0] == 1:
            return True
    return False


def user_exists(c, user_id):
    c.execute(f'SELECT COUNT(1) FROM tweets WHERE user_id = {user_id}')
    for row in c:
        if row[0] > 0:
            return True
    return False


def random_split():
    # create a temporary list of all tweet IDs
    cmd = 'SELECT id FROM tweets'
    input_cursor.execute(cmd)
    with open(DATA_DIR / 'temp.txt', 'w') as f:
        for i, row in enumerate(input_cursor):  # input_cursor:
            if i > 10000:
                break
            f.write(f'{row[0]}\n')

    with open(DATA_DIR / 'temp_shuf.txt', 'w') as f:
        subprocess.run(['shuf', (DATA_DIR / 'temp.txt').resolve()], stdout=f)
    os.remove(DATA_DIR / 'temp.txt')

    def count_lines(filename):
        out = subprocess.Popen(['wc', '-l', str(filename.resolve())],
                               stdout=subprocess.PIPE,
                               stderr=subprocess.STDOUT
                               ).communicate()[0]
        return int(out.decode("utf-8").strip().split(' ')[0])

    TRAIN_TEST_SPLIT = 0.95
    total_tweets = count_lines(DATA_DIR / 'temp_shuf.txt')

    train_conn, train_cursor = setup_database(
        DATA_DIR / 'processed' / f'{DATASET}_random_train.db')
    test_conn, test_cursor = setup_database(
        DATA_DIR / 'processed' / f'{DATASET}_random_test.db')

    print("- Copying tweets")
    with open(DATA_DIR / 'temp_shuf.txt') as f:
        for i, line in enumerate(f.readlines()):
            if i % 1000 == 0:
                train_conn.commit()
                test_conn.commit()

            tweet_id = int(line[:-1])

            input_cursor.execute(
                f'SELECT id, fips, city, user_id, text, created_at, is_retweet, original_tweet_id, retweet_count, favorite_count, lang FROM tweets WHERE id = {tweet_id}')
            cmd = 'INSERT INTO tweets VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)'
            for row in input_cursor:
                if i < total_tweets * TRAIN_TEST_SPLIT:
                    train_cursor.execute(cmd, row)
                else:
                    test_cursor.execute(cmd, row)
        train_conn.commit()
        test_conn.commit()

    os.remove(DATA_DIR / 'temp_shuf.txt')

    print("- Copying hashtags")

    input_cursor.execute('SELECT tweet_id, hashtag FROM hashtags')
    for i, row in enumerate(input_cursor):
        if i % 1000 == 0:
            train_conn.commit()
            test_conn.commit()
        tweet_id = row[0]
        cmd = 'INSERT INTO hashtags VALUES (?, ?)'
        if tweet_exists(train_cursor, tweet_id):
            train_cursor.execute(cmd, row)
        else:
            test_cursor.execute(cmd, row)
    train_conn.commit()
    test_conn.commit()

    print("- Copying users")

    input_cursor.execute('SELECT * FROM users')
    for i, row in enumerate(input_cursor):
        if i % 1000 == 0:
            train_conn.commit()
            test_conn.commit()
        user_id = row[0]
        cmd = 'INSERT INTO users VALUES (?, ?, ?, ?, ?, ?)'
        if user_exists(train_cursor, user_id):
            train_cursor.execute(cmd, row)
        else:
            test_cursor.execute(cmd, row)
    train_conn.commit()
    test_conn.commit()

    print("- Copying demographics")
    input_cursor.execute('SELECT * FROM demographics')
    for i, row in enumerate(input_cursor):
        if i % 1000 == 0:
            train_conn.commit()
            test_conn.commit()
        user_id = row[0]
        cmd = 'INSERT INTO demographics VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ' + \
            '?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ' + \
            '?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ' + \
            '?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ' + \
            '?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ' + \
            '?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ' + \
            '?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?); '
        train_cursor.execute(cmd, row)
        test_cursor.execute(cmd, row)
    train_conn.commit()
    test_conn.commit()

    print('- Copying covid')

    input_cursor.execute('SELECT * FROM covid')  # demographics')
    for i, row in enumerate(input_cursor):
        if i % 1000 == 0:
            train_conn.commit()
            test_conn.commit()
        user_id = row[0]
        cmd = 'INSERT INTO covid VALUES (?, ?, ?, ?, ?, ?)'
        # if not covid_exists(train_cursor, row[0], row[1]):
        train_cursor.execute(cmd, row)
        test_cursor.execute(cmd, row)
    train_conn.commit()
    test_conn.commit()


def date_to_str(d):
    return d.strftime('%Y-%m-%d')


def str_to_date(s):
    return datetime.strptime(s, '%Y-%m-%d').date()


def time_split():
    train_conn, train_cursor = setup_database(
        DATA_DIR / 'processed' / f'{DATASET}_time_train.db')
    test_conn, test_cursor = setup_database(
        DATA_DIR / 'processed' / f'{DATASET}_time_test.db')

    print('- Copying tweets')

    input_cursor.execute(
        f'SELECT id, fips, city, user_id, text, created_at, is_retweet, original_tweet_id, retweet_count, favorite_count, lang FROM tweets WHERE created_at < "{date_to_str(cutoff)}"')

    for i, row in enumerate(input_cursor):
        if i % 1000 == 0:
            train_conn.commit()
        tweet_id = row[0]
        cmd = 'INSERT INTO tweets VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)'
        train_cursor.execute(cmd, row)
    train_conn.commit()

    input_cursor.execute(
        f'SELECT id, fips, city, user_id, text, created_at, is_retweet, original_tweet_id, retweet_count, favorite_count, lang FROM tweets WHERE created_at >= "{date_to_str(cutoff)}"')

    for i, row in enumerate(input_cursor):
        if i % 1000 == 0:
            test_conn.commit()
        tweet_id = row[0]
        cmd = 'INSERT INTO tweets VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)'
        test_cursor.execute(cmd, row)
    test_conn.commit()

    print("- Copying hashtags")

    input_cursor.execute('SELECT tweet_id, hashtag FROM hashtags')
    for i, row in enumerate(input_cursor):
        if i % 1000 == 0:
            train_conn.commit()
            test_conn.commit()
        tweet_id = row[0]
        cmd = 'INSERT INTO hashtags VALUES (?, ?)'
        if tweet_exists(train_cursor, tweet_id):
            train_cursor.execute(cmd, row)
        else:
            test_cursor.execute(cmd, row)
    train_conn.commit()
    test_conn.commit()

    print("- Copying users")

    input_cursor.execute('SELECT * FROM users')
    for i, row in enumerate(input_cursor):
        if i % 1000 == 0:
            train_conn.commit()
            test_conn.commit()
        user_id = row[0]
        cmd = 'INSERT INTO users VALUES (?, ?, ?, ?, ?, ?)'
        if user_exists(train_cursor, user_id):
            train_cursor.execute(cmd, row)
        else:
            test_cursor.execute(cmd, row)
    train_conn.commit()
    test_conn.commit()

    print("- Copying demographics")
    input_cursor.execute('SELECT * FROM demographics')
    for i, row in enumerate(input_cursor):
        if i % 1000 == 0:
            train_conn.commit()
            test_conn.commit()
        user_id = row[0]
        cmd = 'INSERT INTO demographics VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ' + \
            '?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ' + \
            '?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ' + \
            '?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ' + \
            '?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ' + \
            '?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ' + \
            '?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?); '
        train_cursor.execute(cmd, row)
        test_cursor.execute(cmd, row)
    train_conn.commit()
    test_conn.commit()

    print('- Copying covid')

    input_cursor.execute('SELECT * FROM covid')  # demographics')
    for i, row in enumerate(input_cursor):
        if i % 1000 == 0:
            train_conn.commit()
            test_conn.commit()
        user_id = row[0]
        cmd = 'INSERT INTO covid VALUES (?, ?, ?, ?, ?, ?)'
        # if not covid_exists(train_cursor, row[0], row[1]):
        train_cursor.execute(cmd, row)
        test_cursor.execute(cmd, row)
    train_conn.commit()
    test_conn.commit()


print('Executing random split')
random_split()

cutoff = date(2021, 5, 1)
print(f'Executing time split along {date_to_str(cutoff)}')
time_split()
