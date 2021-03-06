from pathlib import Path
import sqlite3
import pandas as pd
import nltk
from nltk.tokenize import word_tokenize
from tqdm import tqdm
nltk.download('punkt')

DATA_DIR = Path(__file__).parent.parent / 'data'
DB_FILE = 'covid19.db'
KEYWORDS_FILE = 'keywords.txt'

plandemic_hashtags = ['plandemic', 'theplandemic', 'covidplandemic', 'plandemic2020', 'plandemicdocumentary', 'plandemichoax', 'endtheplandemic', 'covidplandemic',
                      'fakeccpplandemic', 'ccpplandemic', 'democratplandemic', 'plandemicchronicles', 'endthecovidplandemicnow', 'covidisaplandemic', 'plandemicnotpandemic',
                      'covid19plandemic', 'plandemic2021']
scamdemic_hashtags = ['scamdemic', 'scamdemic2020',
                      'thegreatscamdemic', 'scamdemicisover', 'scamdemic2021']
fakenews_hashtags = ['fakenews', 'fakenewscnn', 'fakenewsmedia', 'cnnfakenews', 'cnnisfakenews', 'stopfakenews', 'fakenewsvirus', 'fakenewsalert', 'fakenewsnyt',
                     'fakenewsshitshows', 'coronavirusisfakenews', 'fakenewsnbc', 'fakenewsfoxnews', 'mediaisfakenews', 'fakenewsreporters', 'foxnewsisfakenews', 'cbsfakenews',
                     'fakenewscdc', 'boycottfakenewsstations', 'covid19fakenews', 'msmisfakenews', 'fakenewspromoteschina', 'fakenewsepidemic', 'foxfakenews', 'trumpisfakenews',
                     'chinafakenewscollusion2020', 'fakenewsmsnbc', 'fakenewsconspiracy', 'fakenewsalert', 'fakenewspandemic', 'fakenewstrump', 'fakenewsenemyofthepeople',
                     'fakenewsdoctor', 'covidfakenews', 'corruptfakenews', 'lyingfakenewsmedia', 'fightingfakenews', 'stopspreadingfakenews', 'trumpcovid19isfakenews', 'originsoffakenews',
                     'foxistherealfakenews', 'stopfakenews', 'fakenewsclowns']
chinavirus_hashtags = ['chinavirus', 'chinavirussesungguhnya', 'chinavirushide', 'terminatorchinavirus', 'chinaviruses', 'madeinchina', 'chinavirusmadeinchina',
                       'madeinchinavirus', 'chinavirus19', 'chinavirustyranny', 'chinavirushoax', 'chinaviruscoverup', 'demchinavirus', 'chinavirus2020', 'covidisachinavirus', 'thechinavirus']
propaganda_hashtags = ['covidpropaganda', 'propagandavirus', 'pandemicpropaganda',
                       'chinesepropaganda', 'chinacovidpropaganda', 'newssourcepropaganda', 'covid19propaganda']
misc_hashtags = ['madagascar', 'madagascarcure', 'komeshacorona', 'stopbulos', 'bulo', 'bulos', 'merida', 'stopcovid',
                 'endtheshutdown', 'defundthefda', 'maskfree', 'biologicalwarfare', 'healthfreedom', 'dontstayathome', 'governmentcontrol', 'coronabs']

with open(DATA_DIR / 'raw' / 'avax' / KEYWORDS_FILE, 'r') as f:
    avax_hashtags = [l.strip().lower() for l in f]

ALL_HASHTAGS = set(plandemic_hashtags + scamdemic_hashtags +
                   fakenews_hashtags + misc_hashtags + avax_hashtags)

covid_doesnt_exist = [("covid-19", "covid", "corona", "coronavirus"), ("covid-19 is fake", "covid is fake", "corona is fake", "coronavirus is fake", "covid-19 is not real", "covid is not real", "corona is not real", "coronavirus is not real", "covid-19 isn't real", "covid isn't real",
                                                                       "corona isn't real", "coronavirus isn't real", "covid-19 doesn't exist", "covid doesn't exist", "corona doesn't exist", "coronavirus doesn't exist", "covid-19 do not exist", "covid do not exist", "corona do not exist", "coronavirus do not exist", "is overexaggerated")]
covid_isnt_bad = [("covid-19", "covid", "corona", "coronavirus"), ("like the flu", "is flulike", "is flu-like",
                                                                   "not dangerous", "not deadly", "not a threat", "isn't that bad", "not bad", "is overexaggerated")]
depopulation = [("covid-19", "covid", "corona", "coronavirus", "vaccine", "vaccines",
                 "vaccination", "jab", "injection"), ("depopulation", "reduce the population", "kill people")]
vaccine_causes_covid = [("covid-19", "covid", "corona", "coronavirus"), ("vaccine", "vaccines", "shot", "jab"), ("causes covid-19", "causes coronavirus",
                                                                                                                 "causes covid", "cause coronavirus", "cause Covid-19", "infect people with", "infects people with", "infects people with", "infect people with")]
other_vaccines = [("covid-19", "covid", "corona", "coronavirus"), ("should use bleach", "can use bleach",
                                                                   "must use bleach", "should use chemicals", "can use chemicals", "must use chemicals")]
vaccine_deadly = [("covid-19", "covid", "corona", "coronavirus"), ("vaccine", "vaccines",
                                                                   "vaccination", "jab", "injection"), ("kills", "will kill", "you will die", "is deadly")]
vaccine_causes_autism = [("covid-19", "covid", "corona", "coronavirus"), ("vaccine", "vaccines", "vaccination",
                                                                          "jab", "injection"), ("causes autism", "does cause autism", "will cause autism", "creates autism")]
vaccines_unsafe = [("covid-19", "covid", "corona", "coronavirus"), ("vaccine",
                                                                    "vaccines", "vaccination", "jab", "injection"), ("are unsafe", "are not safe")]
vaccines_innefective = [("covid-19", "covid", "corona", "coronavirus"), ("vaccine", "vaccines",
                                                                         "vaccination", "jab", "injection"), ("don't work", "do not work", "ineffective", "not effective")]
vaccine_ingredients = [("covid-19", "covid", "corona", "coronavirus"), ("vaccine", "vaccines", "vaccination", "jab", "injection"),
                       ("contains", "contain", "have", "has"), ("aluminium", "mercury", "thimerosal", "aluminum", "microchip", "tracker")]
vaccines_cause_infertility = [("covid-19", "covid", "corona", "coronavirus"), ("vaccine", "vaccines", "shot", "jab"), ("cause infertility", "causes infertility",
                                                                                                                       "makes infertile", "want to sterilise", "want to sterilize", "going to sterilise", "going to sterilize", "make you sterile", "makes sterile")]
vaccines_untested = [("covid-19", "covid", "corona", "coronavirus"), ("vaccine", "vaccines",
                                                                      "shot", "jab"), ("untested", "are rushed", "is rushed", "were rushed", "not tested")]

boolean_search = [covid_doesnt_exist, covid_isnt_bad, depopulation, vaccine_causes_covid, other_vaccines, vaccine_deadly,
                  vaccine_causes_autism, vaccines_unsafe, vaccines_innefective, vaccine_ingredients, vaccines_cause_infertility, vaccines_untested]
boolean_search = list(map(lambda search: list(map(
    lambda lst: set(lst), search)), boolean_search))

conn = sqlite3.connect(DATA_DIR / 'processed' / DB_FILE)
c = conn.cursor()  # c handles select tweets
c2 = conn.cursor()  # c2 handles deletion

all_hashtags = pd.DataFrame(pd.read_sql_query(
    'SELECT * FROM hashtags', conn), columns=['tweet_id', 'hashtag'])
all_hashtags['hashtag'] = all_hashtags['hashtag'].str.lower()
all_hashtags = all_hashtags.set_index('tweet_id', drop=False)
all_hashtags = all_hashtags.sort_index()


def get_hashtags(tweet_id):
    if tweet_id not in all_hashtags.index:
        return set()
    hashtags = all_hashtags.loc[tweet_id]['hashtag']
    if isinstance(hashtags, str):
        return set([hashtags])
    return set(hashtags)


def matches_boolean_search(text):
    words = set([w for w in word_tokenize(text) if w.isalpha()])
    # check boolean search strings
    for search in boolean_search:
        failed = False
        # loop through OR clauses
        for lst in search:
            # if one of the OR clauses is not met, the tweet fails this search string
            if not(words & lst):
                failed = True
                break

        if not failed:
            return True
    return False


def get_total_tweets():
    c = conn.cursor()
    c.execute('SELECT COUNT(*) FROM tweets;')
    for row in c:
        return row[0]


def set_overlap(a, b):
    for item in a:
        if item in b:
            return True
    return False


c.execute('SELECT id, text FROM tweets;')
total_yields = 0
pbar = tqdm(enumerate(c), total=get_total_tweets())
for i, row in pbar:
    tweet_id = row[0]
    text = row[1]

    hashtags = get_hashtags(tweet_id)
    # check if tweet contains misinformation hashtags
    if hashtags & ALL_HASHTAGS:
        total_yields += 1
        continue

    if matches_boolean_search(text):
        total_yields += 1
        continue
    # if this is not a misinfo tweet, delete
    c2.execute(f'DELETE FROM tweets WHERE id={tweet_id}')
    if i % 10000 == 0:
        pbar.set_postfix({'yield_rate': f'{total_yields / (i+1):.5f}'})

conn.commit()
