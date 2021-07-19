from pathlib import Path
import sqlite3
import pandas as pd
import nltk
from nltk.tokenize import word_tokenize
nltk.download('punkt')

DATA_DIR = Path(__file__).parent.parent / 'data'
DB_FILE = 'processed.db'
KEYWORDS_FILE = 'avax-keywords.txt'
HASHTAGS = ['fakenews', 'madagascar', 'komeshacorona', 'stopbulos', 'bulo', 'merida', 'stopcovid', 'chinavirus', 
            'plandemic', 'covidpropaganda', 'nomask', 'endtheshutdown', 'defundthefda', 'maskfree', 'biologicalwarfare', 
            'healthfreedom', 'scamdemic', 'dontstayathome', 'governmentcontrol', 'coronabs']

with open(DATA_DIR / 'raw' / 'avax' / KEYWORDS_FILE, 'r') as f:
    AVAX_KEYWORDS = [l.strip().lower() for l in f]

ALL_HASHTAGS = HASHTAGS + AVAX_KEYWORDS

covid_doesnt_exist = [("covid-19", "covid", "corona", "coronavirus"), ("covid-19 is fake", "covid is fake", "corona is fake", "coronavirus is fake", "covid-19 is not real", "covid is not real", "corona is not real", "coronavirus is not real", "covid-19 isn't real", "covid isn't real", "corona isn't real", "coronavirus isn't real", "covid-19 doesn't exist", "covid doesn't exist", "corona doesn't exist", "coronavirus doesn't exist", "covid-19 do not exist", "covid do not exist", "corona do not exist", "coronavirus do not exist", "is overexaggerated")]
covid_isnt_bad = [("covid-19", "covid", "corona", "coronavirus"), ("like the flu", "is flulike", "is flu-like", "not dangerous", "not deadly", "not a threat", "isn't that bad", "not bad", "is overexaggerated")]
depopulation = [("covid-19", "covid", "corona", "coronavirus", "vaccine", "vaccines", "vaccination", "jab", "injection"), ("depopulation", "reduce the population", "kill people")]
vaccine_causes_covid = [("covid-19", "covid", "corona", "coronavirus"), ("vaccine", "vaccines", "shot", "jab"), ("causes covid-19", "causes coronavirus", "causes covid", "cause coronavirus", "cause Covid-19", "infect people with", "infects people with", "infects people with", "infect people with")]
other_vaccines = [("covid-19", "covid", "corona", "coronavirus"), ("should use bleach", "can use bleach", "must use bleach", "should use chemicals", "can use chemicals", "must use chemicals")]
vaccine_deadly = [("covid-19", "covid", "corona", "coronavirus"), ("vaccine", "vaccines", "vaccination", "jab", "injection"), ("kills", "will kill", "you will die", "is deadly")]
vaccine_causes_autism = [("covid-19", "covid", "corona", "coronavirus"), ("vaccine", "vaccines", "vaccination", "jab", "injection"), ("causes autism", "does cause autism", "will cause autism", "creates autism")]
vaccines_unsafe = [("covid-19", "covid", "corona", "coronavirus"), ("vaccine", "vaccines", "vaccination", "jab", "injection"), ("are unsafe", "are not safe")]
vaccines_innefective = [("covid-19", "covid", "corona", "coronavirus"), ("vaccine", "vaccines", "vaccination", "jab", "injection"), ("don't work", "do not work", "ineffective", "not effective")]
vaccine_ingredients = [("covid-19", "covid", "corona", "coronavirus"), ("vaccine", "vaccines", "vaccination", "jab", "injection"), ("contains", "contain", "have", "has"), ("aluminium", "mercury", "thimerosal", "aluminum", "microchip", "tracker")]
vaccines_cause_infertility = [("covid-19", "covid", "corona", "coronavirus"), ("vaccine", "vaccines", "shot", "jab"), ("cause infertility", "causes infertility", "makes infertile", "want to sterilise", "want to sterilize", "going to sterilise", "going to sterilize", "make you sterile", "makes sterile")]
vaccines_untested = [("covid-19", "covid", "corona", "coronavirus"), ("vaccine", "vaccines", "shot", "jab"), ("untested", "are rushed", "is rushed", "were rushed", "not tested")]

boolean_search = [covid_doesnt_exist, covid_isnt_bad, depopulation, vaccine_causes_covid, other_vaccines, vaccine_deadly, vaccine_causes_autism, vaccines_unsafe, vaccines_innefective, vaccine_ingredients, vaccines_cause_infertility, vaccines_untested]
face_check = ("misinformation", "fact check", "false")

conn = sqlite3.connect(DATA_DIR / 'processed' / DB_FILE)
c = conn.cursor()
tweets_query = pd.read_sql_query("""SELECT * FROM tweets""", conn)
tweets = pd.DataFrame(tweets_query, columns=['id', 'fips', 'city', 'user_id', 'text', 'created_at', 'is_retweet', 'original_tweet_id', 'retweet_count', 'favorite_count', 'lang'])
new_tweets = pd.DataFrame(columns=['id', 'fips', 'city', 'user_id', 'text', 'created_at', 'is_retweet', 'original_tweet_id', 'retweet_count', 'favorite_count', 'lang'])
print(new_tweets)

hashtags_query = pd.read_sql_query("""SELECT * FROM hashtags""", conn)
hashtags = pd.DataFrame(hashtags_query, columns=['tweet_id', 'hashtag'])

for idx, tweet in tweets.iterrows():
    tweet_id = tweet[0]
    htags = list(hashtags[hashtags['tweet_id'] == tweet_id]['hashtag'])
    words = [w for w in word_tokenize(tweet[4]) if w.isalpha()]

    # check if tweet contains misinformation hashtags
    if set(hashtags) & set(ALL_HASHTAGS):
        new_tweets = new_tweets.append(tweet, ignore_index=True)

    # check boolean search strings
    for search in boolean_search:
        failed = False
        # loop through OR clauses
        for lst in search:
            # if one of the OR clauses is not met, the tweet fails this search string
            if not(set(words) & set(lst)): failed = True
        
        # if the tweet passed this search string, add it to the df and move on
        if failed == False:
            new_tweets = new_tweets.append(tweet, ignore_index=True)
            break

print(new_tweets)