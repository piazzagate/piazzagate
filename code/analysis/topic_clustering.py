from sklearn.decomposition import LatentDirichletAllocation
from sklearn.feature_extraction.text import CountVectorizer
from sklearn.feature_extraction._stop_words import ENGLISH_STOP_WORDS
import sqlite3
from pathlib import Path
import pandas as pd
import pyLDAvis.sklearn
import pyLDAvis

DATA_DIR = Path(__file__).parent.parent.parent / 'data'
DATASET = 'processed_random_train'
conn = sqlite3.connect(DATA_DIR / 'processed' / f'{DATASET}.db')

# Functions for printing keywords for each topic


def selected_topics(model, vectorizer, top_n=10):
    with open(DATA_DIR / 'analysis' / 'topics.txt', 'w') as f:
        for idx, topic in enumerate(model.components_):
            print("Topic %d:" % (idx))
            print([(vectorizer.get_feature_names()[i], topic[i])
                   for i in topic.argsort()[:-top_n - 1:-1]])
            line = ','.join([vectorizer.get_feature_names()[i]
                             for i in topic.argsort()[:-top_n - 1:-1]])
            f.write(line + '\n')


stop_words = list(ENGLISH_STOP_WORDS) + [
    'https', 'covid', 'coronavirus', 'amp'  # amp is &amp which is just &
]
stop_words = frozenset(stop_words)


def model_lda(conn, n_topics=8):
    cmd = '''SELECT text FROM tweets'''

    df = pd.read_sql_query(cmd, conn)
    texts = list(df['text'])
    vectorizer = CountVectorizer(min_df=2, max_df=0.9, stop_words=stop_words,
                                 lowercase=True, token_pattern='[a-zA-Z\-][a-zA-Z\-]{2,}')
    vectorized = vectorizer.fit_transform(texts)

    # print(vectorizer.inverse_transform(vectorized))

    lda = LatentDirichletAllocation(
        n_components=n_topics, max_iter=10, learning_method='online', verbose=True)
    lda.fit_transform(vectorized)

    return lda, vectorizer, vectorized


lda, vectorizer, vectorized = model_lda(conn)
selected_topics(lda, vectorizer)
dash = pyLDAvis.sklearn.prepare(lda, vectorized, vectorizer, mds='tsne')
with open(DATA_DIR / 'analysis' / 'topics.html', 'w') as f:
    pyLDAvis.save_html(dash, f)
