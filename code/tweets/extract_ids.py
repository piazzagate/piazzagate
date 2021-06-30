from pathlib import Path
import pandas as pd

DATA_DIR = Path(__file__).parent.parent.parent / 'data'
csv_paths = (DATA_DIR / 'raw' / 'tweets').glob('*.csv')
tweet_file = DATA_DIR / 'processed' / 'tweets_ids.txt'

with open(tweet_file, 'w') as fout:
    for csv_path in csv_paths:
        df = pd.read_csv(csv_path, header=None)
        ids = df.iloc[:, 0]

        for id in ids:
            fout.write(f'{id}\n')
