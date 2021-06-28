from pathlib import Path
import pandas as pd
from tqdm import tqdm
import subprocess

DATA_DIR = Path(__file__).parent.parent / 'data'
PORTION = 1 / 50  # only extract a fraction of the data due to rate limits


def get_ids(tweet_files):
    for tweet_file in tweet_files:
        df = pd.read_csv(tweet_file, header=None)
        ids = list(df.iloc[:, 0])
        ids = ids[:int(len(ids) * PORTION)]

        for id in ids:
            yield id


def count_lines(filename):
    out = subprocess.Popen(['wc', '-l', str(filename.absolute())],
                           stdout=subprocess.PIPE,
                           stderr=subprocess.STDOUT
                           ).communicate()[0]
    return int(int(out.decode("utf-8").strip().split(' ')[0]) * PORTION)


def get_total_count(tweet_files):
    total = 0
    for tweet_file in tweet_files:
        total += count_lines(tweet_file)

    return total


tweet_files = (DATA_DIR / 'raw' / 'tweets').glob('*.csv')
print("Counting total number of IDs to hydrate")
total_count = get_total_count(tweet_files)

print("Beginning hydration")
ids = get_ids(tweet_files)

for id in tqdm(ids, total=total_count):
    pass
