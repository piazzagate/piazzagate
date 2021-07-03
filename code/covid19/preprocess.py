from pathlib import Path
import json
import os

DATA_DIR = Path(__file__).parent.parent.parent / 'data'
DATASET = 'covid19'

SOURCE_FOLDER = DATA_DIR / 'raw' / DATASET

with open(SOURCE_FOLDER / f'{DATASET}.tsv', 'w') as fout:
    files = SOURCE_FOLDER.glob('*.json')
    for filename in files:
        with open(filename, 'r') as f:
            for line in f.readlines():
                try:
                    row = json.loads(line)
                except:
                    continue  # ignore failed lines
                location = row['location']
                if location and location['country'] == 'United States' and location['city'] is not None:
                    fout.write(
                        f"{row['tweet_id']}\t{location['city']}\t{location['state']}\n")

        os.remove(filename)
