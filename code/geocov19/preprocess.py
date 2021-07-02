from pathlib import Path
import json
import os

DATA_DIR = Path(__file__).parent.parent.parent / 'data'
DATASET = 'geocov19'

SOURCE_FOLDER = DATA_DIR / 'raw' / DATASET

with open(SOURCE_FOLDER / 'tweets.tsv', 'w') as fout:
    files = SOURCE_FOLDER.glob('*.json')
    for filename in files:
        with open(filename, 'r') as f:
            for line in f.readlines():
                row = json.loads(line)
                location = row['geo_source']
                if row['geo_source'] == 'coordinates':
                    if 'county' in row['geo'] and row['geo']['country_code'] == 'us':
                        location = row['geo']
                elif row['geo_source'] == 'place':
                    if 'county' in row['place'] and row['place']['country_code'] == 'us':
                        location = row['place']
                elif row['geo_source'] == 'user_location':
                    if 'county' in row['user_location'] and row['user_location']['country_code'] == 'us':
                        location = row['user_location']

                if location is not None and 'state' in location:
                    fout.write(
                        f"{row['tweet_id']}\t{location['county']}\t{location['state']}\n")

        os.remove(filename)
