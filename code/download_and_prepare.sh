#!/bin/bash

# download dependencies
cd "$(dirname "$0")/../"
pip install -r requirements.txt

# download dataset
cd "$(dirname "$0")/../data/raw"
gdown https://drive.google.com/uc?id=1dx9Lr7p3GkZWXrokLr4IO9j6f_yZ8RGs
unzip tweets.zip -d tweets

# hydrate
cd ../../
python code/tweets/extract_ids.py
python code/tweets/hydrate_tweets.py