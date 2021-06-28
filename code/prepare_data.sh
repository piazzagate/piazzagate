#!/bin/bash

cd "$(dirname "$0")/../data/raw"

unzip corona_tweets_1-100.zip
unzip corona_tweets_101-200.zip
unzip corona_tweets_201-300.zip
unzip corona_tweets_301-400.zip -d corona_tweets_301-400

mkdir tweets
mv corona_tweets_1-100/* tweets
mv corona_tweets_101-200/* tweets

unzip "corona_tweets_201-300/*.zip" -d tweets
unzip "corona_tweets_301-400/*.zip" -d tweets

rm -rf corona_tweets_*