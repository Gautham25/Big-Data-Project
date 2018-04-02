#!/bin/bash
python ../python_scripts/tweets.py
python ../python_scripts/clean_text.py
spark-submit --class edu.ucr.cs.cs226.GroupG.Twitter_clean --master local spark-twitter-1.0-SNAPSHOT.jar ../sample_tweets/cleaned_tweets.json