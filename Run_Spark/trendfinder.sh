#!/bin/bash
spark-submit --class edu.ucr.cs.cs226.GroupG.Main --master local --driver-memory 8g CS226.Project-1.0-SNAPSHOT-jar-with-dependencies.jar ../sample_tweets/processed_tweets.json ../datasets/COUNTY.csv $1 ../sample_user_network/network.csv $2 $3