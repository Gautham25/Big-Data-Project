import json
import re
import glob
import sys

#tweet_file=open("/media/ssing068/My Passport/cs226/clean_tweets.json",'w')
#for file in glob.glob('/media/ssing068/My Passport/cs226/twitter_stream/*.json'):
tweet_file=open("../sample_tweets/filtered_tweets.json",'w')
for file in glob.glob('../sample_tweets/*.json'):
	print file
	a=open(file,'r')
	for x in a:
		d = json.loads(x)
		if 'id' in d and d["lang"]=="en" and 'place' in d:			
			tweet_file.write(str(x))
tweet_file.close()