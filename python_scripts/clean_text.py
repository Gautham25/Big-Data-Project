import json
import re
import glob
import sys
from string import punctuation



for file in glob.glob('../sample_tweets/filtered_tweets.json'):
	a=open(file,'r')
	for x in a:
		d = json.loads(x)
		if 'full_text' in d:
			text=d['full_text']
		else:
			text=d['text'] 
		text_clean = re.sub(r'[^\x00-\x7F]+','', text)
		text_clean = re.sub(r'https?://[^\s<>"]+|www\.[^\s<>"]+','',text_clean)
		text_clean = re.sub(r'@\w*','',text_clean)
		text_clean = re.sub(r'\$[a-zA-Z0-9]*', '', text_clean)
		text_clean = re.sub(r'&amp;quot;|&amp;amp''', '', text_clean)
		text_clean=text_clean.lower()
		for p in list(punctuation):
			if(p!='#'):
				text_clean=text_clean.replace(p,'')
		d['textclean']=text_clean
		tweet_file=open("../sample_tweets/cleaned_tweets.json",'a')
		json.dump(d, tweet_file)
		tweet_file.write("\n")
		tweet_file.close()