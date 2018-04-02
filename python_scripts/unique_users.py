import json
from sets import Set
import glob
import sys

u=Set([])
for file in glob.glob('../sample_tweets/*.json'):
	a=open(file,'r')
	for x in a:
		d = json.loads(x)
		if 'id' not in d:
			continue
		y= d['user']['id']
		u.add(y)


user_file=open("../sample_user_network/users_stream.txt",'w')
for user in u:
	user_file.write(str(user)+"\n")
user_file.close()
    
