#Import the necessary methods from tweepy library
import sys
import tweepy
import time
from random import randint
import glob

#Variables that contains the user credentials to access Twitter API 
ACCESS_TOKEN = "28117038-kkVlVZqXdeZ7K5lBzNgXc9dxmGbDqemAy1cayNgyd"
ACCESS_TOKEN_SECRET = "HixRiZWHyzPJFD3GZtW5NpiZSOGwhkHuPSzwSB5CnIAbW"
CONSUMER_KEY = "P1ynzZDp4IquoQxtp3F2jhjkz"
CONSUMER_SECRET = "D5TEYL48yFbjfpuusSjYVGrQe0QTGC7Ne4hlhFipeKSTo8rvXK"

try:
	auth = tweepy.OAuthHandler(CONSUMER_KEY, CONSUMER_SECRET)
	auth.set_access_token(ACCESS_TOKEN, ACCESS_TOKEN_SECRET)
	api = tweepy.API(auth, wait_on_rate_limit=True, wait_on_rate_limit_notify=True, compression=True)
	
	for file in glob.glob('../sample_user_network/users_stream.txt'):
		a=open(file,'r')
        
		for x in a:
			cur_user = long(x)
			saveFile = open('../sample_user_network/network.csv', 'a')
			print cur_user
	
			c = tweepy.Cursor(api.friends_ids, id = cur_user) #api.friends_ids #api.followers_ids 
			#print "type(c)=", type(c)
			
			#ids = []
			try:
				for page in c.pages():

					for e in page:
						data = str(cur_user)+"\t"+str(e)+"\n"
						saveFile.write(data)
				#ids.append(page)
			#print "ids=", ids
			#print "ids[0]=", ids[0]
			#print "len(ids[0])=", len(ids[0])
	#print 5/0
			except:
				continue
			c2 = tweepy.Cursor(api.followers_ids, id = cur_user) #api.friends_ids #api.followers_ids 
			#print "type(c)=", type(c)
			#ids = []
			try:
				for page in c2.pages():
					for f in page:
						data2 = str(f)+"\t"+str(cur_user)+"\n"
						saveFile.write(data2)
				#ids.append(page)
			#print "ids=", ids
			#print "ids[0]=", ids[0]
			#print "len(ids[0])=", len(ids[0])
	#print 5/0
			except:
				continue
			saveFile.close()
		
		
except tweepy.TweepError:
	print "tweepy.TweepError=", tweepy.TweepError
except:
	e = sys.exc_info()[0]
	print "Error: %s" % e
    #print "error."
