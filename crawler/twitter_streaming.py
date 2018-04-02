#Import the necessary methods from tweepy library
from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream
import time
import datetime
import sys


#Variables that contains the user credentials to access Twitter API 
access_token = "28117038-uafK7WRfsvhY7DjvIvJ8G7F12R90fWEpPvz0q1mlZ"
access_token_secret = "3SwssbkhCYtulGTsAzGg6nAQMPVFHy05lOio5WQ4k5Woh"
consumer_key = "zkouwShFHI6USdDvrT9LFmOvF"
consumer_secret = "hBbNawF740L6JybefqBarijv0XfdCRzhbBiMD3M8yMnWAiRNhX"


#This is a basic listener that just write received tweets to a file.
class MyTweetReader(StreamListener):

	def __init__(self, time_limit=60, path =""):
		self.start_time = time.time()
		self.limit = time_limit
		self.path = path
		super(MyTweetReader, self).__init__()

	def on_data(self, data):
		print (data)
		if (time.time() - self.start_time) < self.limit:
			now = datetime.datetime.now()
			#print now.year, now.month, now.day, now.hour, now.minute, now.second
			fn = str(now.year)+"-"+str(now.month)+"-"+str(now.day)+"-"+str(now.hour)
			#fn = str(time.time())
			saveFile = open(self.path+""+fn+'.json', 'a')
			saveFile.write(data)
			saveFile.close()
			return True
			
		else:
			return False

	def on_error(self, status):
		print (status)
		if status == 420:#if its due to over-rate
			time.sleep(60)


if __name__ == '__main__':
	print sys.argv
	if len(sys.argv) > 2:
		t = long(sys.argv[1])
		p = path=sys.argv[2]
	elif len(sys.argv) > 1:
		t = long(sys.argv[1])
		p = ""
	else:
		t = 60
		p = ""
    #This handles Twitter authetification and the connection to Twitter Streaming API
	reader = MyTweetReader(time_limit=t, path = p)
	auth = OAuthHandler(consumer_key, consumer_secret)
	auth.set_access_token(access_token, access_token_secret)
	stream = Stream(auth, reader)

    #This line filter Twitter Streams to capture data for only USA location
	stream.filter(locations=[-140.0,25.0,-50.0,60.0]) #USA
