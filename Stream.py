# Daniel Roberts
# Daoqun Yang

# CS 4371 Final Project
# This program is used to stream data live from twitter,
# sends it to Spark.py via a socket connection, where it
# is then processed using spark.

import tweepy
import socket
import re
from geopy.geocoders import Nominatim
from geopy.exc import GeocoderTimedOut
import time
import json

# Enter your Twitter keys here!!!
ACCESS_TOKEN = "XXXXXX"
ACCESS_SECRET = "XXXXXX"
CONSUMER_KEY = "XXXXXX"
CONSUMER_SECRET = "XXXXXX"


auth = tweepy.OAuthHandler(CONSUMER_KEY, CONSUMER_SECRET)
auth.set_access_token(ACCESS_TOKEN, ACCESS_SECRET)


hashtag = '#covid19'

TCP_IP = 'localhost'
TCP_PORT = 9002



def preprocessing(tweet):
    emoji_pattern = re.compile("["
                       u"\U0001F600-\U0001F64F"  # emoticons
                       u"\U0001F300-\U0001F5FF"  # symbols & pictographs
                       u"\U0001F680-\U0001F6FF"  # transport & map symbols
                       u"\U0001F1E0-\U0001F1FF"  # flags (iOS)
                       u"\U00002702-\U000027B0"
                       u"\U000024C2-\U0001F251"
                       u"\U0001f926-\U0001f937"
                       u"\u200d"
                       u"\u2640-\u2642" 
                       "]+", flags=re.UNICODE)
    
    tweet = emoji_pattern.sub(r'', tweet)
    tweet = tweet.replace("\r","")
    tweet = tweet.replace("\n","")
    return tweet

def getTweet(status):
    
    # You can explore fields/data other than location and the tweet itself. 
    # Check what else you could explore in terms of data inside Status object

    tweet = ""
    location = ""

    location = status.user.location
    
    if hasattr(status, "retweeted_status"):  # Check if Retweet
        try:
            tweet = status.retweeted_status.extended_tweet["full_text"]
        except AttributeError:
            tweet = status.retweeted_status.text
    else:
        try:
            tweet = status.extended_tweet["full_text"]
        except AttributeError:
            tweet = status.text

    return location, preprocessing(tweet)

# create sockets
s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
s.bind((TCP_IP, TCP_PORT))
s.listen(1)
print("Accepting")
conn, addr = s.accept()
print("Accepted")

class MyStreamListener(tweepy.StreamListener):

    def on_status(self, status):
        location, tweet = getTweet(status)
        geolocator = Nominatim(user_agent="user")

        if (location != None and tweet != None):
            
            # Getting location info
            try:
                location = geolocator.geocode(location)
            except GeocoderTimedOut as e:
                location = None
            
            # If geopy gets location
            if (location != None):
                info = str(location.longitude) + "~" + str(location.latitude) + "~" + location.address + "::" + tweet + "\n"

                print(info)
                print("------------------------------------------------------")

                if (len(info.split("::")) > 1):
                    conn.send(info.encode('utf-8'))
                else:
                    print("Error:")
                    print(info)
                    print("------------------------------------------------------")

        return True
        
    def on_error(self, status_code):
        if status_code == 420:
            return False
        else:
            print(status_code)

# Opening stream as an async to allow input to be read in
myStream = tweepy.Stream(auth=auth, listener=MyStreamListener())
myStream.filter(track=[hashtag], languages=["en"], is_async = True)

# Reading new hashtags in
new = hashtag
while True:
    new = str(input())
    
    if new == "q":
        break
        
    if new != hashtag:
        myStream.disconnect()
        myStream.filter(track=[new], languages=["en"], is_async = True)

    hashtag = new

myStream.disconnect()
s.close()
