# Daniel Roberts
# Daoqun Yang

# CS 4371 Final Project
# This program is used to accept data from Stream.py, 
# process it using spark and then output it as a series of text files.
# This data is then used in Analysis.py for data analysis.

from pyspark import SparkConf, SparkContext
from pyspark.streaming import StreamingContext
from textblob import TextBlob
import json
import os
import time
    
TCP_IP = 'localhost'
TCP_PORT = 9002

def processTweet(tweet):

    # Here, you should implement:
    # (i) Sentiment analysis,
    # (ii) Get data corresponding to place where the tweet was generate (using geopy or googlemaps)
    # (iii) Index the data using Elastic Search 
        
    tweetData = tweet.split("::")
    
    # If data is proper length
    if (len(tweetData) > 1):
        location = tweetData[0].split("~")
        text = tweetData[1]
        
        # Sentiment Analysis
        rate = TextBlob(text).polarity
        check = ""
        if rate > 0.05: 
            check = "positive"
        elif rate < -0.05: 
            check = "negative"
        else: 
            check = "neutral"

        return json.dumps({"long" : location[0], "lat" : location[1], "add" : location[2], "text" : text, "sent" : check, "rate" : str(rate)})
    else:
        return json.dumps({"len" : str(len(tweetData)), "tweet" : tweetData})
    
# Pyspark
# create spark configuration
conf = SparkConf()
conf.setAppName('TwitterApp')
conf.setMaster('local[2]')

# create spark context with the above configuration
sc = SparkContext.getOrCreate(conf=conf)

# create the Streaming Context from spark context with interval size 4 seconds
ssc = StreamingContext(sc, 4)
ssc.checkpoint("checkpoint_TwitterApp")

# read data from port 9001
dataStream = ssc.socketTextStream(TCP_IP, TCP_PORT)

# Mapping data then saving
dataStream = dataStream.map(processTweet)
dataStream.pprint()
dataStream.repartition(1).saveAsTextFiles("/home/daniel/Desktop/4371 Proj/Stream/Output")

# Running for set time
ssc.start()
ssc.awaitTerminationOrTimeout(400)
ssc.stop()
