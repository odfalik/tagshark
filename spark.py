from config import *
import json
from pyspark import SparkConf, SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import SQLContext
# from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
# from geopy.geocoders import Nominatim
# from textblob import TextBlob
# from elasticsearch import Elasticsearch

# vader = SentimentIntensityAnalyzer()


# Here, you should implement:
# (i) Sentiment analysis,
# (ii) Get data corresponding to place where the tweet was generate (using geopy or googlemaps)
# (iii) Index the data using Elastic Search
def processTweet(formatted_tweet):
    tweet = formatted_tweet.split('::')
    text = tweet[0]
    location = tweet[1]

    # print('\n' + location)
    # if len(tweet) > 0:
    #     text = tweet['text']
    #     rawLocation = tweet['location']

    #     print("\n\n=========================\ntweet: ", text)

    # (i) Apply Sentiment analysis in "text"
    # print(f'sentiment: {vader.polarity_scores(text)}')

    # (ii) Get geolocation (state, country, lat, lon, etc...) from rawLocation

    # print("Raw location from tweet status: ", rawLocation)
    # print("lat: ", lat)
    # print("lon: ", lon)
    # print("state: ", state)
    # print("country: ", country)
    # print("Text: ", text)
    # print("Sentiment: ", sentiment)

    # (iii) Post the index on ElasticSearch or log your data in some other way (you are always free!!)


conf = SparkConf()
conf.setAppName('tagshark')
conf.setMaster('local[2]')  # 2-core master
sc = SparkContext(conf=conf)

ssc = StreamingContext(sc, 4)   # 4s interval
# ssc.checkpoint("checkpoint_tagshark")


dataStream = ssc.socketTextStream(TCP_IP, TCP_PORT) \
    .foreachRDD(lambda rdd: rdd.foreach(processTweet))

ssc.start()
ssc.awaitTermination()
