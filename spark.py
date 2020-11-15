from config import *
import json
from pyspark import SparkConf, SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import SQLContext
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
from geopy.geocoders import Nominatim
# from textblob import TextBlob
from elasticsearch import Elasticsearch


def process_partition(partition):
    es = Elasticsearch([{'host': ES_IP,'port': ES_PORT}])
    vader = SentimentIntensityAnalyzer()

    for tweet in partition:
        tweet = json.loads(tweet)
        text = tweet['text']
        location = tweet['location']

        doc = {
            'location': location,
            'sentiment': vader.polarity_scores(text),
            'text': text
        }

        res = es.index(index='tagshark', body=doc)
        print(res)


sc = SparkContext(
    appName='tagshark',
    master='local[2]'
)
sc.setLogLevel("ERROR")

ssc = StreamingContext(sc, 4)   # 4s interval
ssc.checkpoint("checkpoint")

stream = ssc.socketTextStream(STREAM_IP, STREAM_PORT)
stream.foreachRDD(lambda r: r.foreachPartition(process_partition))

ssc.start()
ssc.awaitTermination()
