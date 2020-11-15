from config import *
import json
from pyspark import SparkConf, SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import SQLContext
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
from geopy.geocoders import Nominatim
from elasticsearch import Elasticsearch


def process_partition(partition):

    # Perform resource-heavy connections/intializations per partition
    es = Elasticsearch([{'host': ES_IP, 'port': ES_PORT}])
    vader = SentimentIntensityAnalyzer()
    geolocator = Nominatim(user_agent='tagshark')

    for tweet in partition:
        tweet = json.loads(tweet)

        # Sentiment analysis
        tweet['sentiment'] = vader.polarity_scores(tweet['text'])['compound']

        # Location analysis
        if 'location' in tweet and isinstance(tweet['location'], str):
            geoloc = geolocator.geocode(tweet['location'])
            if geoloc is not None:
                tweet['location'] = [geoloc.latitude, geoloc.longitude]

        # Remove data
        tweet.pop('text', None)

        # Push document to ES
        res = es.index(index='tagshark', body=tweet)


sc = SparkContext(
    appName='tagshark',
    master='local[2]'
)
sc.setLogLevel('ERROR')

ssc = StreamingContext(sc, 4)   # 4s interval
ssc.checkpoint('checkpoint')

stream = ssc.socketTextStream(STREAM_IP, STREAM_PORT)
stream.foreachRDD(lambda r: r.foreachPartition(process_partition))

ssc.start()
ssc.awaitTermination()
