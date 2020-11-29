from config import *
import json
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
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
        try:
            if 'location' in tweet and isinstance(tweet['location'], str):
                geoloc = geolocator.geocode(tweet['location'])
                if geoloc is not None:
                    tweet['location'] = [geoloc.latitude, geoloc.longitude]
        except Exception as e:
            print('Nominatim error: ' + str(e))

        # Indexing
        if 'location' in tweet and isinstance(tweet['location'], list):

            tweet['location'] = ','.join([str(el) for el in tweet['location']])

            # Remove data
            tweet.pop('text', None)

            # Write document to ES
            try:
                es.index(index='tagshark', body=tweet)
            except Exception as e:
                print('ES indexing error: ' + str(e))


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
