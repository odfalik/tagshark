from config import *
from pyspark import SparkConf, SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import SQLContext
from elasticsearch import Elasticsearch


es = Elasticsearch([{
    'host': ES_IP,
    'port': ES_PORT
}])

idx = es.get(index='tweettest', id=1)
print(idx)