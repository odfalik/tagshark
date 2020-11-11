from config import *
import json
from pyspark.sql import SparkSession
from pyspark.sql.types import TimestampType, StringType, StructType, StructField
from pyspark.sql.functions import explode
from pyspark.sql.functions import split

spark = SparkSession \
    .builder \
    .appName('tagshark') \
    .getOrCreate()

schema = StructType([ StructField('text', StringType(), True),
                      StructField('location', StringType(), True)])

# Create DataFrame representing the stream of input tweets from connection
tweets = spark \
    .readStream \
    .format('socket') \
    .option('host', TCP_IP) \
    .option('port', TCP_PORT) \
    .load()

# Split the tweets into words
# words = tweets.select(
#     explode(
#         split(tweets.value, ' ')
#     ).alias('word')
# )

# # Generate running word count
# wordCounts = words.groupBy('word').count()

query = tweets \
    .writeStream \
    .outputMode('complete') \
    .format('console') \
    .start()

query.awaitTermination()
