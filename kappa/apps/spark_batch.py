from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.window import Window
import pyspark.sql.functions as F

# Create a Spark session
spark : SparkSession = SparkSession.builder.appName("Twitch Messages Batch").getOrCreate()
spark._sc.setLogLevel("OFF")

# Read the Parquet file into a DataFrame
messages_per_minute = spark.read.parquet("/opt/spark-data/data/messages_per_minute")
messages_per_user = spark.read.parquet("/opt/spark-data/data/messages_per_user")
most_frequent_words = spark.read.parquet("/opt/spark-data/data/most_frequent_words")
twich_messages = spark.read.parquet("/opt/spark-data/data/twitch_messages")

# Compute the number of words per 10 minutes
print("Computing the number of words per 10 minutes...")
new_messages_per_minute = messages_per_minute \
    .groupBy("channel", F.window("timestamp", "10 minutes")) \
    .max("wordcount") \
    .withColumnRenamed("max(wordcount)", "wordcount") \
    .selectExpr("channel", "window.start as timestamp", "wordcount") \
    .orderBy("channel", "timestamp")

# Compute the number of messages per user
print("Computing the number of messages per user...")
new_messages_per_user = messages_per_user \
    .groupBy("channel", "user") \
    .max("msgcount") \
    .withColumnRenamed("max(msgcount)", "msgcount") \
    .withColumn("rank", F.row_number().over(
        Window.partitionBy("channel").orderBy(F.desc("msgcount"))
    )) \
    .filter(F.col("rank") <= 3) \
    .drop("rank") \
    .orderBy("channel", F.desc("msgcount")) \
    
# Compute the most frequent words
print("Computing the most frequent words...")
new_most_frequent_words = most_frequent_words \
    .groupBy("channel", "word") \
    .max("count") \
    .withColumnRenamed("max(count)", "count") \
    .withColumn("rank", F.row_number().over(
        Window.partitionBy("channel").orderBy(F.desc("count"))
    )) \
    .filter(F.col("rank") <= 5) \
    .drop("rank") \
    .orderBy("channel", F.desc("count")) \

# Send to Cassandra
print("\nWriting to Cassandra...")
new_messages_per_minute.write \
    .format("org.apache.spark.sql.cassandra") \
    .options(table="wordcount_batch", keyspace="projet") \
    .mode("append") \
    .save()

new_messages_per_user.write \
    .format("org.apache.spark.sql.cassandra") \
    .options(table="msgperuser_batch", keyspace="projet") \
    .mode("append") \
    .save()
    
new_most_frequent_words.write \
    .format("org.apache.spark.sql.cassandra") \
    .options(table="wordfreq_batch", keyspace="projet") \
    .mode("append") \
    .save()

print("Done.")
spark.stop()