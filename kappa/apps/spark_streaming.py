from pyspark.sql import SparkSession
from pyspark.sql.types import *
import pyspark.sql.functions as F
from pyspark.sql.window import Window

# Schemas
twitchMessageSchema = StructType([
    StructField("timestamp", TimestampType()), 
    StructField("channel", StringType()),
    StructField("author", StringType()), 
    StructField("content", StringType())
])

def writeToCassandra(writeDF, epochId, table, mode="append"):
    writeDF.write \
        .format("org.apache.spark.sql.cassandra")\
        .mode(mode)\
        .options(table=table, keyspace="projet")\
        .save()
    
def main():

    spark = SparkSession.builder \
        .appName("Twitch Messages") \
        .getOrCreate()

    spark._sc.setLogLevel("OFF")

    twitch_messages = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:29092") \
        .option("subscribe", "twitch_messages") \
        .option("startingOffsets", "latest") \
        .load() \
        .selectExpr("CAST(value AS STRING)") \
        .select(F.from_json(F.col("value"),twitchMessageSchema).alias("data")).select("data.*")

    twitch_messages.printSchema()

    # Number of words per minute
    twitch_messages = twitch_messages.withColumn("content", F.regexp_replace(F.col("content"), "\\s+", " "))
    twitch_messages = twitch_messages.withColumn("content", F.trim(F.col("content")))
    twitch_messages = twitch_messages.withColumn("word_count", F.size(F.split(F.col("content"), " ")))

    messages_per_minute = twitch_messages \
        .groupBy(
            "channel",
            F.window("timestamp", "1 minute")
        ) \
        .sum("word_count") \
        .withColumnRenamed("sum(word_count)", "wordcount") \
        .selectExpr("channel", "window.start as timestamp", "wordcount")

    query_msg_per_min =messages_per_minute.writeStream \
            .option("spark.cassandra.connection.host","cassandra1:9042")\
            .foreachBatch(lambda df, epochId: writeToCassandra(df, epochId, "wordcount", "append")) \
            .outputMode("update") \
            .start()\
    
    # Most frequent words
    most_frequent_words = twitch_messages \
        .select(F.explode(F.split(F.col("content"), " ")).alias("word")) \
        .groupBy("word") \
        .count() \
        .selectExpr("word", "count").orderBy(F.desc("count")).limit(5)
    
    most_frequent_words.printSchema()
    
    query_freq_words = most_frequent_words.writeStream \
        .option("spark.cassandra.connection.host","cassandra1:9042")\
        .foreachBatch(lambda df, epochId: writeToCassandra(df, epochId, "wordfreq", "append")) \
        .outputMode("complete") \
        .start()\
    
    # Number of messages per active user
    messages_per_user = twitch_messages \
        .groupBy("channel", "author") \
        .count() \
        .selectExpr("channel", "author as user", "count as msgcount") \
    
    messages_per_user.printSchema()

    query_msg_per_user = messages_per_user.writeStream \
        .option("spark.cassandra.connection.host","cassandra1:9042")\
        .foreachBatch(lambda df, epochId: writeToCassandra(df, epochId, "msgperuser", "append")) \
        .outputMode("update") \
        .start()
    
    query_msg_per_min.awaitTermination()
    query_freq_words.awaitTermination()
    query_msg_per_user.awaitTermination()

if __name__ == "__main__":
    main()