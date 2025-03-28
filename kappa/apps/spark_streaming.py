from pyspark.sql import SparkSession
from pyspark.sql.types import *
import pyspark.sql.functions as F

stopwords = ['i', 'me', 'my', 'myself', 'we', 'our', 'ours', 
'ourselves', 'you', "you're", "you've", "you'll", "you'd", 'your', 'yours', 'yourself', 
'yourselves', 'he', 'him', 'his', 'himself', 'she', "she's", 'her', 'hers', 'herself', 
'it', "it's", 'its', 'itself', 'they', 'them', 'their', 'theirs', 'themselves',
 'what', 'which', 'who', 'whom', 'this', 'that', "that'll", 'these', 'those', 'am', 'is', 'are',
 'was', 'were', 'be', 'been', 'being', 'have', 'has', 'had', 'having', 'do', 'does', 'did', 'doing',
 'a', 'an', 'the', 'and', 'but', 'if', 'or', 'because', 'as', 'until', 'while', 'of', 'at', 'by', 'for', 
'with', 'about', 'against', 'between', 'into', 'through', 'during', 'before', 'after', 'above', 'below', 'to', 
'from', 'up', 'down', 'in', 'out', 'on', 'off', 'over', 'under', 'again', 'further', 'then', 'once', 
'here', 'there', 'when', 'where', 'why', 'how', 'all', 'any', 'both', 'each', 'few', 'more', 'most', 
'other', 'some', 'such', 'no', 'nor', 'not', 'only', 'own', 'same', 'so', 'than', 'too', 'very', 
's', 't', 'can', 'will', 'just', 'don', "don't", 'should', "should've", 'now', 'd', 'll', 'm', 'o', 're', 've', 'y',
 'ain', 'aren', "aren't", 'couldn', "couldn't", 'didn', "didn't", 'doesn', "doesn't", 
'hadn', "hadn't", 'hasn', "hasn't", 'haven', "haven't", 'isn', "isn't", 'ma', 'mightn',
 "mightn't", 'mustn', "mustn't", 'needn', "needn't", 'shan', "shan't", 'shouldn', "shouldn't", 
'wasn', "wasn't", 'weren', "weren't", 'won', "won't", 'wouldn', "wouldn't"]

# Schemas
twitchMessageSchema = StructType([
    StructField("timestamp", TimestampType()), 
    StructField("channel", StringType()),
    StructField("author", StringType()), 
    StructField("content", StringType())
])

############################################
# Other functions
############################################
def get_twitch_messages(spark):
    twitch_messages = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:29092") \
        .option("subscribe", "twitch_messages") \
        .option("startingOffsets", "latest") \
        .load() \
        .selectExpr("CAST(value AS STRING)") \
        .select(F.from_json(F.col("value"),twitchMessageSchema).alias("data")).select("data.*")
    return twitch_messages

def preprocess_messages(messages):
    messages = messages.withColumn("content", F.regexp_replace(F.col("content"), "\\s+", " ")) # Replace multiple spaces with a single space
    messages = messages.withColumn("content", F.trim(F.col("content"))) # Remove leading and trailing spaces
    messages = messages.withColumn("word_count", F.size(F.split(F.col("content"), " "))) # Count the number of words
    return messages

def write_to_cassandra(df, to_table, output_mode, trigger_time):
    def write_batch(df, epoch_id, to_table, output_mode):
        df.write \
            .format("org.apache.spark.sql.cassandra")\
            .mode("append")\
            .options(table=to_table, keyspace="projet")\
            .save()
        
    return df.writeStream \
            .option("spark.cassandra.connection.host","cassandra1:9042")\
            .foreachBatch(lambda df, epochId: write_batch(df, epochId, to_table, output_mode)) \
            .outputMode(output_mode) \
            .trigger(processingTime=trigger_time) \
            .start()

def save_to_parquet(df, path, checkpoint, trigger_time, output_mode):
    return df \
        .writeStream \
        .outputMode(output_mode) \
        .trigger(processingTime=trigger_time) \
        .format("parquet") \
        .option("path", path) \
        .option("checkpointLocation", checkpoint) \
        .start()

############################################
# Process twitch messages
############################################
def get_word_count(twitch_messages):
    messages_per_minute = twitch_messages \
        .withWatermark("timestamp", "1 minutes") \
        .groupBy(
            "channel",
            F.window("timestamp", "1 minute")
        ) \
        .sum("word_count") \
        .withColumnRenamed("sum(word_count)", "wordcount") \
        .selectExpr("channel", "window.start as timestamp", "wordcount")
    return messages_per_minute

def get_most_frequent_words(twitch_messages, with_watermark):
    words = twitch_messages \
        .withColumn("word", (F.explode(F.split(F.col("content"), " ")))) \
        .withColumn("word", F.trim(F.col("word"))) \
        .filter(F.col("word") != "") \
        .withColumn("word_lower", F.lower(F.col("word"))) \
        .filter(~F.col("word_lower").isin(stopwords)) \

    if with_watermark:
        most_frequent_words = words \
            .withWatermark("timestamp", "2 minute") \
            .groupBy("channel", "word", F.window("timestamp", "5 minute")) \
            .count() \
            .selectExpr("channel", "word", "count", "window.start as window_start", "window.end as window_end")
    else:
        most_frequent_words = words \
            .groupBy("channel", "word") \
            .count() \
            .selectExpr("channel", "word", "count")
    
    return most_frequent_words

def get_messages_per_user(twitch_messages, with_watermark):
    if with_watermark:
        messages_per_user = twitch_messages \
            .withWatermark("timestamp", "2 minute") \
            .groupBy("channel", "author", F.window("timestamp", "5 minute")) \
            .count() \
            .selectExpr("channel", "author as user", "count as msgcount", "window.start as window_start", "window.end as window_end")
    else:
        messages_per_user = twitch_messages \
            .groupBy("channel", "author") \
            .count() \
            .selectExpr("channel", "author as user", "count as msgcount")
    return messages_per_user


############################################
# Main
############################################
def main():

    # Start Spark
    spark = SparkSession.builder.appName("Twitch Messages").getOrCreate()
    spark._sc.setLogLevel("OFF")

    # Get twitch messages from Kafka
    twitch_messages = get_twitch_messages(spark)
    twitch_messages = preprocess_messages(twitch_messages)
    print("\nTwitch messages schema:")
    twitch_messages.printSchema()


    # Process messages
    messages_per_minute = get_word_count(twitch_messages)
    print("\nMessages per minute schema:")
    messages_per_minute.printSchema()

    most_frequent_words = get_most_frequent_words(twitch_messages, with_watermark=False)
    print("\nMost frequent words schema:")
    most_frequent_words.printSchema()

    messages_per_user = get_messages_per_user(twitch_messages, with_watermark=False)
    print("\nMessages per user schema:")
    messages_per_user.printSchema()
    

    # Write to Cassandra
    query_msg_per_min = write_to_cassandra(messages_per_minute, "wordcount", "update", "1 minute")
    query_freq_words = write_to_cassandra(most_frequent_words, "wordfreq", "update", "5 minute")
    query_msg_per_user = write_to_cassandra(messages_per_user, "msgperuser", "update", "5 minute")

    
    # Save files
    most_frequent_words_wm = get_most_frequent_words(twitch_messages, with_watermark=True)
    messages_per_user_wm = get_messages_per_user(twitch_messages, with_watermark=True)

    path = "/opt/spark-data/data/"
    checkpoint = "/opt/spark-data/checkpoints/"
    save_to_parquet(messages_per_minute, path + "messages_per_minute", checkpoint + "messages_per_minute", "1 minute", "append")
    save_to_parquet(most_frequent_words_wm, path + "most_frequent_words", checkpoint + "most_frequent_words", "5 minute", "append")
    save_to_parquet(messages_per_user_wm, path + "messages_per_user", checkpoint + "messages_per_user", "5 minute", "append")
    save_to_parquet(twitch_messages, path + "twitch_messages", checkpoint + "twitch_messages", "5 minute", "append")

    # Start the streaming queries
    query_msg_per_min.awaitTermination()
    query_freq_words.awaitTermination()
    query_msg_per_user.awaitTermination()


if __name__ == "__main__":
    main()