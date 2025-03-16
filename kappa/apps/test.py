from pyspark.sql import SparkSession
from pyspark.sql.types import *
import pyspark.sql.functions as F

from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('Exercice2').getOrCreate()
spark._sc.setLogLevel("OFF")

# Create a dataframe that stores twitch messages 
df = spark.createDataFrame([
    ("2019-12-01 00:00:00", "user1", "bonjour ceci est un message"),
    ("2019-12-01 00:00:01", "user2", "autre message"),
    ("2019-12-01 00:00:02", "user1", "message de  user1  ")], 
    ["timestamp", "user", "message"])

# Split the messages into words and explode the array to get individual words
df_words = df.withColumn("word", F.explode(F.split(F.col("message"), " ")))

# Group by word and count the occurrences
df_word_freq = df_words.groupBy("word").count()

# Sort the words by frequency in descending order
df1 = df_word_freq.orderBy(F.desc("count"))

print(df1.show())