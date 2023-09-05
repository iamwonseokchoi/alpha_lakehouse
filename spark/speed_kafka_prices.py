# from pyspark.sql import SparkSession
# from pyspark.sql.types import *
# from pyspark.sql.functions import from_json, col, current_timestamp
# import os 
# import json 
# import datetime
# from dotenv import load_dotenv
# import logging

# load_dotenv()
# tickers = json.loads(os.getenv('TICKERS'))
# topics_string = (','.join(tickers)).lower()

# schema_prices = StructType([
#     StructField("t", LongType(), False),
#     StructField("o", DoubleType(), True),
#     StructField("h", DoubleType(), True),
#     StructField("l", DoubleType(), True),
#     StructField("c", DoubleType(), True),
#     StructField("v", LongType(), True),
#     StructField("vw", DoubleType(), True),
#     StructField("n", LongType(), True),
#     StructField("symbol", StringType(), False)
# ])

# def process_batch(df, batch_id):
#     topic_schemas = {ticker.lower(): schema_prices for ticker in tickers}
#     for topic, schema in topic_schemas.items():
#         filtered_df = df.filter(df["topic"] == topic)
#         json_df = filtered_df \
#             .selectExpr("CAST(value AS STRING)") \
#             .select(from_json(col("value"), schema).alias("parsed_value")) \
#             .select("parsed_value.*")
        
#         # Skip malformed records
#         valid_rows_df = json_df.filter(
#             (json_df.t.isNotNull()) & 
#             (json_df.symbol.isNotNull())
#         )

#         if valid_rows_df.count() == 0:
#             logging.warning(f"No valid rows for topic {topic}")
#             continue

#         transformed_df = valid_rows_df \
#             .withColumnRenamed("t", "timestamp") \
#             .withColumnRenamed("o", "open") \
#             .withColumnRenamed("h", "high") \
#             .withColumnRenamed("l", "low") \
#             .withColumnRenamed("c", "close") \
#             .withColumnRenamed("v", "volume") \
#             .withColumnRenamed("vw", "volume_weighted") \
#             .withColumnRenamed("n", "transactions")
        
#         json_df_with_timestamp = transformed_df \
#             .withColumn("updated_at", current_timestamp())

#         json_df_with_timestamp.write \
#             .format("org.apache.spark.sql.cassandra") \
#             .mode("append") \
#             .options(table=topic, keyspace="price") \
#             .save()

# # Initialize Spark session
# spark = SparkSession.builder \
#     .appName("KafkaPricesToCassandra") \
#     .config("spark.cassandra.connection.host", "localhost") \
#     .config("spark.cassandra.connection.port", "9042") \
#     .config("spark.cassandra.auth.username", "cassandra") \
#     .config("spark.cassandra.auth.password", "cassandra") \
#     .config("spark.driver.host", "localhost") \
#     .getOrCreate()

# spark.sparkContext.setLogLevel("ERROR")

# # Read Kafka stream
# df = spark \
#     .readStream \
#     .format("kafka") \
#     .option("kafka.bootstrap.servers", "localhost:9092") \
#     .option("subscribe", topics_string) \
#     .option("failOnDataLoss", "false") \
#     .load()

# # Start the stream and apply the batch processing function
# query = df \
#     .writeStream \
#     .foreachBatch(process_batch) \
#     .option("checkpointLocation", "./volumes/kafka_prices_checkpoint") \
#     .start()

# query.awaitTermination()


# spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.0,com.datastax.spark:spark-cassandra-connector_2.12:3.4.1 spark/speed_kafka_prices.py

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType
from pyspark.sql.functions import current_timestamp, col, from_json

# Define schema for incoming Kafka data
schema_prices = StructType([
    StructField("t", LongType(), False),
    StructField("o", DoubleType(), True),
    StructField("h", DoubleType(), True),
    StructField("l", DoubleType(), True),
    StructField("c", DoubleType(), True),
    StructField("v", LongType(), True),
    StructField("vw", DoubleType(), True),
    StructField("n", LongType(), True),
    StructField("symbol", StringType(), False)
])

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("KafkaPricesToCassandra") \
    .config("spark.cassandra.connection.host", "localhost") \
    .config("spark.cassandra.connection.port", "9042") \
    .config("spark.cassandra.auth.username", "cassandra") \
    .config("spark.cassandra.auth.password", "cassandra") \
    .getOrCreate()

# Read data from Kafka
kafkaStreamDF = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "aapl,amzn,googl,msft,nvda,tsla") \
    .option("failOnDataLoss", "false") \
    .load()

# Deserialize the JSON data
jsonParsedDF = kafkaStreamDF \
    .select(from_json(col("value").cast("string"), schema_prices).alias("parsed_value"), col("topic")) \
    .select("parsed_value.*", "topic")

# Rename columns and add an updated_at column
renamedDF = jsonParsedDF \
    .withColumnRenamed("t", "timestamp") \
    .withColumnRenamed("o", "open") \
    .withColumnRenamed("h", "high") \
    .withColumnRenamed("l", "low") \
    .withColumnRenamed("c", "close") \
    .withColumnRenamed("v", "volume") \
    .withColumnRenamed("vw", "volume_weighted") \
    .withColumnRenamed("n", "transactions") \
    .withColumn("updated_at", current_timestamp())

def process_batch(df, batch_id):
    topic_schemas = {
        'aapl': schema_prices,
        'amzn': schema_prices,
        'googl': schema_prices,
        'msft': schema_prices,
        'nvda': schema_prices,
        'tsla': schema_prices
    }

    for topic, schema in topic_schemas.items():
        filtered_df = df.filter(df["topic"] == topic)
        # Drop the 'topic' column from the DataFrame
        filtered_df = filtered_df.drop("topic")

        filtered_df.write \
            .format("org.apache.spark.sql.cassandra") \
            .mode("append") \
            .options(table=topic, keyspace="price") \
            .save()


# Stream data to Cassandra, partitioned by the "symbol" column
query = renamedDF \
    .writeStream \
    .foreachBatch(process_batch) \
    .outputMode("append") \
    .option("checkpointLocation", "./volumes/kafka_prices_checkpoint") \
    .start()

# Await termination
query.awaitTermination()

