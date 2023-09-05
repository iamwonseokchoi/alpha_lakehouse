from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import from_json, col, current_timestamp

# Define Schemas for each topic
schema_stats = StructType([
    StructField("timestamp", LongType(), False),
    StructField("value", DoubleType(), True),
    StructField("symbol", StringType(), False)
])

schema_macd = StructType([
    StructField("timestamp", LongType(), False),
    StructField("value", DoubleType(), True),
    StructField("signal", DoubleType(), True),
    StructField("histogram", DoubleType(), True),
    StructField("symbol", StringType(), False)
])

# Process each batch of data
def process_batch(df, batch_id):
    topic_schemas = {
        'sma': schema_stats,
        'ema': schema_stats,
        'macd': schema_macd,
        'rsi': schema_stats
    }

    for topic, schema in topic_schemas.items():
        filtered_df = df.filter(df["topic"] == topic)
        json_df = filtered_df \
            .selectExpr("CAST(value AS STRING)") \
            .select(from_json(col("value"), schema).alias("parsed_value")) \
            .select("parsed_value.*")
        
        json_df_with_timestamp = json_df \
            .withColumn("updated_at", current_timestamp())

        json_df_with_timestamp.write \
            .format("org.apache.spark.sql.cassandra") \
            .mode("append") \
            .options(table=topic, keyspace="technicals") \
            .save()

# Initialize Spark session
spark = SparkSession.builder \
    .appName("KafkaTechnicalsToCassandra") \
    .config("spark.cassandra.connection.host", "localhost") \
    .config("spark.cassandra.connection.port", "9042") \
    .config("spark.cassandra.auth.username", "cassandra") \
    .config("spark.cassandra.auth.password", "cassandra") \
    .config("spark.driver.host", "localhost") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# Read Kafka stream
df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "sma, ema, macd, rsi") \
    .option("failOnDataLoss", "false") \
    .load()

# Start the stream and apply the batch processing function
query = df \
    .writeStream \
    .foreachBatch(process_batch) \
    .option("checkpointLocation", "./volumes/kafka_technicals_checkpoint") \
    .start()

query.awaitTermination()


# spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.0,com.datastax.spark:spark-cassandra-connector_2.12:3.4.1 spark/speed_kafka_technicals.py
