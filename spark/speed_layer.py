from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.types import ArrayType, StructType, StructField, StringType, DoubleType, LongType
from pyspark.sql.functions import current_timestamp, col, from_json, explode

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

schema_ws_list = ArrayType(
    StructType([
        StructField("ev", StringType(), True),
        StructField("sym", StringType(), False),
        StructField("v", LongType(), True),
        StructField("av", LongType(), True),
        StructField("op", DoubleType(), True),
        StructField("vw", DoubleType(), True),
        StructField("o", DoubleType(), True),
        StructField("c", DoubleType(), True),
        StructField("h", DoubleType(), True),
        StructField("l", DoubleType(), True),
        StructField("a", DoubleType(), True),
        StructField("z", LongType(), True),
        StructField("s", LongType(), False),
        StructField("e", LongType(), False)
    ])
)


# Kryo Serialization
conf = SparkConf().set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("SpeedLayer") \
    .config("spark.cassandra.connection.host", "localhost") \
    .config("spark.cassandra.connection.port", "9042") \
    .config("spark.cassandra.auth.username", "cassandra") \
    .config("spark.cassandra.auth.password", "cassandra") \
    .config("spark.driver.host", "localhost") \
    .getOrCreate()

spark.sparkContext.setLogLevel("INFO")

# Read
kafkaStreamDF = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "aapl,amzn,googl,msft,nvda,tsla,sma,ema,macd,rsi,websocket") \
    .option("failOnDataLoss", "false") \
    .option("mode", "DROPMALFORMED") \
    .load()

# ForeachBatch Logic
def process_batch(df, batch_id):
    if not df.isEmpty():  
        price_schemas = {
            'aapl': schema_prices,
            'amzn': schema_prices,
            'googl': schema_prices,
            'msft': schema_prices,
            'nvda': schema_prices,
            'tsla': schema_prices
        }
        technicals_schemas = {
            'sma': schema_stats,
            'ema': schema_stats,
            'macd': schema_macd,
            'rsi': schema_stats
        }
        websocket_schemas = {
            'ws': schema_ws_list
        }

        # For price data
        for topic, schema in price_schemas.items():
            filtered_df = df.filter(df["topic"] == topic)
            if not filtered_df.isEmpty():
                filtered_df = filtered_df \
                    .select(from_json(col("value").cast("string"), schema).alias("parsed_value")) \
                    .select("parsed_value.*") \
                    .withColumnRenamed("t", "timestamp") \
                    .withColumnRenamed("o", "open") \
                    .withColumnRenamed("h", "high") \
                    .withColumnRenamed("l", "low") \
                    .withColumnRenamed("c", "close") \
                    .withColumnRenamed("v", "volume") \
                    .withColumnRenamed("vw", "volume_weighted") \
                    .withColumnRenamed("n", "transactions") \
                    .withColumn("updated_at", current_timestamp())
                filtered_df.write \
                    .format("org.apache.spark.sql.cassandra") \
                    .mode("append") \
                    .options(table=topic, keyspace="price") \
                    .option("spark.cassandra.output.batch.size.rows", "auto") \
                    .option("spark.cassandra.output.consistency.level", "LOCAL_QUORUM") \
                    .save()

        # For technicals data
        for topic, schema in technicals_schemas.items():
            filtered_df = df.filter(df["topic"] == topic)
            if not filtered_df.isEmpty():
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
                    .option("spark.cassandra.output.batch.size.rows", "auto") \
                    .option("spark.cassandra.output.consistency.level", "LOCAL_QUORUM") \
                    .save()
        
        # For websocket data
        filtered_ws_df = df.filter(df["topic"] == "websocket")
        if not filtered_ws_df.isEmpty():
            filtered_ws_df = filtered_ws_df \
                .select(from_json(col("value").cast("string"), schema_ws_list).alias("parsed_value")) \
                .select(explode("parsed_value").alias("single_value")) \
                .select("single_value.*") \
                .withColumnRenamed("ev", "event") \
                .withColumnRenamed("sym", "symbol") \
                .withColumnRenamed("v", "volume") \
                .withColumnRenamed("av", "agg_volume") \
                .withColumnRenamed("op", "open_price") \
                .withColumnRenamed("vw", "volume_weighted") \
                .withColumnRenamed("o", "open") \
                .withColumnRenamed("c", "close") \
                .withColumnRenamed("h", "high") \
                .withColumnRenamed("l", "low") \
                .withColumnRenamed("a", "avg_price") \
                .withColumnRenamed("z", "total_trades") \
                .withColumnRenamed("s", "start_time") \
                .withColumnRenamed("e", "end_time") \
                .withColumn("updated_at", current_timestamp()) \
                .na.drop(subset=["symbol", "start_time", "end_time"])
            
            filtered_ws_df.show()

            filtered_ws_df.write \
                .format("org.apache.spark.sql.cassandra") \
                .mode("append") \
                .options(table="ws", keyspace="websocket") \
                .option("spark.cassandra.output.batch.size.rows", "auto") \
                .option("spark.cassandra.output.consistency.level", "LOCAL_QUORUM") \
                .save()

# Write
query = kafkaStreamDF \
    .writeStream \
    .foreachBatch(process_batch) \
    .outputMode("append") \
    .option("checkpointLocation", "./volumes/speed_layer_checkpoint") \
    .start()

query.awaitTermination()



# spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.0,com.datastax.spark:spark-cassandra-connector_2.12:3.4.1 spark/speed_layer.py