from concurrent.futures import ThreadPoolExecutor
from pyspark.sql.types import *
from dotenv import load_dotenv
from delta.tables import *
import logging
import json
import os
from shared_functions import init_aws_spark, save_to_delta, read_from_cassandra, add_newest_timestamp, repartition_by_timestamp

load_dotenv()
logging.basicConfig(level=logging.INFO)
symbols = json.loads(os.getenv('TICKERS'))
price_tables = [symbol.lower() for symbol in symbols]
technical_tables = ["ema", "sma", "macd", "rsi"]

def process_price_table(table):
    df = read_from_cassandra(spark, "price", table)
    df = add_newest_timestamp(df)
    df = repartition_by_timestamp(df)
    save_to_delta(spark, df, table, s3_path_price)

def process_technical_table(table):
    df = read_from_cassandra(spark, "technicals", table)
    df = add_newest_timestamp(df)
    df = repartition_by_timestamp(df)
    save_to_delta(spark, df, table, s3_path_technicals)


if __name__ == "__main__":
    configs = {
        "spark.cassandra.connection.host": "localhost",
        "spark.cassandra.connection.port": "9042",
        "spark.cassandra.auth.username": "cassandra",
        "spark.cassandra.auth.password": "cassandra",
    }
    spark = init_aws_spark("ReplicateCassandraToDelta", configs)
    
    s3_path_price = "s3a://wonseokchoi-data-lake-project/lake/bronze/price"
    s3_path_technicals = "s3a://wonseokchoi-data-lake-project/lake/bronze/technicals"
    
    with ThreadPoolExecutor() as executor:
        executor.map(process_price_table, price_tables)
        executor.map(process_technical_table, technical_tables)
    
    spark.stop()


# spark-submit \
#   --driver-memory 6g \
#   --packages com.datastax.spark:spark-cassandra-connector_2.12:3.4.1,\
# io.delta:delta-core_2.12:2.4.0,\
# org.apache.hadoop:hadoop-aws:3.2.2 \
#   --conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension" \
#   --conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog" \
#   --conf "spark.memory.fraction=0.7" \
#   spark/batch_bronze_migrate.py