from concurrent.futures import ThreadPoolExecutor
import pyspark.sql.functions as F
from delta.tables import *
import logging
import os
from dotenv import load_dotenv
import json
from shared_functions import init_aws_spark, save_to_delta

load_dotenv()
logging.basicConfig(level=logging.INFO)
symbols = json.loads(os.getenv('TICKERS'))
price_tables = [symbol.lower() for symbol in symbols]
technical_tables = ["ema", "sma", "macd", "rsi"]

def process_price_to_silver(table, source_path, target_path):
    df = (spark.read.format("delta")
            .load(f"{source_path}{table}")
            .withColumn("timestamp", F.date_format(F.from_unixtime(F.col("timestamp") / 1000), "yyyy-MM-dd HH:mm:ss"))
            .withColumn("updated_at", F.current_timestamp())
    )
    
    save_to_delta(spark, df, table, target_path)

def process_technicals_to_silver(table, source_path, target_path):
    df = (spark.read.format("delta")
            .load(f"{source_path}{table}")
            .withColumn("timestamp", F.date_format(F.from_unixtime(F.col("timestamp") / 1000), "yyyy-MM-dd HH:mm:ss"))
            .withColumn("updated_at", F.current_timestamp())
    )
    
    unique_symbols = df.select("symbol").distinct().rdd.flatMap(lambda x: x).collect()
    
    for symbol in unique_symbols:
        df_filtered = df.filter(F.col("symbol") == symbol)
        save_to_delta(spark, df_filtered, f"{table}/{symbol.lower()}", target_path)
    

if __name__ == "__main__":
    configs = {}
    spark = init_aws_spark("BronzeToSilverTransformation", configs)
    
    bronze_price_path = "s3a://wonseokchoi-data-lake-project/lake/bronze/price/"
    bronze_technicals_path = "s3a://wonseokchoi-data-lake-project/lake/bronze/technicals/"
    silver_price_path = "s3a://wonseokchoi-data-lake-project/lake/silver/price/"
    silver_technicals_path = "s3a://wonseokchoi-data-lake-project/lake/silver/technicals/"

    with ThreadPoolExecutor() as executor:
        futures_price = {executor.submit(process_price_to_silver, table, bronze_price_path, silver_price_path) for table in price_tables}
        futures_technicals = {executor.submit(process_technicals_to_silver, table, bronze_technicals_path, silver_technicals_path) for table in technical_tables}

    spark.stop()


# spark-submit \
#   --driver-memory 6g \
#   --packages com.datastax.spark:spark-cassandra-connector_2.12:3.4.1,\
# io.delta:delta-core_2.12:2.4.0,\
# org.apache.hadoop:hadoop-aws:3.2.2 \
#   --conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension" \
#   --conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog" \
#   --conf "spark.memory.fraction=0.7" \
#   spark/batch_silver_migrate.py