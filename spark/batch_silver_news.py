import os
import json
from dotenv import load_dotenv
from concurrent.futures import ThreadPoolExecutor
from pyspark.sql import functions as F
from delta.tables import *
import logging
from shared_functions import init_aws_spark, save_to_delta_news

load_dotenv()
logging.basicConfig(level=logging.INFO)
tickers = json.loads(os.getenv("TICKERS"))


def batch_news_silver(df, ticker):
    df_filtered = df.filter(F.array_contains(df.tickers, ticker))
    return df_filtered

def process_ticker(ticker):
    df_silver = batch_news_silver(df_bronze, ticker)
    df_silver = df_silver.withColumn("updated_at", F.current_timestamp())
    save_to_delta_news(spark, df_silver, ticker.lower(), f"s3a://wonseokchoi-data-lake-project/lake/silver/news/")


if __name__ == "__main__":
    configs = {}
    spark = init_aws_spark("NewsSilver", configs)
    
    bronze_path = "s3a://wonseokchoi-data-lake-project/lake/bronze/news/"
    df_bronze = spark.read.format("delta").load(bronze_path)
    
    with ThreadPoolExecutor() as executor:
        executor.map(process_ticker, tickers)

    spark.stop()


# spark-submit \
#   --driver-memory 6g \
#   --packages io.delta:delta-core_2.12:2.4.0,\
# org.apache.hadoop:hadoop-aws:3.2.2 \
#   --conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension" \
#   --conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog" \
#   --conf "spark.memory.fraction=0.7" \
#   spark/batch_silver_news.py
