from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from delta import *
from delta.tables import DeltaTable
from dotenv import load_dotenv
import yfinance as yf
import json 
import os

load_dotenv()
aws_key = os.getenv('AWS_ACCESS_KEY')
aws_secret = os.getenv('AWS_ACCESS_KEY_SECRET')
symbols = json.loads(os.getenv('TICKERS'))


def init_spark_session(app_name, configs):
    builder = SparkSession.builder.appName(app_name)
    for key, value in configs.items():
        builder = builder.config(key, value)

    builder.config("spark.hadoop.fs.s3a.access.key", aws_key)
    builder.config("spark.hadoop.fs.s3a.secret.key", aws_secret)
    builder.config("spark.hadoop.fs.s3a.endpoint", "s3.ap-northeast-2.amazonaws.com")
    builder.config("spark.sql.sources.partitionOverwriteMode", "dynamic")
    builder.config("spark.databricks.delta.schema.autoMerge.enabled", "true")
    builder.config('spark.hadoop.fs.s3a.aws.credentials.provider', 'org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider')   

    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    return spark

def fetch_stock_data(ticker):
    stock_data = yf.download(ticker, period='max')
    stock_data.reset_index(inplace=True)
    stock_data.columns = [
        col.replace(" ", "_").replace(".", "_").replace("-", "_").lower() for col in stock_data.columns
        ]
    stock_data['symbol'] = ticker
    return stock_data

def save_to_delta(spark, df, s3_path):
    delta_path = f"{s3_path}"
    df = df.repartition("timestamp", "symbol")
    
    if DeltaTable.isDeltaTable(spark, delta_path):
        delta_table = DeltaTable.forPath(spark, delta_path)
        delta_table.alias("t").merge(
            df.alias("s"),
            "t.timestamp = s.timestamp AND t.symbol = s.symbol"
        ).whenMatchedUpdateAll(
        ).whenNotMatchedInsertAll(
        ).execute()
    else:
        df.write \
            .format("delta") \
            .mode("overwrite") \
            .option("mergeSchema", "true") \
            .save(delta_path)

if __name__ == "__main__":
    configs = {
        
    }
    spark = init_spark_session("Historical_Stock_Data", configs)

    for symbol in symbols:
        raw_stock_data = fetch_stock_data(symbol)
        sanitized_column_names = [col.replace(" ", "_").replace(".", "_").replace("-", "_") for col in raw_stock_data.columns]
        raw_stock_data.columns = sanitized_column_names
        df = spark.createDataFrame(raw_stock_data)
        df = df \
            .withColumnRenamed('Date', 'timestamp') \
            .withColumn('timestamp', F.col('timestamp').cast('Timestamp')) \
            .withColumn('updated_at', F.current_timestamp())

        s3_path = f"s3a://wonseokchoi-data-lake-project/lake/historical_daily/{symbol}"
        
        save_to_delta(spark, df, s3_path)
        
    spark.stop()
        
        
# For 3.4.x latest Spark:
# spark-submit \
#   --packages io.delta:delta-core_2.12:2.4.0,\
# org.apache.hadoop:hadoop-aws:3.2.2 \
#   --conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension" \
#   --conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog" \
#   spark/batch_historical_daily.py

## For 3.3.0:
# spark-submit \
#   --packages io.delta:delta-core_2.12:2.3.0,\
# org.apache.hadoop:hadoop-aws:3.2.2 \
#   --conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension" \
#   --conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog" \
#   spark/batch_historical_daily.py