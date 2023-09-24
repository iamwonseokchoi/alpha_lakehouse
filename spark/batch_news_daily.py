import os
import json
import requests
from datetime import datetime, timedelta
from dotenv import load_dotenv
from concurrent.futures import ThreadPoolExecutor
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, ArrayType
from delta import configure_spark_with_delta_pip
from delta.tables import *
import logging


load_dotenv()
logging.basicConfig(level=logging.INFO)
tickers = json.loads(os.getenv("TICKERS"))
api_key = os.getenv("POLYGON_API_KEY")
start_date = "2015-01-01"
aws_key = os.getenv('AWS_ACCESS_KEY')
aws_secret = os.getenv('AWS_ACCESS_KEY_SECRET')


class PolygonNewsAPI:
    def __init__(self, tickers, start_date, api_key):
        self.base_url = "https://api.polygon.io/v2/reference/news"
        self.tickers = tickers
        self.start_date = datetime.strptime(start_date, "%Y-%m-%d")
        self.end_date = datetime.today()
        self.api_key = api_key
        self.news_data = []
        self.seen_ids = set()

    def fetch_news(self, ticker, from_date, to_date):
        params = {
            "ticker": ticker,
            "published_utc.gte": from_date,
            "published_utc.lte": to_date,
            "order": "asc",
            "limit": 1000,
            "sort": "published_utc",
            "apiKey": self.api_key
        }

        response = requests.get(self.base_url, params=params)
        if response.status_code == 200:
            json_data = response.json()
            for result in json_data["results"]:
                article_id = result.get("id")
                if article_id not in self.seen_ids:
                    self.news_data.append(result)
                    self.seen_ids.add(article_id)
                    
            next_url = json_data.get("next_url")
            while next_url:
                next_response = requests.get(next_url)
                if next_response.status_code == 200:
                    next_json_data = next_response.json()
                    for result in next_json_data["results"]:
                        article_id = result.get("id")
                        if article_id not in self.seen_ids:
                            self.news_data.append(result)
                            self.seen_ids.add(article_id)
                    next_url = next_json_data.get("next_url")
                else:
                    print(f"Failed to fetch next_url data for {ticker}, status code: {next_response.status_code}")
                    break
        else:
            print(f"Failed to fetch news data for {ticker}, status code: {response.status_code}")

    def fetch_for_ticker(self, ticker):
        curr_date = self.start_date
        delta = timedelta(days=7)

        while curr_date <= self.end_date:
            from_date = curr_date.strftime("%Y-%m-%d")
            to_date = (curr_date + delta).strftime("%Y-%m-%d")
            self.fetch_news(ticker, from_date, to_date)
            curr_date += delta + timedelta(days=1)

    def fetch_all_tickers(self):
        with ThreadPoolExecutor() as executor:
            executor.map(self.fetch_for_ticker, self.tickers)

def init_aws_spark(app_name, configs):
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
    return spark

def save_to_delta(spark, df, table_name, s3_path):
    delta_path = f"{s3_path}/{table_name}"
    df = df.coalesce(1)
    logging.info(f"Saving {table_name} to {delta_path}")
    
    if DeltaTable.isDeltaTable(spark, delta_path):
        delta_table = DeltaTable.forPath(spark, delta_path)
        delta_table.alias("oldData").merge(
            df.alias("newData"),
            f"oldData.id = newData.id"
        ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()

        history = delta_table.history(1).select("operation", "operationParameters", "timestamp").collect()
        for row in history:
            logging.info(f"{row.operation} on {table_name} at {row.timestamp}")
            if row["operation"] == "MERGE":
                logging.info(f"Merged with parameters: {row['operationParameters']}")
    else:
        logging.info(f"Creating new Delta table for {table_name}")
        df.write \
            .format("delta") \
            .mode("overwrite") \
            .option("mergeSchema", "true") \
            .save(delta_path)


if __name__ == "__main__":
    polygon_api = PolygonNewsAPI(tickers, start_date, api_key)
    polygon_api.fetch_all_tickers()
    
    configs = {}
    spark = init_aws_spark("Polygon News", configs)

    schema = StructType([
        StructField("id", StringType(), True),
        StructField("amp_url", StringType(), True),
        StructField("article_url", StringType(), True),
        StructField("author", StringType(), True),
        StructField("description", StringType(), True),
        StructField("image_url", StringType(), True),
        StructField("keywords", ArrayType(StringType()), True),
        StructField("published_utc", StringType(), True),
        StructField("publisher", StringType(), True),
        StructField("tickers", ArrayType(StringType()), True),
        StructField("title", StringType(), True),
    ])

    news_df = spark.createDataFrame(polygon_api.news_data, schema=schema)
    news_df_with_timestamp = news_df.withColumn("updated_at", F.current_timestamp())

    s3_path = "s3a://wonseokchoi-data-lake-project/lake/cleaned/news_batch/"
    save_to_delta(spark, news_df_with_timestamp, "news", s3_path)


# spark-submit \
#   --packages io.delta:delta-core_2.12:2.4.0,\
# org.apache.hadoop:hadoop-aws:3.2.2 \
#   --conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension" \
#   --conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog" \
#   spark/batch_news_daily.py