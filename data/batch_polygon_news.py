from datetime import datetime, timedelta
import os
from dotenv import load_dotenv
import json
import logging
import requests
from urllib.parse import urlencode

load_dotenv()

tickers = json.loads(os.getenv("TICKERS"))
api_key = os.getenv("POLYGON_API_KEY")

class PolygonAPI:
    BASE_URL = "https://api.polygon.io/v2/"

    def __init__(self, api_key=None):
        self.api_key = api_key or os.getenv('POLYGON_API_KEY')
        self.session = requests.Session()

    def _get(self, endpoint, params=None):
        if params is None:
            params = {}
        params["apiKey"] = self.api_key
        url = f"{self.BASE_URL}{endpoint}"
        try:
            response = self.session.get(url, params=params)
            response.raise_for_status()
            return response.json()
        except requests.RequestException as e:
            logging.error(f"Failed to fetch data: {e}")
            return None

    def get_all_news(self, ticker, filters=None):
        all_news = []
        filters = filters or {}
        filters['ticker'] = ticker
        next_url = None

        while True:
            response_data = self._get("reference/news", params=filters) if not next_url else self._get(next_url.replace(self.BASE_URL, ""))
            if response_data:
                all_news.extend(response_data.get('results', []))
                next_url = response_data.get('next_url')
                if not next_url:
                    break
            else:
                logging.error("Failed to retrieve news.")
                break

        return all_news


# from data.news_data import PolygonAPI
# from pyspark.sql import SparkSession
# from pyspark.sql.types import *
# import pyspark.sql.functions as F
# from dotenv import load_dotenv
# from delta.tables import DeltaTable
# from delta import *
# import logging
# import json
# import os


# # Initialize
# load_dotenv()
# logging.basicConfig(level=logging.INFO)
# aws_key = os.getenv('AWS_ACCESS_KEY')
# aws_secret = os.getenv('AWS_ACCESS_KEY_SECRET')
# symbols = json.loads(os.getenv('TICKERS'))
# api_client = PolygonAPI()
# spark = SparkSession.builder \
#     .appName("KafkaNewsToLake") \
#     .config("spark.hadoop.fs.s3a.access.key", aws_key) \
#     .config("spark.hadoop.fs.s3a.secret.key", aws_secret) \
#     .config("spark.hadoop.fs.s3a.endpoint", "s3.ap-northeast-2.amazonaws.com") \
#     .config("spark.sql.sources.partitionOverwriteMode", "dynamic") \
#     .config("spark.databricks.delta.schema.autoMerge.enabled", "true") \
#     .config('spark.hadoop.fs.s3a.aws.credentials.provider', 'org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider')
# spark.sparkContext.setLogLevel("ERROR")

# # Define API filters for Polygon class 
# filters = {
#     'published_utc.gt': '2023-08-30',
#     'order': 'desc',
#     'limit': 100,
#     'sort': 'published_utc'
# }


# # Fetch all news for TSLA
# all_news_data = api_client.get_all_news('TSLA', filters=filters)

# if all_news_data:
#     logging.info(f"Successfully retrieved {len(all_news_data)} news items.")
#     print(all_news_data)
# else:
#     logging.error("Failed to retrieve news.")