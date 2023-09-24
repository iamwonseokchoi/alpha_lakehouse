from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
import json
import os
from dotenv import load_dotenv

class StockDataAggregator:
    def __init__(self, tickers):
        self.tickers = tickers
        self.session = self.connect_to_cassandra()

    def connect_to_cassandra(self):
        auth_provider = PlainTextAuthProvider(
            username=os.getenv("CASSANDRA_USERNAME"),
            password=os.getenv("CASSANDRA_PASSWORD"))
        cluster = Cluster(["localhost"], auth_provider=auth_provider, port=9042)
        return cluster.connect()

    def read_from_cassandra(self, ticker):
        query = f"SELECT * FROM price.{ticker}"
        try:
            return self.session.execute(query)
        except Exception as e:
            print(f"Error reading from Cassandra for ticker {ticker}: ", e)
            return None

    def add_time_bucket(self, df):
        return df.withColumn("time_bucket", (F.floor(F.col("timestamp") / F.lit(24 * 3600))))

    def write_to_cassandra_in_batches(self, df, num_partitions=10):
        df_repartitioned = df.repartition(num_partitions)
        df_repartitioned.write.format("org.apache.spark.sql.cassandra") \
            .mode("append") \
            .options(table="all_stocks", keyspace="price") \
            .save()

    def aggregate_stock_data(self):
        spark = (SparkSession.builder
                .appName("PriceDataAggregator")
                .config("spark.cassandra.connection.host", "localhost")
                .config("spark.cassandra.connection.port", "9042")
                .config("spark.cassandra.auth.username", "cassandra")
                .config("spark.cassandra.auth.password", "cassandra")
                .getOrCreate())
        dfs = []
        for ticker in self.tickers:
            data = self.read_from_cassandra(ticker)
            if data is not None:
                df = spark.createDataFrame(data)
                df = self.add_time_bucket(df)
                dfs.append(df)
        aggregated_df = dfs[0]
        for df in dfs[1:]:
            aggregated_df = aggregated_df.union(df)
        aggregated_df = aggregated_df.withColumn("updated_at", F.current_timestamp())
        
        self.write_to_cassandra_in_batches(aggregated_df, num_partitions=10)
        
        spark.stop()

if __name__ == "__main__":
    load_dotenv()
    tickers = json.loads(os.getenv("TICKERS"))
    aggregator = StockDataAggregator(tickers)
    aggregator.aggregate_stock_data()



# spark-submit \
#   --packages com.datastax.spark:spark-cassandra-connector_2.12:3.4.1,\
#   spark/batch_compile_stream.py