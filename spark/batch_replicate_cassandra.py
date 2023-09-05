from pyspark.sql import SparkSession
from pyspark.sql.types import *
import pyspark.sql.functions as F
from dotenv import load_dotenv
from delta import configure_spark_with_delta_pip
from delta.tables import *
import logging
import json
import os

load_dotenv()
logging.basicConfig(level=logging.INFO)
aws_key = os.getenv('AWS_ACCESS_KEY')
aws_secret = os.getenv('AWS_ACCESS_KEY_SECRET')
symbols = json.loads(os.getenv('TICKERS'))
price_tables = [symbol.lower() for symbol in symbols]
price_tables.append("all_stocks")
technical_tables = ["ema", "sma", "macd", "rsi"]


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

def read_from_cassandra(spark, keyspace, table):
    return spark.read \
        .format("org.apache.spark.sql.cassandra") \
        .options(table=table, keyspace=keyspace) \
        .load()

def add_newest_timestamp(df):
    return df.withColumn("updated_at", F.current_timestamp())

def save_to_delta(spark, df, table_name, s3_path):
    delta_path = f"{s3_path}/{table_name}"
    df = df.repartition("timestamp", "symbol")
    logging.info(f"Saving {table_name} to {delta_path}")
    
    if DeltaTable.isDeltaTable(spark, delta_path):
        delta_table = DeltaTable.forPath(spark, delta_path)
        delta_table.alias("oldData").merge(
            df.alias("newData"),
            f"oldData.timestamp = newData.timestamp AND oldData.symbol = newData.symbol"
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
    configs = {
        "spark.cassandra.connection.host": "cassandra",
        "spark.cassandra.connection.port": "9042",
        "spark.cassandra.auth.username": "cassandra",
        "spark.cassandra.auth.password": "cassandra",
    }
    spark = init_aws_spark("ReplicateCassandraToDelta", configs)
    
    dfs_price = {table: read_from_cassandra(spark, "price", table) for table in price_tables}
    dfs_technicals = {table: read_from_cassandra(spark, "technicals", table) for table in technical_tables}

    for table in price_tables:
        dfs_price[table] = add_newest_timestamp(dfs_price[table])
    for table in technical_tables:
        dfs_technicals[table] = add_newest_timestamp(dfs_technicals[table])
        
    s3_path_price = "s3a://wonseokchoi-data-lake-project/lake/cassandra_replication/price"
    s3_path_technicals = "s3a://wonseokchoi-data-lake-project/lake/cassandra_replication/technicals"
    
    for table, df in dfs_price.items():
        save_to_delta(spark, df, table, s3_path_price)
    for table, df in dfs_technicals.items():
        save_to_delta(spark, df, table, s3_path_technicals)
    
    spark.stop()


# spark-submit \
#   --packages com.datastax.spark:spark-cassandra-connector_2.12:3.4.1,\
# io.delta:delta-core_2.12:2.4.0,\
# org.apache.hadoop:hadoop-aws:3.2.2 \
#   --conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension" \
#   --conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog" \
#   spark/batch_replicate_cassandra.py