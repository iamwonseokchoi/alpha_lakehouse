from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from delta import configure_spark_with_delta_pip
from dotenv import load_dotenv
from delta.tables import *
import logging
import os

load_dotenv()
aws_key = os.getenv('AWS_ACCESS_KEY')
aws_secret = os.getenv('AWS_ACCESS_KEY_SECRET')


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
    logging.info(f"Saving {table_name} to {delta_path}")
    
    if DeltaTable.isDeltaTable(spark, delta_path):
        delta_table = DeltaTable.forPath(spark, delta_path)
        
        merge_condition = "oldData.timestamp = newData.timestamp AND oldData.symbol = newData.symbol"
        
        delta_table.alias("oldData").merge(
            df.alias("newData"),
            merge_condition
        ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()

        delta_table.optimize().executeZOrderBy("timestamp", "symbol")

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
        
        DeltaTable.forPath(spark, delta_path).optimize().executeZOrderBy("timestamp", "symbol")


def save_to_delta_news(spark, df, table_name, s3_path):
    delta_path = f"{s3_path}/{table_name}"
    logging.info(f"Saving {table_name} to {delta_path}")
    
    if DeltaTable.isDeltaTable(spark, delta_path):
        delta_table = DeltaTable.forPath(spark, delta_path)
        
        merge_condition = "oldData.id = newData.id AND oldData.updated_at = newData.updated_at"
        
        delta_table.alias("oldData").merge(
            df.alias("newData"),
            merge_condition
        ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()

        delta_table.optimize().executeZOrderBy("published_utc")

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
        
        DeltaTable.forPath(spark, delta_path).optimize().executeZOrderBy("published_utc")


def read_from_cassandra(spark, keyspace, table):
    return spark.read \
        .format("org.apache.spark.sql.cassandra") \
        .options(table=table, keyspace=keyspace) \
        .load()


def add_newest_timestamp(df):
    return df.withColumn("updated_at", F.current_timestamp())


def repartition_by_timestamp(df):
    return df.repartition("timestamp", "symbol")