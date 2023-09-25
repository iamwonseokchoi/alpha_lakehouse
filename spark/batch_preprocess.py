import os
import json
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from delta import configure_spark_with_delta_pip
from delta.tables import *
import logging


load_dotenv()
logging.basicConfig(level=logging.INFO)
tickers = json.loads(os.getenv("TICKERS"))
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

def clean_and_delta(df, tickers):
    df1 = df.withColumn("timestamp", F.date_format(F.from_unixtime(F.col("timestamp")/1000), "yyyy-MM-dd HH:mm:ss")) \
            .drop("updated_at", "time_bucket")

    for ticker in tickers:
        df_x = df1.filter(F.col("symbol") == ticker).orderBy(F.col("timestamp").asc())

        path = f"s3a://wonseokchoi-data-lake-project/lake/cleaned/price_cleaned/{ticker}/"
        df_x.write.format("delta").mode("overwrite").save(path)
        
        deltaTable = DeltaTable.forPath(spark, path)
        deltaTable.optimize()

        deltaTable.toDF().show()
        print(f"**Optimized {ticker} Count: {deltaTable.toDF().count()}")

    return


if __name__ == "__main__":
    configs = {}
    spark = init_aws_spark("Clean Prices Data", configs)

    spark.sql("optimize 's3a://wonseokchoi-data-lake-project/lake/cassandra_replication/price/all_stocks/'")

    df = spark.read.format("delta").load("s3a://wonseokchoi-data-lake-project/lake/cassandra_replication/price/all_stocks/")
    clean_and_delta(df, tickers)


# spark-submit \
#   --packages io.delta:delta-core_2.12:2.4.0,\
# org.apache.hadoop:hadoop-aws:3.2.2 \
#   --conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension" \
#   --conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog" \
#   spark/batch_preprocess.py