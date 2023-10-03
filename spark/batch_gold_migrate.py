from shared_functions import init_aws_spark
from datetime import datetime, timedelta
from pyspark.sql.window import Window
import pyspark.sql.functions as F
from pyspark.sql.types import *
from dotenv import load_dotenv
from delta.tables import *
import logging
import json
import os

load_dotenv()
logging.basicConfig(level=logging.INFO)
symbols = json.loads(os.getenv('TICKERS'))


def save_to_delta_gold(spark, df, table_name, s3_path):
    delta_path = f"{s3_path}/{table_name}"
    logging.info(f"Saving {table_name} to {delta_path}")
    
    if DeltaTable.isDeltaTable(spark, delta_path):
        delta_table = DeltaTable.forPath(spark, delta_path)
        
        merge_condition = "oldData.timestamp = newData.timestamp"
        
        delta_table.alias("oldData").merge(
            df.alias("newData"),
            merge_condition
        ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()

        delta_table.optimize().executeZOrderBy("timestamp")

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
        
        DeltaTable.forPath(spark, delta_path).optimize().executeZOrderBy("timestamp")


if __name__ == "__main__":
    configs = {
        "spark.cassandra.connection.host": "localhost",
        "spark.cassandra.connection.port": "9042",
        "spark.cassandra.auth.username": "cassandra",
        "spark.cassandra.auth.password": "cassandra",
    }
    spark = init_aws_spark("BronzeToSilverTransformation", configs)

    for symbol in symbols:
        table = symbol.lower()
        DF_news = spark.read.format("delta").load(f"s3a://wonseokchoi-data-lake-project/lake/silver/news/{table}/").orderBy(F.desc("published_utc"))
        DF_price = spark.read.format("delta").load(f"s3a://wonseokchoi-data-lake-project/lake/silver/price/{table}/").orderBy(F.desc("timestamp"))
        DF_ema = spark.read.format("delta").load(f"s3a://wonseokchoi-data-lake-project/lake/silver/technicals/ema/{table}/").orderBy(F.desc("timestamp"))
        DF_macd = spark.read.format("delta").load(f"s3a://wonseokchoi-data-lake-project/lake/silver/technicals/macd/{table}/").orderBy(F.desc("timestamp"))
        DF_rsi = spark.read.format("delta").load(f"s3a://wonseokchoi-data-lake-project/lake/silver/technicals/rsi/{table}/").orderBy(F.desc("timestamp"))
        DF_sma = spark.read.format("delta").load(f"s3a://wonseokchoi-data-lake-project/lake/silver/technicals/sma/{table}/").orderBy(F.desc("timestamp"))

        all_dataframes = [DF_price, DF_ema, DF_macd, DF_rsi, DF_sma, DF_news]
        timestamp_fields = ["timestamp", "timestamp", "timestamp", "timestamp", "timestamp", "published_utc"]

        for i, df in enumerate(all_dataframes):
            timestamp_field = timestamp_fields[i]
            df = df.withColumn(timestamp_field, F.to_timestamp(timestamp_field))

        windowSpec = F.window("timestamp", "1 hour")

        agg_exprs_price = [F.avg(c).alias(f"avg_{c}") for c in ["open", "high", "low", "close", "volume", "volume_weighted", "transactions"]]

        DF_price_hourly = DF_price.groupBy(windowSpec).agg(*agg_exprs_price)

        DF_ema_hourly = DF_ema.groupBy(windowSpec).agg(F.avg("value").alias("avg_ema"))
        DF_macd_hourly = DF_macd.groupBy(windowSpec).agg(F.avg("value").alias("avg_macd"))
        DF_rsi_hourly = DF_rsi.groupBy(windowSpec).agg(F.avg("value").alias("avg_rsi"))
        DF_sma_hourly = DF_sma.groupBy(windowSpec).agg(F.avg("value").alias("avg_sma"))

        DF_news_hourly = DF_news.groupBy(F.window("published_utc", "1 hour")).agg(F.collect_list("description").alias("news_descriptions"))

        DF_final = DF_price_hourly.join(DF_ema_hourly, ["window"], "left_outer") \
                                    .join(DF_macd_hourly, ["window"], "left_outer") \
                                    .join(DF_rsi_hourly, ["window"], "left_outer") \
                                    .join(DF_sma_hourly, ["window"], "left_outer") \
                                    .join(DF_news_hourly, ["window"], "left_outer")

        DF_final = DF_final.withColumn("timestamp", F.col("window").start).drop("window").orderBy(F.desc("timestamp"))

        windowSpec = Window.orderBy("timestamp")

        columns_to_fill = ["avg_ema", "avg_macd", "avg_rsi", "avg_sma", "avg_open", "avg_high", "avg_low", "avg_close", "avg_volume", "avg_volume_weighted", "avg_transactions"]

        for col in columns_to_fill:
            DF_final = DF_final.withColumn(col, F.last(col, ignorenulls=True).over(windowSpec))
            
        DF_final = DF_final.na.drop(subset=["avg_ema", "avg_macd", "avg_rsi", "avg_sma"])

        DF_final = DF_final.withColumn(
            "news_descriptions",
            F.when(
                F.col("news_descriptions").isNull(),
                F.array(F.lit("NoNews"))
            ).otherwise(F.col("news_descriptions"))
        )
        
        save_to_delta_gold(spark, DF_final, table, "s3a://wonseokchoi-data-lake-project/lake/gold/")
        
        date_last_year = (datetime.today() - timedelta(days=365)).strftime("%Y-%m-%d")
        
        DF_gold = spark.read.format("delta").load(f"s3a://wonseokchoi-data-lake-project/lake/gold/{table}/").filter(F.col("timestamp") > F.lit(date_last_year))
        
        DF_frontend = DF_gold.select("timestamp", "avg_close", "avg_volume", "avg_volume_weighted", "avg_transactions", "avg_ema", "avg_macd", "avg_rsi", "avg_sma") \
            .withColumnRenamed("avg_open", "open") \
            .withColumnRenamed("avg_high", "high") \
            .withColumnRenamed("avg_low", "low") \
            .withColumnRenamed("avg_close", "close") \
            .withColumnRenamed("avg_volume", "volume") \
            .withColumnRenamed("avg_volume_weighted", "volume_weighted") \
            .withColumnRenamed("avg_transactions", "transactions") \
            .withColumnRenamed("avg_ema", "ema") \
            .withColumnRenamed("avg_macd", "macd") \
            .withColumnRenamed("avg_rsi", "rsi") \
            .withColumnRenamed("avg_sma", "sma") \
            .withColumn("updated_at", F.current_timestamp()) \
            .orderBy(F.asc("timestamp")) 
    
        DF_frontend.write \
            .format("org.apache.spark.sql.cassandra")\
            .options(table=table, keyspace="cleaned")\
            .option("confirm.truncate", "true")\
            .mode("overwrite")\
            .save()


    spark.stop()


# spark-submit \
#   --driver-memory 7g \
#   --packages com.datastax.spark:spark-cassandra-connector_2.12:3.4.1,\
# io.delta:delta-core_2.12:2.4.0,\
# org.apache.hadoop:hadoop-aws:3.2.2 \
#   --conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension" \
#   --conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog" \
#   --conf "spark.memory.fraction=0.8" \
#   spark/batch_gold_migrate.py