#!/bin/bash

# Function to submit a Spark job and exit if it fails.
run_spark_submit() {
    spark-submit "$@"
    if [ $? -ne 0 ]; then
        echo "Spark job failed. Exiting."
        exit 1
    fi
}

# Run batch_bronze_news.py
run_spark_submit \
  --driver-memory 6g \
  --packages io.delta:delta-core_2.12:2.4.0,org.apache.hadoop:hadoop-aws:3.2.2 \
  --conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension" \
  --conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog" \
  --conf "spark.memory.fraction=0.7" \
  spark/batch_bronze_news.py

# Run batch_silver_news.py
run_spark_submit \
  --driver-memory 6g \
  --packages io.delta:delta-core_2.12:2.4.0,org.apache.hadoop:hadoop-aws:3.2.2 \
  --conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension" \
  --conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog" \
  --conf "spark.memory.fraction=0.7" \
  spark/batch_silver_news.py

# Run batch_bronze_migrate.py
run_spark_submit \
  --driver-memory 6g \
  --packages com.datastax.spark:spark-cassandra-connector_2.12:3.4.1,io.delta:delta-core_2.12:2.4.0,org.apache.hadoop:hadoop-aws:3.2.2 \
  --conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension" \
  --conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog" \
  --conf "spark.memory.fraction=0.7" \
  spark/batch_bronze_migrate.py

# Run batch_silver_migrate.py
run_spark_submit \
  --driver-memory 6g \
  --packages com.datastax.spark:spark-cassandra-connector_2.12:3.4.1,io.delta:delta-core_2.12:2.4.0,org.apache.hadoop:hadoop-aws:3.2.2 \
  --conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension" \
  --conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog" \
  --conf "spark.memory.fraction=0.7" \
  spark/batch_silver_migrate.py

# Run batch_gold_migrate.py
run_spark_submit \
  --driver-memory 7g \
  --packages com.datastax.spark:spark-cassandra-connector_2.12:3.4.1,io.delta:delta-core_2.12:2.4.0,org.apache.hadoop:hadoop-aws:3.2.2 \
  --conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension" \
  --conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog" \
  --conf "spark.memory.fraction=0.8" \
  spark/batch_gold_migrate.py

echo "All Spark jobs completed successfully."
