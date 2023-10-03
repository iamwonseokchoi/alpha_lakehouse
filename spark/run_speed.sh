#!/bin/bash

run_spark_submit() {
    spark-submit "$@"
    if [ $? -ne 0 ]; then
        echo "Spark job failed. Exiting."
        exit 1
    fi
}

spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.0,com.datastax.spark:spark-cassandra-connector_2.12:3.4.1 spark/speed_layer.py