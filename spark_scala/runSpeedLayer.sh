spark-submit \
  --class "SpeedLayer" \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1,com.datastax.spark:spark-cassandra-connector_2.12:3.4.1 \
  target/scala-2.12/SparkStreamPriceTechnicals_2.12-0.1.jar