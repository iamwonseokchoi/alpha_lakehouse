spark-submit \
  --class "ReplicateCassandraToDelta" \
  --packages io.delta:delta-core_2.12:2.4.0,\
org.apache.hadoop:hadoop-aws:3.2.2 \
  --conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension" \
  --conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog" \
  target/scala-2.12/AlphaLakeHouse-assembly-0.1.jar
