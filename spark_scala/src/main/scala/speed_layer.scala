import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.cassandra._
import org.apache.spark.sql.types.{StructType, StructField, StringType, DoubleType, LongType}

object SpeedLayer {
  def main(args: Array[String]): Unit = {
    val schemaPrices: StructType = new StructType()
      .add("t", LongType, false)
      .add("o", DoubleType, true)
      .add("h", DoubleType, true)
      .add("l", DoubleType, true)
      .add("c", DoubleType, true)
      .add("v", LongType, true)
      .add("vw", DoubleType, true)
      .add("n", LongType, true)
      .add("symbol", StringType, false)

    val schemaStats: StructType = new StructType()
      .add("timestamp", LongType, false)
      .add("value", DoubleType, true)
      .add("symbol", StringType, false)

    val schemaMacd: StructType = new StructType()
      .add("timestamp", LongType, false)
      .add("value", DoubleType, true)
      .add("signal", DoubleType, true)
      .add("histogram", DoubleType, true)
      .add("symbol", StringType, false)

    val spark = SparkSession.builder()
      .appName("SpeedLayer")
      .config("spark.cassandra.connection.host", "localhost")
      .config("spark.cassandra.connection.port", "9042")
      .config("spark.cassandra.auth.username", "cassandra")
      .config("spark.cassandra.auth.password", "cassandra")
      .config("spark.driver.host", "localhost")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") 
      .config("spark.kryo.registrationRequired", "true")
      .config("spark.kryo.classesToRegister", 
              "org.apache.spark.sql.cassandra.CassandraSourceRelation")
      .getOrCreate()
    spark.sparkContext.setLogLevel("DEBUG")

    val kafkaStreamDF = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "aapl,amzn,googl,msft,nvda,tsla,sma,ema,macd,rsi")
      .option("failOnDataLoss", "false")
      .load()

    def processBatch(df: DataFrame, batchId: Long): Unit = {
      if (!df.isEmpty) {
        val priceTopics = Seq("aapl", "amzn", "googl", "msft", "nvda", "tsla")
        val technicalTopics = Seq("sma", "ema", "macd", "rsi")

        for (topic <- priceTopics) {
          val filteredDF = df.filter(col("topic") === topic)
          if (!filteredDF.isEmpty) {
            val parsedDF = filteredDF.select(
              from_json(col("value").cast("string"), schemaPrices).alias("parsed_value")
            ).select("parsed_value.*")
              .withColumnRenamed("t", "timestamp")
              .withColumnRenamed("o", "open")
              .withColumnRenamed("h", "high")
              .withColumnRenamed("l", "low")
              .withColumnRenamed("c", "close")
              .withColumnRenamed("v", "volume")
              .withColumnRenamed("vw", "volume_weighted")
              .withColumnRenamed("n", "transactions")
              .withColumn("updated_at", current_timestamp())
            parsedDF.write
              .cassandraFormat(topic, "price")
              .mode("append")
              .save()
          }
        }

        for (topic <- technicalTopics) {
          val filteredDF = df.filter(col("topic") === topic)
          if (!filteredDF.isEmpty) {
            val schema = if (topic == "macd") schemaMacd else schemaStats
            val parsedDF = filteredDF.select(
              from_json(col("value").cast("string"), schema).alias("parsed_value")
            ).select("parsed_value.*")
              .withColumn("updated_at", current_timestamp())
            parsedDF.write
              .cassandraFormat(topic, "technicals")
              .mode("append")
              .save()
          }
        }
      }
    }

    val query = kafkaStreamDF.writeStream
      .foreachBatch((df: DataFrame, batchId: Long) => processBatch(df, batchId))
      .outputMode("append")
      .option("checkpointLocation", "./volumes/speed_layer_checkpoint")
      .start()

    query.awaitTermination()
  }
}


// spark-submit \
//   --class "SpeedLayer" \
//   --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1,com.datastax.spark:spark-cassandra-connector_2.12:3.4.1 \
//   target/scala-2.12/SparkStreamPriceTechnicals_2.12-0.1.jar