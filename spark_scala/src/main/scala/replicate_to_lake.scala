import org.apache.spark.sql.{DeltaTable, SparkSession}
import org.apache.spark.sql.functions.current_timestamp
import com.typesafe.config.ConfigFactory
import org.slf4j.LoggerFactory

object ReplicateCassandraToDelta {
  val logger = LoggerFactory.getLogger(this.getClass)

  def main(args: Array[String]): Unit = {
    val config = ConfigFactory.load()
    val awsKey = sys.env("AWS_ACCESS_KEY")
    val awsSecret = sys.env("AWS_ACCESS_KEY_SECRET")
    val symbols = sys.env("TICKERS").split(",").map(_.trim)
    val priceTables = symbols.map(_.toLowerCase) :+ "all_stocks"
    val technicalTables = Array("ema", "sma", "macd", "rsi")

    val sparkConfigs = Map(
      "spark.cassandra.connection.host" -> "cassandra",
      "spark.cassandra.connection.port" -> "9042",
      "spark.cassandra.auth.username" -> "cassandra",
      "spark.cassandra.auth.password" -> "cassandra"
    )

    val spark = initAWSSpark("ReplicateCassandraToDelta", sparkConfigs, awsKey, awsSecret)

    val dfsPrice = priceTables.map(table => (table, readFromCassandra(spark, "price", table))).toMap
    val dfsTechnicals = technicalTables.map(table => (table, readFromCassandra(spark, "technicals", table))).toMap

    val s3PathPrice = "s3a://wonseokchoi-data-lake-project/lake/cassandra_replication/price"
    val s3PathTechnicals = "s3a://wonseokchoi-data-lake-project/lake/cassandra_replication/technicals"

    dfsPrice.foreach { case (table, df) =>
      saveToDelta(spark, addNewestTimestamp(df), table, s3PathPrice)
    }

    dfsTechnicals.foreach { case (table, df) =>
      saveToDelta(spark, addNewestTimestamp(df), table, s3PathTechnicals)
    }

    spark.stop()
  }

  def initAWSSpark(appName: String, configs: Map[String, String], awsKey: String, awsSecret: String): SparkSession = {
    var builder = SparkSession.builder().appName(appName)
    configs.foreach { case (key, value) =>
      builder = builder.config(key, value)
    }

    builder.config("spark.hadoop.fs.s3a.access.key", awsKey)
      .config("spark.hadoop.fs.s3a.secret.key", awsSecret)
      .config("spark.hadoop.fs.s3a.endpoint", "s3.ap-northeast-2.amazonaws.com")
      .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
      .config("spark.databricks.delta.schema.autoMerge.enabled", "true")
      .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
      .getOrCreate()
  }

  def readFromCassandra(spark: SparkSession, keyspace: String, table: String) = {
    spark.read
      .format("org.apache.spark.sql.cassandra")
      .option("keyspace", keyspace)
      .option("table", table)
      .load()
  }

  def addNewestTimestamp(df: DataFrame) = {
    df.withColumn("updated_at", current_timestamp())
  }

  def saveToDelta(spark: SparkSession, df: DataFrame, tableName: String, s3Path: String): Unit = {
    val deltaPath = s"$s3Path/$tableName"
    df.repartition(col("timestamp"), col("symbol"))
    logger.info(s"Saving $tableName to $deltaPath")

    if (DeltaTable.isDeltaTable(deltaPath)) {
      val deltaTable = DeltaTable.forPath(deltaPath)
      deltaTable.alias("oldData").merge(
        df.alias("newData"),
        "oldData.timestamp = newData.timestamp AND oldData.symbol = newData.symbol"
      )
      .whenMatched()
      .updateAll()
      .whenNotMatched()
      .insertAll()
      .execute()
    } else {
      logger.info(s"Creating new Delta table for $tableName")
      df.write
        .format("delta")
        .mode("overwrite")
        .option("mergeSchema", "true")
        .save(deltaPath)
    }
  }
}
