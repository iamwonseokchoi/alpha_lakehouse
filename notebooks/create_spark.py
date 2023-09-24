from pyspark.sql import SparkSession
import os

class CreateSparkSession:
    def __init__(self):
        self.configure_environment()
        self.session = self.create_spark_session()

    def configure_environment(self):
        os.environ["JAVA_HOME"] = "/Users/wonseokchoi/Documents/Installs/jdk-11.0.19.jdk/Contents/Home"
        os.environ["PATH"] = f'{os.environ["JAVA_HOME"]}/bin:{os.environ["PATH"]}'
        os.environ["SPARK_HOME"] = "/Users/wonseokchoi/Documents/Installs/spark-3.4.1-bin-hadoop3"
        os.environ["PATH"] = f'{os.environ["PATH"]}:{os.environ["SPARK_HOME"]}/bin'
        spark_home = os.environ.get('SPARK_HOME')
        if spark_home:
            zips = ":".join(os.path.join(spark_home, "python/lib", f) for f in os.listdir(os.path.join(spark_home, "python/lib")) if f.endswith('.zip'))
            python_path = os.environ.get("PYTHONPATH", "")
            new_python_path = f"{zips}:{python_path}"
            os.environ["PYTHONPATH"] = new_python_path
        else:
            print("SPARK_HOME is not set in the environment.")
        os.environ["HADOOP_HOME"] = "/Users/wonseokchoi/Documents/Installs/hadoop-3.3.6"
        os.environ["PATH"] = f'{os.environ["PATH"]}:{os.environ["HADOOP_HOME"]}/bin'
        os.environ["HADOOP_CONF_DIR"] = f'{os.environ["HADOOP_HOME"]}/etc/hadoop'
        os.environ["YARN_CONF_DIR"] = f'{os.environ["HADOOP_HOME"]}/etc/hadoop'
        os.environ["HADOOP_CLASSPATH"] = f'{os.environ["HADOOP_HOME"]}/share/hadoop/tools/lib/*'
        os.environ["PYSPARK_SUBMIT_ARGS"] = "--master local[*] pyspark-shell"

    def create_spark_session(self):
        spark_jars_packages = "com.amazonaws:aws-java-sdk:1.11.563,org.apache.hadoop:hadoop-aws:3.2.2,io.delta:delta-core_2.12:2.4.0"
        spark = (
            SparkSession.builder.master("local[*]")
            .appName("PySparkLocal")
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
            .config("spark.hadoop.fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
            .config("spark.hadoop.fs.AbstractFileSystem.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
            .config("spark.delta.logStore.class", "org.apache.spark.sql.delta.storage.S3SingleDriverLogStore")
            .config("spark.hadoop.fs.s3a.connection.timeout", "3600000")
            .config("spark.hadoop.fs.s3a.connection.maximum", "1000")
            .config("spark.hadoop.fs.s3a.threads.max", "1000")
            .config("spark.jars.packages", spark_jars_packages)
            .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
            .config("spark.databricks.delta.schema.autoMerge.enabled", "true")
            .config("spark.hadoop.fs.s3a.endpoint", "s3.ap-northeast-2.amazonaws.com")
            .config("spark.hadoop.fs.s3a.aws.credentials.provider", "com.amazonaws.auth.DefaultAWSCredentialsProviderChain")
            .getOrCreate()
        )
        spark.sparkContext.setLogLevel("ERROR")

        return spark