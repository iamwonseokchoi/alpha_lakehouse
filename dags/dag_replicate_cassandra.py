from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator 
from datetime import datetime, timedelta
from dotenv import load_dotenv


default_args = {
    'owner': 'wonseok',
    'start_date': datetime.today(),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'catchup': False
}

dag = DAG(
    dag_id = 'BATCH_replicate_cassandra',
    default_args = default_args,
    schedule_interval = '@daily',
    tags = ['batch', 'cassandra', "delta_lake"],
    description = 'Replicates Cassandra tables to Delta Lake tables as batch'
)

batch_compile_spark = SparkSubmitOperator(
    application='/app/spark_scripts/batch_replicate_cassandra.py',
    conn_id='spark_cluster',
    task_id='spark_submit_task',
    packages='com.datastax.spark:spark-cassandra-connector_2.12:3.4.1,io.delta:delta-core_2.12:2.3.0,org.apache.hadoop:hadoop-aws:3.2.2',
    conf={
        'spark.sql.extensions': 'io.delta.sql.DeltaSparkSessionExtension',
        'spark.sql.catalog.spark_catalog': 'org.apache.spark.sql.delta.catalog.DeltaCatalog'
    },
    dag=dag
)

batch_compile_spark
