from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator 
from datetime import datetime, timedelta
import os
from dotenv import load_dotenv

load_dotenv()


default_args = {
    'owner': 'wonseok',
    'start_date': datetime.today(),
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
    'catchup': False
}

dag = DAG(
    dag_id = 'BATCH_get_historical_daily_price',
    default_args = default_args,
    schedule_interval = '@daily',
    tags = ['batch', 'delta_lake'],
    description = 'Get historical daily data with CDC from yfinance'
)

batch_to_data_lake = SparkSubmitOperator(
    application='/app/spark_scripts/batch_historical_daily.py',
    conn_id='spark_cluster',
    task_id='batch_to_data_lake_task',
    packages='io.delta:delta-core_2.12:2.3.0,org.apache.hadoop:hadoop-aws:3.2.2',
    conf={
        'spark.sql.extensions': 'io.delta.sql.DeltaSparkSessionExtension',
        'spark.sql.catalog.spark_catalog': 'org.apache.spark.sql.delta.catalog.DeltaCatalog'
    },
    dag=dag
)

batch_to_data_lake
