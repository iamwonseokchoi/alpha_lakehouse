from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator 
from datetime import datetime, timedelta


default_args = {
    'owner': 'wonseok',
    'start_date': datetime.today(),
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
    'catchup': False
}

dag = DAG(
    dag_id = 'BATCH_daily_news',
    default_args = default_args,
    schedule_interval='0 15 * * *',
    tags = ['batch', 'delta_lake', 'bronze', 'silver'],
    description = 'Daily news data on tickers to Delta Lake tables as batch'
)

batch_news_bronze = SparkSubmitOperator(
    application='/app/spark_scripts/batch_bronze_news.py',
    conn_id='spark_cluster',
    task_id='batch_bronze_news',
    packages='io.delta:delta-core_2.12:2.3.0,org.apache.hadoop:hadoop-aws:3.2.2',
    conf={
        'spark.driver.memory': '4g',
        'spark.sql.extensions': 'io.delta.sql.DeltaSparkSessionExtension',
        'spark.sql.catalog.spark_catalog': 'org.apache.spark.sql.delta.catalog.DeltaCatalog'
    },
    dag=dag
)

batch_news_silver = SparkSubmitOperator(
    application='/app/spark_scripts/batch_silver_news.py',
    conn_id='spark_cluster',
    task_id='batch_silver_news',
    packages='io.delta:delta-core_2.12:2.3.0,org.apache.hadoop:hadoop-aws:3.2.2',
    conf={
        'spark.driver.memory': '6g',
        'spark.memory.fraction': '0.7',
        'spark.sql.extensions': 'io.delta.sql.DeltaSparkSessionExtension',
        'spark.sql.catalog.spark_catalog': 'org.apache.spark.sql.delta.catalog.DeltaCatalog'
    },
    dag=dag
)

batch_news_bronze >> batch_news_silver
