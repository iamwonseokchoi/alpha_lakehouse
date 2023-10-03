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
    dag_id='BATCH_pipeline',
    default_args=default_args,
    schedule_interval='0 17 * * *',
    tags=['batch', 'delta_lake', 'bronze', 'silver'],
    description='Daily replication of CassandraDB to Delta Lakehouse'
)

common_spark_conf = {
    'spark.driver.memory': '7g',
    'spark.memory.fraction': '0.8',
    'spark.sql.extensions': 'io.delta.sql.DeltaSparkSessionExtension',
    'spark.sql.catalog.spark_catalog': 'org.apache.spark.sql.delta.catalog.DeltaCatalog'
}

batch_bronze_pipeline = SparkSubmitOperator(
    application='/app/spark_scripts/batch_bronze_migrate.py',
    conn_id='spark_cluster',
    task_id='batch_bronze_pipeline',
    packages='io.delta:delta-core_2.12:2.3.0,org.apache.hadoop:hadoop-aws:3.2.2',
    conf=common_spark_conf,
    dag=dag
)

batch_silver_pipeline = SparkSubmitOperator(
    application='/app/spark_scripts/batch_silver_migrate.py',
    conn_id='spark_cluster',
    task_id='batch_silver_pipeline',
    packages='io.delta:delta-core_2.12:2.3.0,org.apache.hadoop:hadoop-aws:3.2.2',
    conf=common_spark_conf,
    dag=dag
)

batch_gold_pipeline = SparkSubmitOperator(
    application='/app/spark_scripts/batch_gold_migrate.py',
    conn_id='spark_cluster',
    task_id='batch_gold_pipeline',
    packages='io.delta:delta-core_2.12:2.3.0,org.apache.hadoop:hadoop-aws:3.2.2',
    conf=common_spark_conf,
    dag=dag
)

batch_bronze_pipeline >> batch_silver_pipeline >> batch_gold_pipeline
