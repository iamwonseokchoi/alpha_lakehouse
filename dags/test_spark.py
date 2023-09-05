import airflow
from datetime import timedelta
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator 
from airflow.utils.dates import days_ago

default_args = {
    'owner': 'wonseok',    
    'retry_delay': timedelta(minutes=5),
}

dag_spark = DAG(
        dag_id = "TEST_spark_test",
        default_args=default_args,
        schedule_interval='@once',	
        dagrun_timeout=timedelta(minutes=60),
        description='testing spark on airflow',
        start_date = days_ago(0),
        tags=['test']
)

spark_submit_local = SparkSubmitOperator(
    application ='/app/spark_scripts/test_spark.py' ,
    conn_id= 'spark_cluster', 
    task_id='spark_submit_task', 
    dag=dag_spark
    )


spark_submit_local