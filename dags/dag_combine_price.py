from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator 
from datetime import datetime, timedelta
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
import os
from dotenv import load_dotenv

load_dotenv()

class CassandraConnector:
    def __init__(self):
        auth_provider = PlainTextAuthProvider(
            username=os.getenv("CASSANDRA_USERNAME"),
            password=os.getenv("CASSANDRA_PASSWORD")
        )
        self.cluster = Cluster(["cassandra"], auth_provider=auth_provider, port=9042)
        self.session = self.cluster.connect()

def validate_data_count():
    connector = CassandraConnector()
    session = connector.session
    count_row = session.execute("SELECT COUNT(*) FROM price.all_stocks").one()
    if count_row:
        count = count_row.count
        if count == 0:
            print("Warning: No data found in price.all_stocks")
        else:
            print(f"Data count validated: {count} records found.")
    else:
        raise ValueError("Query execution failed.")

def validate_schema():
    connector = CassandraConnector()
    session = connector.session
    query = """
    SELECT column_name, type
    FROM system_schema.columns
    WHERE keyspace_name = 'price' AND table_name = 'all_stocks';
    """
    rows = session.execute(query)
    schema_dict = {row.column_name: row.type for row in rows}
    expected_schema = {
        'symbol': 'text',
        'timestamp': 'bigint',
        'open': 'double',
        'high': 'double',
        'low': 'double',
        'close': 'double',
        'volume': 'bigint',
        'volume_weighted': 'bigint',
        'transactions': 'bigint',
        'time_bucket': 'bigint',
        'updated_at': 'timestamp'
    }
    if schema_dict != expected_schema:
        raise ValueError(f"Schema mismatch. Expected {expected_schema}, but found {schema_dict}")
    else:
        print("Schema validated.")


default_args = {
    'owner': 'wonseok',
    'start_date': datetime.today(),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'catchup': False
}

dag = DAG(
    dag_id = 'BATCH_combine_hourly_price',
    default_args = default_args,
    schedule_interval = timedelta(minutes=5),
    tags = ['batch', 'cassandra'],
    description = 'Merges individual stock tables into one table regularly as batch'
)

batch_compile_spark = SparkSubmitOperator(
    application ='/app/spark_scripts/batch_compile_stream.py' ,
    conn_id= 'spark_cluster', 
    task_id='spark_submit_task',
    packages='com.datastax.spark:spark-cassandra-connector_2.12:3.4.1', 
    dag=dag
    )

validate_data_count_task = PythonOperator(
    task_id='validate_data_count',
    python_callable=validate_data_count,
    dag=dag,
)

validate_schema_task = PythonOperator(
    task_id='validate_schema',
    python_callable=validate_schema,
    dag=dag,
)


batch_compile_spark >> [validate_data_count_task, validate_schema_task]
