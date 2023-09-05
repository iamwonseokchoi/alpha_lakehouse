from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
from datetime import datetime, timedelta
import json 
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

def validate_technicals_schema():
    connector = CassandraConnector()
    session = connector.session
    query = """
    SELECT column_name, type
    FROM system_schema.columns
    WHERE keyspace_name = 'technicals' AND table_name = '{}';
    """
    technicals = ["ema", "sma", "rsi"]
    for technical in technicals:
        rows = session.execute(query.format(technical))
        schema_dict = {row.column_name: row.type for row in rows}
        expected_schema = {
            'timestamp': 'bigint',
            'value': 'double',
            'symbol': 'text',
            'updated_at': 'timestamp'
        }
        if schema_dict != expected_schema:
            raise ValueError(f"Schema mismatch. Expected {expected_schema}, but found {schema_dict}")
        else:
            print("Schema validated.")

def validate_price_schema():
    connector = CassandraConnector()
    session = connector.session
    query = """
    SELECT column_name, type
    FROM system_schema.columns
    WHERE keyspace_name = 'price' AND table_name = '{}';
    """
    tickers = ["aapl", "amzn", "googl", "msft", "nvda", "tsla"]
    for ticker in tickers:
        rows = session.execute(query.format(ticker))
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
    dag_id = 'ONCE_cassandra_create_tables',
    default_args = default_args,
    schedule_interval = '@once',
    tags = ['once', 'cassandra'],
    description = 'Create tables in Cassandra'
)

create_keyspace_tables = BashOperator(
    task_id = 'create_keyspace',
    bash_command = 'python /app/cassandra_scripts/create_tables.py',
    dag = dag
)

validate_technical_schema = PythonOperator(
    task_id='validate_technical_schema',
    python_callable=validate_technicals_schema,
    dag=dag,
)

validate_price_schema = PythonOperator(
    task_id='validate_price_schema',
    python_callable=validate_price_schema,
    dag=dag,
)

create_keyspace_tables >> [validate_technical_schema, validate_price_schema]