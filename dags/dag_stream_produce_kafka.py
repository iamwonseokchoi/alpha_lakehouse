from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

from concurrent.futures import ThreadPoolExecutor
import threading
import requests
import json
import logging
from dotenv import load_dotenv
from kafka import KafkaProducer
import os

class PolygonPriceAPI:
    def __init__(self):
        load_dotenv()
        logging.basicConfig(level=logging.INFO)
        self.producer = KafkaProducer(
            bootstrap_servers=['localhost:9092'],
            value_serializer=lambda x: json.dumps(x).encode('utf-8')
        )
        self.api_key = os.getenv('POLYGON_API_KEY')
        self.tickers = json.loads(os.getenv('TICKERS'))

    def get_date_range(self, days_ago_start=730, days_ago_end=1):
        end_date = datetime.now() - timedelta(days=days_ago_end) 
        start_date = datetime.now() - timedelta(days=days_ago_start) 
        return [(start_date + timedelta(days=i)).strftime('%Y-%m-%d') for i in range((end_date - start_date).days + 1)]

    def add_symbol_to_payload(self, symbol, payload):
        for data_point in payload:
            data_point['symbol'] = symbol
        return payload

    def validate_response(self, data):
        return "results" in data and isinstance(data["results"], list)

    def send_to_kafka(self, topic, payload):
        self.producer.send(topic, payload)

    def fetch_data(self, symbol, date, retry_count=0, next_url=None):
        if retry_count >= 2:
            logging.info(f"Reached maximum retry count for {symbol} on {date}. Moving on.")
            return
        url = next_url if next_url else f"https://api.polygon.io/v2/aggs/ticker/{symbol}/range/1/minute/{date}/{date}?adjusted=true&sort=asc&limit=25000&apiKey={self.api_key}"
        try:
            response = requests.get(url)
            data = response.json()
            if self.validate_response(data):
                payload = self.add_symbol_to_payload(symbol, data["results"])
                for individual_payload in payload:
                    logging.info(f"Fetched payload for {symbol} on {date}: {individual_payload}")
                    self.send_to_kafka(symbol.lower(), individual_payload)
                
                if 'next_url' in data:
                    logging.info(f"Fetching additional data for {symbol} on {date} from next_url.")
                    self.fetch_data(symbol, date, retry_count, data['next_url'])
            else:
                logging.error(f"Invalid data for {symbol} on {date}: {data}")
        except Exception as e:
            logging.error(f"Error fetching data for {symbol} on {date}: {e}")
            self.fetch_data(symbol, date, retry_count + 1)

    def execute(self, days_ago_start=1825, days_ago_end=1):
        date_range = self.get_date_range(days_ago_start, days_ago_end)
        
        for date in date_range:
            with ThreadPoolExecutor() as executor:
                futures = [executor.submit(self.fetch_data, symbol, date) for symbol in self.tickers]
                for future in futures:
                    future.result()


class PolygonTechnicalsAPI:
    def __init__(self):
        logging.basicConfig(level=logging.INFO)
        load_dotenv()
        self.api_key = os.getenv('POLYGON_API_KEY')
        self.tickers = json.loads(os.getenv('TICKERS'))
        self.producer = KafkaProducer(
            bootstrap_servers=['localhost:9092'],
            value_serializer=lambda x: json.dumps(x).encode('utf-8')
        )

    def produce_to_kafka(self, api_func, symbol):
        try:
            topic, data = api_func(symbol)
            if data:
                for record in data:
                    self.producer.send(topic, value=record)
                    record_timestamp = record.get('timestamp', 'N/A')
                    logging.info(f"Data for {datetime.datetime.fromtimestamp(record_timestamp/1000).strftime('%Y-%m-%d')} sent to Kafka Topic: {topic}")
            else:
                logging.warning(f"No data sent for topic: {topic}")
        except requests.exceptions.RequestException as e:
            logging.error(f"API request failed: {str(e)}")
        except Exception as e:
            logging.error(f"Failed to produce to Kafka: {str(e)}")

    def construct_url(self, base_url, symbol, params):
        params['apiKey'] = self.api_key
        param_str = '&'.join([f"{k}={v}" for k, v in params.items()])
        return f"{base_url}{symbol}?{param_str}"

    def add_symbol_to_payload(self, symbol, payload):
        for data_point in payload:
            data_point['symbol'] = symbol
        return payload

    def get_sma_by_hour(self, symbol, timestamp):
        topic_name = "sma"
        url = self.construct_url("https://api.polygon.io/v1/indicators/sma/", symbol, {
            'timestamp': timestamp,
            'timespan': 'minute',
            'adjusted': 'true',
            'window': 20,
            'series_type': 'close',
            'order': 'desc',
            'limit': 1000
        })
        response = requests.get(url)
        data = response.json()
        payload = data.get("results", {}).get("values", {})
        payload = self.add_symbol_to_payload(symbol, payload)
        return (topic_name, payload)

    def get_ema_by_hour(self, symbol, timestamp):
        topic_name = "ema"
        url = self.construct_url("https://api.polygon.io/v1/indicators/ema/", symbol, {
            'timestamp': timestamp,
            'timespan': 'minute',
            'adjusted': 'true',
            'window': 20,
            'series_type': 'close',
            'order': 'desc',
            'limit': 1000
        })
        response = requests.get(url)
        data = response.json()
        payload = data.get("results", {}).get("values", {})
        payload = self.add_symbol_to_payload(symbol, payload)
        return (topic_name, payload)

    def get_macd_by_hour(self, symbol, timestamp):
        topic_name = "macd"
        url = self.construct_url("https://api.polygon.io/v1/indicators/macd/", symbol, {
            'timestamp': timestamp,
            'timespan': 'minute',
            'adjusted': 'true',
            'short_window': 10,
            'long_window': 40,
            'signal_window': 25,
            'series_type': 'close',
            'expand_underlying': 'false',
            'order': 'desc',
            'limit': 1000
        })
        response = requests.get(url)
        data = response.json()
        payload = data.get("results", {}).get("values", {})
        payload = self.add_symbol_to_payload(symbol, payload)
        return (topic_name, payload)

    def get_rsi_by_hour(self, symbol, timestamp):
        topic_name = "rsi"
        url = self.construct_url("https://api.polygon.io/v1/indicators/rsi/", symbol, {
            'timestamp': timestamp,
            'timespan': 'minute',
            'adjusted': 'true',
            'window': 20,
            'series_type': 'close',
            'order': 'desc',
            'limit': 1000
        })
        response = requests.get(url)
        data = response.json()
        payload = data.get("results", {}).get("values", {})
        payload = self.add_symbol_to_payload(symbol, payload)
        return (topic_name, payload)


    def parallel_produce_to_kafka(self, symbol, api_func_list):
        with ThreadPoolExecutor() as executor:
            futures = {executor.submit(self.produce_to_kafka, api_func, symbol): api_func for api_func in api_func_list}
            for future in futures:
                future.result()

    def execute(self, furthest_day_back=1825, closest_day_back=1):
        today = datetime.datetime.today()
        
        for days in reversed(range(closest_day_back, furthest_day_back)):
            target_date = today - datetime.timedelta(days=days)
            timestamp = target_date.strftime('%Y-%m-%d')
            
            api_functions = [
                lambda s: self.get_sma_by_hour(s, timestamp),
                lambda s: self.get_ema_by_hour(s, timestamp),
                lambda s: self.get_macd_by_hour(s, timestamp),
                lambda s: self.get_rsi_by_hour(s, timestamp)
            ]
            
            with ThreadPoolExecutor() as executor:
                futures = {executor.submit(self.parallel_produce_to_kafka, symbol, api_functions): symbol for symbol in self.tickers}
                for future in futures:
                    future.result()


def call_api():
    polygon_price_api = PolygonPriceAPI()
    polygon_technicals_api = PolygonTechnicalsAPI()

    price_thread = threading.Thread(target=polygon_price_api.execute, args=(5, 0))
    technical_thread = threading.Thread(target=polygon_technicals_api.execute, args=(5, 0))

    price_thread.start()
    technical_thread.start()

    price_thread.join()
    technical_thread.join()


default_args = {
    'owner': 'wonseok',
    'start_date': datetime.today(),
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
    'catchup': False
}

dag = DAG(
    dag_id = 'STREAM_api_to_kafka',
    default_args = default_args,
    schedule_interval='0 13 * * *',
    tags = ['stream', 'api', 'kafka'],
    description = 'Daily call API and send to Kafka'
)

call_api_daily = PythonOperator(
    task_id='call_api_daily',
    python_callable=call_api,
    dag=dag,
)


call_api_daily