from kafka import KafkaProducer
from concurrent.futures import ThreadPoolExecutor
from dotenv import load_dotenv
import datetime
import requests
import json
import os
import logging

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
