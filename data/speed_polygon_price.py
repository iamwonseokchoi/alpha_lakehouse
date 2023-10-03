from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta
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

    def get_date_range(self, days_ago_start=1825, days_ago_end=1):
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
        url = next_url if next_url else f"https://api.polygon.io/v2/aggs/ticker/{symbol}/range/1/second/{date}/{date}?adjusted=true&sort=asc&limit=50000&apiKey={self.api_key}"
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