from websocket import create_connection
import json
from kafka import KafkaProducer
from dotenv import load_dotenv
import os
import json

load_dotenv()
originals = json.loads(os.getenv('TICKERS', '[]'))
tickers = json.loads(os.getenv('TICKERS', '[]'))
# tickers.append("NDAQ")
api_key = os.getenv('POLYGON_API_KEY')

producer = KafkaProducer(bootstrap_servers='localhost:9092')

ws = create_connection("wss://delayed.polygon.io/stocks")
ws.send(json.dumps({"action": "auth", "params": api_key}))
ws.send(json.dumps({"action": "subscribe", "params": ','.join([f"A.{ticker}" for ticker in tickers])}))

while True:
    data = ws.recv()
    producer.send('websocket', value=data.encode('utf-8'))


# docker exec -t kafka /opt/bitnami/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --delete --topic websocket