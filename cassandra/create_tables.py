from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
from cassandra.query import BatchStatement
import json
import os
from dotenv import load_dotenv

load_dotenv()
tickers = json.loads(os.getenv('TICKERS'))

q0 = """CREATE KEYSPACE IF NOT EXISTS price WITH replication = {
    'class':'SimpleStrategy', 'replication_factor':3};
"""

q1 = """
CREATE TABLE IF NOT EXISTS price.{} (
    symbol TEXT,
    timestamp BIGINT,
    open DOUBLE,
    high DOUBLE,
    low DOUBLE,
    close DOUBLE,
    volume BIGINT,
    volume_weighted BIGINT, 
    transactions BIGINT,
    updated_at TIMESTAMP,
    PRIMARY KEY (symbol, timestamp)
);
"""

q2 = """CREATE KEYSPACE technicals WITH replication = {
    'class':'SimpleStrategy', 'replication_factor':3};
"""

q3 = """
CREATE TABLE IF NOT EXISTS technicals.{} (
    timestamp BIGINT,
    value DOUBLE,
    symbol TEXT,
    updated_at TIMESTAMP,
    PRIMARY KEY (symbol, timestamp)
);
"""

q4 = """
CREATE TABLE IF NOT EXISTS technicals.macd (
    timestamp BIGINT,
    value DOUBLE,
    signal DOUBLE,
    histogram DOUBLE,
    symbol TEXT,
    updated_at TIMESTAMP,
    PRIMARY KEY (symbol, timestamp)
);
"""

q5 = """
CREATE KEYSPACE IF NOT EXISTS ml WITH replication = {
    'class':'SimpleStrategy', 'replication_factor':3};
"""

q6 = """
CREATE TABLE IF NOT EXISTS ml.{} (
    timestamp TIMESTAMP,
    close DOUBLE,
    PRIMARY KEY (timestamp, close)
);
"""

q7 = """
CREATE KEYSPACE IF NOT EXISTS cleaned WITH replication = {
    'class':'SimpleStrategy', 'replication_factor':3};
"""

q8 = """
CREATE TABLE IF NOT EXISTS cleaned.{} (
    timestamp TIMESTAMP,
    updated_at TIMESTAMP,
    open DOUBLE,
    high DOUBLE,
    low DOUBLE,
    close DOUBLE,
    volume BIGINT,
    volume_weighted DOUBLE,
    transactions BIGINT,
    ema DOUBLE,
    macd DOUBLE,
    rsi DOUBLE,
    sma DOUBLE,
    PRIMARY KEY ((timestamp, updated_at))
);
"""

q9 = """
CREATE KEYSPACE IF NOT EXISTS websocket WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 3};
"""

q10 = """
CREATE TABLE IF NOT EXISTS websocket.ws (
    event TEXT,
    symbol TEXT,
    volume BIGINT,
    agg_volume BIGINT,
    open_price DOUBLE,
    volume_weighted DOUBLE,
    open DOUBLE,
    close DOUBLE,
    high DOUBLE,
    low DOUBLE,
    avg_price DOUBLE,
    total_trades BIGINT,
    start_time BIGINT,
    end_time BIGINT,
    updated_at TIMESTAMP,
    PRIMARY KEY (symbol, end_time))
    WITH CLUSTERING ORDER BY (end_time DESC);
"""

def connect_to_cassandra():
    auth_provider = PlainTextAuthProvider(
        username=os.getenv("CASSANDRA_USERNAME"),
        password=os.getenv("CASSANDRA_PASSWORD"))
    cluster = Cluster(["localhost"], auth_provider=auth_provider, port=9042)
    session = cluster.connect() 
    return session

def main():
    session = connect_to_cassandra()
    try: 
        session.execute(q0)
        session.execute(q2)
        session.execute(q4)
        session.execute(q5)
        session.execute(q7)
        session.execute(q9)
        session.execute(q10)
        
        technicals = ["ema", "sma", "rsi"]
        for techincal in technicals:
            session.execute(q3.format(techincal))
        
        for ticker in tickers:
            session.execute(q1.format(ticker.lower()))
            session.execute(q6.format(ticker.lower()))
            session.execute(q8.format(ticker.lower()))
    except Exception as e:
        return e

if __name__ == "__main__":
    main()