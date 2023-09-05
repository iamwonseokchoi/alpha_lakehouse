from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider 
import json
import os
from dotenv import load_dotenv

load_dotenv()
tickers = json.loads(os.getenv('TICKERS'))

q0 = """CREATE KEYSPACE IF NOT EXISTS price WITH replication = {
    'class':'SimpleStrategy', 'replication_factor':1};
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

q2 = """
CREATE TABLE IF NOT EXISTS price.all_stocks (
    symbol TEXT,
    timestamp BIGINT,
    open DOUBLE,
    high DOUBLE,
    low DOUBLE,
    close DOUBLE,
    volume BIGINT,
    volume_weighted BIGINT, 
    transactions BIGINT,
    time_bucket BIGINT,
    updated_at TIMESTAMP,
    PRIMARY KEY ((time_bucket, symbol), timestamp)
) WITH CLUSTERING ORDER BY (timestamp DESC);
"""

q3 = """
CREATE MATERIALIZED VIEW IF NOT EXISTS price.latest_prices AS
    SELECT time_bucket, timestamp, symbol, close FROM price.all_stocks
    WHERE time_bucket IS NOT NULL AND symbol IS NOT NULL AND timestamp IS NOT NULL
    PRIMARY KEY ((symbol, time_bucket), timestamp)
    WITH CLUSTERING ORDER BY (timestamp DESC)
    AND comment='Latest prices (10hrs) for all stocks';
"""

q4 = """CREATE KEYSPACE technicals WITH replication = {
    'class':'SimpleStrategy', 'replication_factor':1};
"""

q5 = """
CREATE TABLE IF NOT EXISTS technicals.{} (
    timestamp BIGINT,
    value DOUBLE,
    symbol TEXT,
    updated_at TIMESTAMP,
    PRIMARY KEY (symbol, timestamp)
);
"""

q6 = """
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

def connect_to_cassandra():
    auth_provider = PlainTextAuthProvider(
        username=os.getenv("CASSANDRA_USERNAME"),
        password=os.getenv("CASSANDRA_PASSWORD"))
    cluster = Cluster(["cassandra"], auth_provider=auth_provider, port=9042)
    session = cluster.connect() 
    return session

def main():
    session = connect_to_cassandra()
    try: 
        session.execute(q0)
        session.execute(q2)
        session.execute(q4)
        session.execute(q6)
        
        technicals = ["ema", "sma", "rsi"]
        for techincal in technicals:
            session.execute(q5.format(techincal))
        try: 
            session.execute(q3)
        except Exception as e:
            pass
        
        for ticker in tickers:
            session.execute(q1.format(ticker.lower()))
    except Exception as e:
        pass

if __name__ == "__main__":
    main()