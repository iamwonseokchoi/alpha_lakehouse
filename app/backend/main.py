from fastapi import FastAPI
from cassandra.cluster import Cluster
from datetime import datetime, timedelta
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.gzip import GZipMiddleware
from datetime import datetime, timedelta
from aiohttp import ClientSession
from dotenv import load_dotenv
import json
import os


# Envs
load_dotenv()
tickers = json.loads(os.getenv('TICKERS', '[]'))
api_key = os.getenv('POLYGON_API_KEY')
description = "## 🚀 Alpha Lakehouse Backend Server 🚀"

# Initialize FastAPI backend
app = FastAPI(
    title="Alpha Lakehouse Backend Server",
    description=description,
    contact={
        "name": "Wonseok Choi",
        "email": "wonseok.chris.choi@gmail.com"
    }
)
app.add_middleware(GZipMiddleware, minimum_size=1000)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
    allow_credentials=True,
)

# Initialize Cassandra Connection
cluster = Cluster()
session_price = cluster.connect('price')
session_technicals = cluster.connect('technicals')
session_ml = cluster.connect('ml')
session_cleaned = cluster.connect('cleaned')
session_ws = cluster.connect('websocket')

# Wrapper function for async requests
async def fetch_data(session: ClientSession, url: str) -> dict:
    async with session.get(url) as response:
        return await response.json()

# Wrapper function for news pagination
async def fetch_all_news(session: ClientSession, initial_url: str, api_key: str) -> list:
    all_data = []
    next_url = initial_url

    while next_url:
        response_data = await fetch_data(session, next_url)
        all_data.extend(response_data.get("results", []))

        next_url = response_data.get("next_url")
        if next_url:
            next_url = f"{next_url}&apiKey={api_key}"

    return all_data


@app.get("/tables/{table_name}")
async def read_table(table_name: str):
    symbol = table_name.upper()
    query = f"SELECT * FROM price.{table_name} WHERE symbol = %s ORDER BY timestamp DESC LIMIT 10;"
    
    rows = session_price.execute(query, [symbol])

    processed_rows = []
    for row in rows:
        row_dict = row._asdict()
        row_dict['timestamp'] = datetime.utcfromtimestamp(row_dict['timestamp'] / 1000).strftime('%Y-%m-%d %H:%M:%S')
        processed_rows.append(row_dict)

    return {"data": processed_rows}


@app.get("/technicals/{indicator}/{symbol}")
async def read_technicals(indicator: str, symbol: str):
    indicator = indicator.lower()
    symbol = symbol.upper()
    query = f"SELECT * FROM technicals.{indicator} WHERE symbol = %s ORDER BY timestamp DESC LIMIT 300;"
    
    rows = session_technicals.execute(query, [symbol])

    processed_rows = []
    for row in rows:
        row_dict = row._asdict()
        row_dict['timestamp'] = datetime.utcfromtimestamp(row_dict['timestamp'] / 1000).strftime('%Y-%m-%d %H:%M:%S')
        processed_rows.append(row_dict)

    return {"data": processed_rows}


@app.get("/ml/{symbol}")
async def read_ml(symbol: str):
    query = f"SELECT * FROM ml.{symbol};"

    rows = session_ml.execute(query)
    
    processed_rows = []
    for row in rows:
        row_dict = row._asdict()
        processed_rows.append(row_dict)
    
    return {"data": processed_rows}


@app.get("/cleaned/{symbol}")
async def read_cleaned(symbol:str):
    yoy = date_last_year = (datetime.today() - timedelta(days=100)).strftime("%Y-%m-%d")
    query = f"SELECT timestamp, close FROM cleaned.{symbol} WHERE timestamp > '{yoy}' ALLOW FILTERING;"
    
    rows = session_cleaned.execute(query)
    
    processed_rows = []
    for row in rows:
        row_dict = row._asdict()
        processed_rows.append(row_dict)
    
    return {"data": processed_rows}


@app.get("/news/{symbol}")
async def read_news(symbol: str):
    ticker = symbol.upper()
    date = (datetime.utcnow() - timedelta(days=1)).strftime('%Y-%m-%d')
    initial_url = f"https://api.polygon.io/v2/reference/news?ticker={ticker}&published_utc={date}&order=desc&limit=5&sort=published_utc&apiKey={api_key}"

    async with ClientSession() as session:
        all_news = await fetch_all_news(session, initial_url, api_key)

    return {"data": all_news[:5]}


@app.get("/websocket/{symbol}")
async def read_websocket(symbol: str):
    query = f"SELECT * FROM websocket.ws WHERE symbol = '{symbol.upper()}' ORDER BY end_time DESC LIMIT 2 ALLOW FILTERING;"

    rows = session_ws.execute(query)
    
    processed_rows = []
    for row in rows:
        row_dict = row._asdict()
        processed_rows.append(row_dict)
    
    return {"data": processed_rows}


@app.get("/market")
async def read_market():
    query = f"SELECT * FROM websocket.ws WHERE symbol = 'NDAQ' ORDER BY end_time DESC;"

    rows = session_ws.execute(query)
    
    processed_rows = []
    for row in rows:
        row_dict = row._asdict()
        processed_rows.append(row_dict)
    
    return {"data": processed_rows}

