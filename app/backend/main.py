from fastapi import FastAPI
from cassandra.cluster import Cluster
from datetime import datetime, timezone, timedelta
import time
from fastapi.middleware.cors import CORSMiddleware

# Initialize FastAPI backend
app = FastAPI()
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
    allow_credentials=True
)

# Initialize Cassandra Connection
cluster = Cluster()
session = cluster.connect('price')


@app.get("/tables/{table_name}")
async def read_table(table_name: str):
    today = datetime.now().astimezone(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
    three_days_ago = today - timedelta(days=3)
    three_days_ago_unix = int(time.mktime(three_days_ago.timetuple())) * 1000
    symbol = table_name.upper()
    
    query = f"SELECT * FROM price.{table_name} WHERE symbol = '{symbol}' AND timestamp >= {three_days_ago_unix} ORDER BY timestamp DESC ALLOW FILTERING;"
    rows = session.execute(query)

    processed_rows = []
    for row in rows:
        row_dict = row._asdict()
        row_dict['timestamp'] = datetime.utcfromtimestamp(row_dict['timestamp'] / 1000).strftime('%Y-%m-%d %H:%M:%S')
        processed_rows.append(row_dict)

    return {"data": processed_rows}
