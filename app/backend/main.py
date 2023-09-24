from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import JSONResponse
import json
from dotenv import load_dotenv
import os
import websockets

# Load environment variables
load_dotenv()
POLYGON_API_KEY = os.getenv("POLYGON_API_KEY")

app = FastAPI()

async def connect_to_polygon():
    uri = "wss://delayed.polygon.io/stocks"

    async with websockets.connect(uri) as websocket:
        auth_data = json.dumps({
            "action": "auth",
            "params": POLYGON_API_KEY
        })
        await websocket.send(auth_data)

        subscribe_data = json.dumps({
            "action": "subscribe",
            "params": "A.GOOGL"
        })
        await websocket.send(subscribe_data)

        async for message in websocket:
            yield message

# This could be replaced by a more robust in-memory database like Redis in a production environment
latest_stock_data = None

@app.websocket("/stocks")
async def websocket_endpoint(websocket: WebSocket):
    global latest_stock_data
    await websocket.accept()
    try:
        async for polygon_message in connect_to_polygon():
            latest_stock_data = json.loads(polygon_message)
            await websocket.send_text(f"Polygon.io Data: {polygon_message}")
    except WebSocketDisconnect:
        pass

@app.get("/stocks/show")
async def get_latest_stock_data():
    if latest_stock_data is not None:
        return JSONResponse(content=latest_stock_data)
    else:
        return JSONResponse(content={"message": "No stock data available"}, status_code=404)
