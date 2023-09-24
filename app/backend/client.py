import asyncio
import websockets

async def websocket_client():
    uri = "ws://localhost:8000/stocks"
    async with websockets.connect(uri) as websocket:
        while True:
            message = await websocket.recv()
            print(f"Received message: {message}")

if __name__ == "__main__":
    asyncio.get_event_loop().run_until_complete(websocket_client())
