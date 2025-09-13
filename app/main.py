from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from .connection_manager import ConnectionManager
import asyncio
from .kalshi_client import orderbook_websocket
from contextlib import asynccontextmanager



manager = ConnectionManager()

@asynccontextmanager
async def lifespan(app: FastAPI):
    asyncio.create_task(orderbook_websocket(manager.broadcast))
    yield

app = FastAPI(lifespan=lifespan)

@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await manager.connect(websocket)
    try:
        while True:
            data = await websocket.receive_text()
            
    except WebSocketDisconnect:
        manager.disconnect(websocket)



if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)