from fastapi import FastAPI, WebSocket, WebSocketDisconnect
import asyncio
from contextlib import asynccontextmanager
from app.order_book_manager import OrderBookManager
from app.connection_manager import ConnectionManager
from app.kalshi_client import KalshiClient, KalshiWebSocketClient
from app.logger import logger
from typing import Optional
from dotenv import load_dotenv
import os
import json

load_dotenv(override=True)

# Global instances
order_book_manager = OrderBookManager()
client_manager = ConnectionManager()
kalshi_client: Optional[KalshiClient] = None
kalshi_websocket_client: Optional[KalshiWebSocketClient] = None

async def broadcast_top_of_book(market_ticker: str):
    """Broadcast top of book for a specific ticker to all connected clients"""
    order_book = order_book_manager.get_order_book(market_ticker)
    if order_book:
        top_of_book = order_book.top_of_book()
        
        await client_manager.broadcast(json.dumps(top_of_book))

async def send_all_top_of_books(websocket: WebSocket):
    """Send current state of all order books to a newly connected client"""
    all_tickers = order_book_manager.get_all_tickers()
    
    for ticker in all_tickers:
        order_book = order_book_manager.get_order_book(ticker)
        if order_book:
            top_of_book = order_book.top_of_book()
            
            await websocket.send_text(json.dumps(top_of_book))

async def process_messages(client: KalshiWebSocketClient, channels: list, tickers: list):
    """Process messages from Kalshi WebSocket"""
    async for message in client.subscribe(channels, tickers):
        msg_type = message.get('type')
        msg = message.get('msg')

        match msg_type:

            case 'subscribed':
                channel = msg.get('channel')
                logger.info(f"Subscribed to {channel}")

            case 'orderbook_snapshot':
                market_ticker = msg.get('market_ticker')
                yes_orders = msg.get('yes')
                no_orders = msg.get('no')

                # Store previous top of book for comparison
                order_book = order_book_manager.get_order_book(market_ticker)
                prev_top = order_book.top_of_book() if order_book else None
                
                # Update order book
                order_book_manager.update_from_snapshot(market_ticker, yes_orders, no_orders)
                
                # Check if top of book changed and broadcast if it did
                new_top = order_book_manager.get_order_book(market_ticker).top_of_book()
                if prev_top != new_top:
                    logger.info(new_top)
                    await broadcast_top_of_book(market_ticker)

            case 'orderbook_delta':
                market_ticker = msg.get('market_ticker')
                price = msg.get('price')
                delta = msg.get('delta')
                side = msg.get('side')
                ts = msg.get('ts')

                # Store previous top of book for comparison
                order_book = order_book_manager.get_order_book(market_ticker)
                prev_top = order_book.top_of_book() if order_book else None
                
                # Update order book
                order_book_manager.update_from_delta(market_ticker, price, delta, side)
                
                # Check if top of book changed and broadcast if it did
                new_top = order_book_manager.get_order_book(market_ticker).top_of_book()
                if prev_top != new_top:
                    logger.info(new_top)
                    await broadcast_top_of_book(market_ticker)

            case _:
                logger.info(msg)

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Manage application lifecycle"""
    global kalshi_websocket_client

    logger.info("Starting up...")

    kalshi_api_key = os.getenv("KALSHI_API_KEY")
    private_key_path = 'kalshi-api-key.txt'
    
    kalshi_client = KalshiClient(kalshi_api_key, private_key_path)
    kalshi_websocket_client = KalshiWebSocketClient(kalshi_api_key, private_key_path)

    # tickers = kalshi_client.get_tickers()
    market_tickers = kalshi_client.get_tickers()
    # market_tickers = ['KXNCAAFGAME-25SEP13CLEMGT-CLEM', 'KXNCAAFGAME-25SEP13CLEMGT-GT']
    channels = ['orderbook_delta']
    asyncio.create_task(process_messages(kalshi_websocket_client, channels, market_tickers))
    
    yield
    
    # Shutdown
    logger.info("Shutting down...")

app = FastAPI(lifespan=lifespan)


@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    """WebSocket endpoint for client connections"""
    await client_manager.connect(websocket)
    
    try:
        # Send initial snapshot of all order books
        logger.info(f"New client connected, sending all top of books")
        await send_all_top_of_books(websocket)
        
        # Keep connection alive and handle client messages
        while True:
            # Wait for any client messages (like ping/pong or subscriptions)
            data = await websocket.receive_text()
            
            # Handle client messages if needed
            try:
                client_message = json.loads(data)
                
                # Handle different message types from client
                if client_message.get("type") == "ping":
                    await websocket.send_text(json.dumps({"type": "pong"}))
                    
                # Add other client message handlers here if needed
                
            except json.JSONDecodeError:
                logger.warning(f"Invalid JSON from client: {data}")
                
    except WebSocketDisconnect:
        client_manager.disconnect(websocket)
        logger.info("Client disconnected")
    except Exception as e:
        logger.error(f"WebSocket error: {e}")
        client_manager.disconnect(websocket)