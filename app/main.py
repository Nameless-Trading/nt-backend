from fastapi import FastAPI, WebSocket
import asyncio
from contextlib import asynccontextmanager
from app.order_book_manager import OrderBookManager
from app.connection_manager import ConnectionManager
from app.kalshi_client import KalshiClient, KalshiWebSocketClient
from typing import Optional
from dotenv import load_dotenv
import os
import logging
from rich.logging import RichHandler

# Configure logging with Rich handler
logging.basicConfig(
    level=logging.INFO,
    format="%(message)s",
    datefmt="[%X]",
    handlers=[RichHandler(rich_tracebacks=True)]
)

logger = logging.getLogger(__name__)

load_dotenv(override=True)

# Global instances
order_book_manager = OrderBookManager()
client_manager = ConnectionManager()
kalshi_client: Optional[KalshiClient] = None
kalshi_websocket_client: Optional[KalshiWebSocketClient] = None

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
                order_book_manager.update_from_snapshot(market_ticker, yes_orders, no_orders)

            case 'orderbook_delta':
                market_ticker = msg.get('market_ticker')
                price = msg.get('price')
                delta = msg.get('delta')
                side = msg.get('side')
                ts = msg.get('ts')

                order_book_manager.update_from_delta(market_ticker, price, delta, side)
                logger.info(order_book_manager.get_order_book(market_ticker).top_of_book())

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
    market_tickers = ['KXNCAAFGAME-25SEP13CLEMGT-CLEM', 'KXNCAAFGAME-25SEP13CLEMGT-GT']
    channels = ['orderbook_delta']
    asyncio.create_task(process_messages(kalshi_websocket_client, channels, market_tickers))
    
    yield
    
    # Shutdown
    logger.info("Shutting down...")

app = FastAPI(lifespan=lifespan)


@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    """WebSocket endpoint for client connections"""
    pass