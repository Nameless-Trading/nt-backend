
import base64
import json
import time
import websockets
from cryptography.hazmat.primitives import serialization, hashes
from cryptography.hazmat.primitives.asymmetric import padding
import os
from dotenv import load_dotenv
from rich import print

load_dotenv(override=True)

KEY_ID = os.getenv("KALSHI_API_KEY")
PRIVATE_KEY_PATH = "kalshi-api-key.txt"
MARKET_TICKER = "KXNCAAFGAME-25SEP12COLOHOU-HOU"
WS_URL = "wss://api.elections.kalshi.com/trade-api/ws/v2"

def sign_pss_text(private_key, text: str) -> str:
    """Sign message using RSA-PSS"""
    message = text.encode('utf-8')
    signature = private_key.sign(
        message,
        padding.PSS(
            mgf=padding.MGF1(hashes.SHA256()),
            salt_length=padding.PSS.DIGEST_LENGTH
        ),
        hashes.SHA256()
    )
    return base64.b64encode(signature).decode('utf-8')

def create_headers(private_key, method: str, path: str) -> dict:
    """Create authentication headers"""
    timestamp = str(int(time.time() * 1000))
    msg_string = timestamp + method + path.split('?')[0]
    signature = sign_pss_text(private_key, msg_string)
    
    return {
        "Content-Type": "application/json",
        "KALSHI-ACCESS-KEY": KEY_ID,
        "KALSHI-ACCESS-SIGNATURE": signature,
        "KALSHI-ACCESS-TIMESTAMP": timestamp,
    }

async def orderbook_websocket(callback=None):
    """Connect to WebSocket and subscribe to orderbook"""
    with open(PRIVATE_KEY_PATH, 'rb') as f:
        private_key = serialization.load_pem_private_key(
            f.read(),
            password=None
        )
    
    ws_headers = create_headers(private_key, "GET", "/trade-api/ws/v2")
    async with websockets.connect(WS_URL, additional_headers=ws_headers) as websocket:
        print(f"Connected! Subscribing to orderbook for {MARKET_TICKER}")
        
        subscribe_msg = {
            "id": 1,
            "cmd": "subscribe",
            "params": {
                "channels": ["orderbook_delta"],
                "market_tickers": ["KXNCAAFGAME-25SEP12COLOHOU-HOU", "KXNCAAFGAME-25SEP12COLOHOU-COLO"]
            }
        }

        await websocket.send(json.dumps(subscribe_msg))
        
        async for message in websocket:
            data = json.loads(message)
            msg_type = data.get("type")
            
            if msg_type == "subscribed":
                if callback:
                    await callback(f"Subscribed: {data}")
                else:
                    print(f"Subscribed: {data}")
                
            elif msg_type == "orderbook_snapshot":
                if callback:
                    await callback(f"Orderbook snapshot: {data}")
                else:
                    print(f"Orderbook snapshot: {data}")
                
            elif msg_type == "orderbook_delta":
                if callback:
                    await callback(f"Orderbook update: {data}")
                else:
                    if 'client_order_id' in data.get('data', {}):
                        print(f"Orderbook update (your order {data['data']['client_order_id']}): {data}")
                    else:
                        print(f"Orderbook update: {data}")
                        
            elif msg_type == "error":
                if callback:
                    await callback(f"Error: {data}")
                else:
                    print(f"Error: {data}")