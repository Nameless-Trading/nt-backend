import base64
import json
import time
import websockets
from cryptography.hazmat.primitives import serialization, hashes
from cryptography.hazmat.primitives.asymmetric import padding
import requests


def sign_pss_text(private_key, text: str) -> str:
    """Sign message using RSA-PSS"""
    message = text.encode("utf-8")
    signature = private_key.sign(
        message,
        padding.PSS(
            mgf=padding.MGF1(hashes.SHA256()), salt_length=padding.PSS.DIGEST_LENGTH
        ),
        hashes.SHA256(),
    )
    return base64.b64encode(signature).decode("utf-8")


def create_headers(
    private_key: str, kalshi_api_key: str, method: str, path: str
) -> dict:
    """Create authentication headers"""
    timestamp = str(int(time.time() * 1000))
    msg_string = timestamp + method + path.split("?")[0]
    signature = sign_pss_text(private_key, msg_string)

    return {
        "Content-Type": "application/json",
        "KALSHI-ACCESS-KEY": kalshi_api_key,
        "KALSHI-ACCESS-SIGNATURE": signature,
        "KALSHI-ACCESS-TIMESTAMP": timestamp,
    }


class KalshiClient:
    def __init__(self, kalshi_api_key: str, private_key_path: str) -> None:
        self._kalshi_api_key = kalshi_api_key

        with open(private_key_path, "rb") as f:
            self._private_key = serialization.load_pem_private_key(
                f.read(), password=None
            )

    def get_tickers(self) -> list[str]:
        base_url = "https://api.elections.kalshi.com"
        endpoint = "/trade-api/v2/markets/"
        method = "GET"

        params = {"limit": 1000, "status": "open", "series_ticker": "KXNCAAFGAME"}

        headers = create_headers(
            self._private_key, self._kalshi_api_key, method, endpoint
        )

        response = requests.get(base_url + endpoint, params=params, headers=headers)
        markets = response.json()["markets"]

        tickers = [market["ticker"] for market in markets]

        return tickers

    def get_markets(self, tickers: list[str] | None = None) -> list[str]:
        base_url = "https://api.elections.kalshi.com"
        endpoint = "/trade-api/v2/markets/"
        method = "GET"

        params = {"limit": 1000, "status": "open", "series_ticker": "KXNCAAFGAME"}

        if tickers is not None:
            params["tickers"] = ",".join(tickers)

        headers = create_headers(
            self._private_key, self._kalshi_api_key, method, endpoint
        )

        response = requests.get(base_url + endpoint, params=params, headers=headers)
        markets = response.json()["markets"]

        return markets


class KalshiWebSocketClient:
    def __init__(self, kalshi_api_key: str, private_key_path: str) -> None:
        self._kalshi_api_key = kalshi_api_key

        with open(private_key_path, "rb") as f:
            self._private_key = serialization.load_pem_private_key(
                f.read(), password=None
            )

    async def subscribe(self, channels: list[str], market_tickers: list[str]):
        """Connect to WebSocket and subscribe to orderbook"""
        base_url = "wss://api.elections.kalshi.com"
        endpoint = "/trade-api/ws/v2"
        method = "GET"
        ws_headers = create_headers(
            self._private_key, self._kalshi_api_key, method, endpoint
        )

        async with websockets.connect(
            base_url + endpoint, additional_headers=ws_headers
        ) as websocket:
            subscribe_msg = {
                "id": 1,
                "cmd": "subscribe",
                "params": {"channels": channels, "market_tickers": market_tickers},
            }

            await websocket.send(json.dumps(subscribe_msg))

            async for message in websocket:
                data = json.loads(message)
                yield data
