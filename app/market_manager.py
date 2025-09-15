from typing import Dict
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo
import polars as pl


@dataclass
class Market:
    ticker: str
    event_ticker: str
    title: str
    team_name: str
    expected_expiration_time_utc: datetime
    estimated_start_time: datetime

    @property
    def game_start_time(self) -> datetime:
        return self.expected_expiration_time_utc.astimezone(
            ZoneInfo("America/Denver")
        ) - timedelta(hours=3)


class MarketManager:
    def __init__(self) -> None:
        self._markets: Dict[str, Market] = {}

    def load(self, markets_df: pl.DataFrame) -> None:
        markets_clean = [
            Market(
                ticker=market["ticker"],
                event_ticker=market["event_ticker"],
                title=market["title"],
                team_name=market["yes_sub_title"],
                expected_expiration_time_utc=market['expected_expiration_time'],
                estimated_start_time=market["estimated_start_time"],
            )
            for market in markets_df.to_dicts()
        ]

        for market in markets_clean:
            self._markets[market.ticker] = market

    def get_market(self, ticker: str) -> Market:
        return self._markets[ticker]

    def get_tickers(self) -> list[str]:
        return list(self._markets.keys())
