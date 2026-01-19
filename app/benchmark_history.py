import datetime as dt
from typing import Literal
from zoneinfo import ZoneInfo

import bear_lake as bl
import polars as pl
from alpaca.data.enums import Adjustment
from alpaca.data.requests import StockBarsRequest
from alpaca.data.timeframe import TimeFrame, TimeFrameUnit
from clients import get_alpaca_historical_stock_data_client, get_bear_lake_client
from utils import get_last_market_date


def clean_benchmark_history(benchmark_history: pl.DataFrame) -> pl.DataFrame:
    return (
        benchmark_history.sort("timestamp")
        .with_columns(
            pl.col("timestamp").dt.convert_time_zone("America/New_York"),
            pl.col("close").pct_change().fill_null(0).alias("return_"),
        )
        .with_columns(
            pl.col("return_").add(1).cum_prod().sub(1).alias("cumulative_return")
        )
        .select(
            "timestamp",
            pl.col("close").alias("price"),
            pl.col("return_"),
            pl.col("cumulative_return"),
        )
    )


def aggregate_benchmark_history(
    benchmark_history: pl.DataFrame, interval: dt.timedelta
) -> pl.DataFrame:
    market_open = dt.time(7, 30, 0, tzinfo=ZoneInfo("America/New_York"))
    market_close = dt.time(16, 0, 0, tzinfo=ZoneInfo("America/New_York"))

    return (
        benchmark_history.filter(
            pl.col("timestamp").dt.time().is_between(market_open, market_close)
        )
        .sort("timestamp")
        .with_columns(pl.col("timestamp").dt.date().alias("date"))
        .group_by_dynamic("timestamp", every=interval)
        .agg(pl.col("price").last())
        .sort("timestamp")
        .with_columns(pl.col("price").pct_change().fill_null(0).alias("return_"))
        .with_columns(
            pl.col("return_").add(1).cum_prod().sub(1).alias("cumulative_return")
        )
        .select("timestamp", "price", "return_", "cumulative_return")
    )


def get_benchmark_history_for_today(tickers: list[str]) -> pl.DataFrame:
    today = dt.datetime.now(ZoneInfo("America/New_York")).date()
    last_market_date = get_last_market_date()

    if today != last_market_date:
        return pl.DataFrame(
            schema={
                "timestamp": pl.Datetime(time_zone="UTC", time_unit="ns"),
                "ticker": pl.String,
                "close": pl.Float64,
            }
        )

    ext_open = dt.time(4, 0, 0, tzinfo=ZoneInfo("America/New_York"))
    ext_close = dt.time(20, 0, 0, tzinfo=ZoneInfo("America/New_York"))

    start = dt.datetime.combine(today, ext_open)
    end = dt.datetime.combine(today, ext_close)

    alpaca_client = get_alpaca_historical_stock_data_client()

    request = StockBarsRequest(
        symbol_or_symbols=tickers,
        start=start,
        end=end,
        timeframe=TimeFrame(amount=1, unit=TimeFrameUnit.Minute),
        adjustment=Adjustment.ALL,
    )

    stock_bars = alpaca_client.get_stock_bars(request)

    return (
        pl.from_pandas(stock_bars.df.reset_index())
        .rename({"symbol": "ticker"})
        .select("timestamp", "ticker", "close")
    )


def get_benchmark_history_between_start_and_end(
    tickers: list[str], start: dt.date, end: dt.date
) -> pl.DataFrame:
    bear_lake_client = get_bear_lake_client()

    ext_open = dt.time(4, 0, 0, tzinfo=ZoneInfo("America/New_York"))
    ext_close = dt.time(20, 0, 0, tzinfo=ZoneInfo("America/New_York"))

    start = dt.datetime.combine(start, ext_open)
    end = dt.datetime.combine(end, ext_close)

    benchmark_history = bear_lake_client.query(
        bl.table("etf_history")
        .filter(
            pl.col("timestamp")
            .dt.convert_time_zone("America/New_York")
            .is_between(start, end),
            pl.col("ticker").is_in(tickers),
        )
        .select("timestamp", "ticker", "close")
    )

    return benchmark_history


def get_benchmark_history(
    period: Literal["1D", "5D", "1M", "6M", "1Y", "ALL"],
) -> pl.DataFrame:
    end = dt.datetime.now(ZoneInfo("America/New_York")) - dt.timedelta(
        days=1
    )  # yesterday
    month = 21
    year = 252
    tickers = ["SPY"]

    today = get_benchmark_history_for_today(tickers)

    match period:
        case "1D":
            return clean_benchmark_history(today)
        case "5D":
            start = end - dt.timedelta(days=5)
            interval = dt.timedelta(minutes=10)
            history = get_benchmark_history_between_start_and_end(tickers, start, end)
        case "1M":
            start = end - dt.timedelta(days=1 * month)
            interval = dt.timedelta(days=1)
            history = get_benchmark_history_between_start_and_end(tickers, start, end)
        case "6M":
            start = end - dt.timedelta(days=6 * month)
            interval = dt.timedelta(days=1)
            history = get_benchmark_history_between_start_and_end(tickers, start, end)
        case "1Y":
            start = end - dt.timedelta(days=1 * year)
            interval = dt.timedelta(days=1)
            history = get_benchmark_history_between_start_and_end(tickers, start, end)
        case "ALL":
            start = dt.date(2026, 1, 2)
            interval = dt.timedelta(days=1)
            history = get_benchmark_history_between_start_and_end(tickers, start, end)
        case _:
            raise ValueError(f"Period not supported: {period}")

    benchmark_history = pl.concat([history, today])

    benchmark_history_clean = clean_benchmark_history(benchmark_history)

    benchmark_history_agg = aggregate_benchmark_history(
        benchmark_history_clean, interval
    )

    return benchmark_history_agg

if __name__ == '__main__':
    print(
        get_benchmark_history('5D')
    )