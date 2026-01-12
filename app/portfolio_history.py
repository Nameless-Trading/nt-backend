import polars as pl
import bear_lake as bl
from clients import get_bear_lake_client, get_alpaca_trading_client
import datetime as dt
from zoneinfo import ZoneInfo
from alpaca.trading.requests import GetPortfolioHistoryRequest
from models import PortfolioSnapshot
from typing import Literal
from zoneinfo import ZoneInfo
import pandas_market_calendars as mcal


def clean_portfolio_history(portfolio_history: pl.DataFrame) -> pl.DataFrame:
    return (
        portfolio_history.sort("timestamp")
        .with_columns(
            pl.col("timestamp").dt.convert_time_zone("America/New_York"),
            pl.col("daily_values").pct_change().fill_null(0).alias("return_"),
        )
        .with_columns(
            pl.col("return_").add(1).cum_prod().sub(1).alias("cumulative_return")
        )
        .select(
            "timestamp",
            pl.col("daily_values").alias("value"),
            pl.col("return_"),
            pl.col("cumulative_return"),
        )
    )


def aggregate_portfolio_history(
    portfolio_history: pl.DataFrame, interval: dt.timedelta
) -> pl.DataFrame:
    market_open = dt.time(7, 30, 0, tzinfo=ZoneInfo("America/New_York"))
    market_close = dt.time(16, 0, 0, tzinfo=ZoneInfo("America/New_York"))

    return (
        portfolio_history.filter(
            pl.col("timestamp").dt.time().is_between(market_open, market_close)
        )
        .sort("timestamp")
        .with_columns(pl.col("timestamp").dt.date().alias("date"))
        .group_by_dynamic("timestamp", every=interval)
        .agg(pl.col("value").last())
        .sort("timestamp")
        .with_columns(pl.col("value").pct_change().fill_null(0).alias("return_"))
        .with_columns(
            pl.col("return_").add(1).cum_prod().sub(1).alias("cumulative_return")
        )
        .select("timestamp", "value", "return_", "cumulative_return")
    )


def get_last_market_date() -> dt.date:
    nyse = mcal.get_calendar("NYSE")
    today = dt.datetime.now(ZoneInfo("America/New_York")).date()

    schedule = nyse.valid_days(
        start_date=today - dt.timedelta(days=10), end_date=today + dt.timedelta(days=10)
    )

    valid_dates = [d.date() for d in schedule if d.date() <= today]

    return valid_dates[-1]


def get_portfolio_history_for_today() -> pl.DataFrame:
    today = dt.datetime.now(ZoneInfo("America/New_York")).date()
    last_market_date = get_last_market_date()

    if today != last_market_date:
        return pl.DataFrame(
            schema={
                "timestamp": pl.Datetime(time_zone="UTC"),
                "daily_cumulative_return": pl.Float64,
                "daily_values": pl.Float64,
            }
        )

    ext_open = dt.time(4, 0, 0, tzinfo=ZoneInfo("America/New_York"))
    ext_close = dt.time(20, 0, 0, tzinfo=ZoneInfo("America/New_York"))

    start = dt.datetime.combine(today, ext_open)
    end = dt.datetime.combine(today, ext_close)

    alpaca_client = get_alpaca_trading_client()

    history_filter = GetPortfolioHistoryRequest(
        timeframe="1Min",  # Can only get 7 days of history.
        start=start,
        end=end,
        intraday_reporting="extended_hours",  # market_hours: 9:30am to 4pm ET. extended_hours: 4am to 8pm ET
        pnl_reset="per_day",
    )

    response = alpaca_client.get_portfolio_history(history_filter)

    portfolio_history = pl.DataFrame(
        {
            "timestamp": response.timestamp,
            "daily_cumulative_return": response.profit_loss_pct,
            "daily_values": response.base_value,
        }
    ).with_columns(
        pl.from_epoch("timestamp").dt.convert_time_zone("UTC"),
        pl.col("daily_values").mul(pl.col("daily_cumulative_return").add(1)),
    )

    return portfolio_history


def get_portfolio_history_between_start_and_end(
    start: dt.date, end: dt.date
) -> list[PortfolioSnapshot]:
    bear_lake_client = get_bear_lake_client()

    ext_open = dt.time(4, 0, 0, tzinfo=ZoneInfo("America/New_York"))
    ext_close = dt.time(20, 0, 0, tzinfo=ZoneInfo("America/New_York"))

    start = dt.datetime.combine(start, ext_open)
    end = dt.datetime.combine(end, ext_close)

    portfolio_history = bear_lake_client.query(
        bl.table("portfolio_history").filter(
            pl.col("timestamp")
            .dt.convert_time_zone("America/New_York")
            .is_between(start, end)
        )
    )

    return portfolio_history


def get_portfolio_history(
    period: Literal["1D", "5D", "1M", "6M", "1Y", "ALL"],
) -> pl.DataFrame:
    end = dt.datetime.now(ZoneInfo("America/New_York")) - dt.timedelta(
        days=1
    )  # yesterday
    month = 21
    year = 252

    today = get_portfolio_history_for_today()

    match period:
        case "1D":
            return clean_portfolio_history(today)
        case "5D":
            start = end - dt.timedelta(days=5)
            interval = dt.timedelta(minutes=10)
            history = get_portfolio_history_between_start_and_end(start, end)
        case "1M":
            start = end - dt.timedelta(days=1 * month)
            interval = dt.timedelta(days=1)
            history = get_portfolio_history_between_start_and_end(start, end)
        case "6M":
            start = end - dt.timedelta(days=6 * month)
            interval = dt.timedelta(days=1)
            history = get_portfolio_history_between_start_and_end(start, end)
        case "1Y":
            start = end - dt.timedelta(days=1 * year)
            interval = dt.timedelta(days=1)
            history = get_portfolio_history_between_start_and_end(start, end)
        case "ALL":
            start = dt.date(2026, 1, 2)
            interval = dt.timedelta(days=1)
            history = get_portfolio_history_between_start_and_end(start, end)
        case _:
            raise ValueError(f"Period not supported: {period}")

    portfolio_history = pl.concat([history, today])

    portfolio_history_clean = clean_portfolio_history(portfolio_history)

    portfolio_history_agg = aggregate_portfolio_history(
        portfolio_history_clean, interval
    )

    return portfolio_history_agg

if __name__ == '__main__':
    result = get_portfolio_history("1D")
    print(result)