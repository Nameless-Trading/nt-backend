import datetime as dt
from typing import Literal
from zoneinfo import ZoneInfo

import bear_lake as bl
import polars as pl
from alpaca.trading.requests import GetPortfolioHistoryRequest
from clients import get_alpaca_trading_client, get_bear_lake_client
from utils import get_last_market_date, get_last_market_dates


def get_portfolio_history_for_today() -> pl.DataFrame:
    today = dt.datetime.now(ZoneInfo("America/New_York")).date()
    last_market_date = get_last_market_date()

    if today != last_market_date:
        return pl.DataFrame(
            schema={
                "timestamp": pl.Datetime(time_zone="UTC"),
                "equity": pl.Float64,
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
            "equity": response.equity,
        }
    ).with_columns(
        pl.from_epoch("timestamp").dt.convert_time_zone("UTC"),
    )

    return portfolio_history


def get_portfolio_history_between_start_and_end(
    start: dt.date, end: dt.date
) -> pl.DataFrame:
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


def get_portfolio_history_base(base_date: dt.date) -> pl.DataFrame:
    bear_lake_client = get_bear_lake_client()

    market_open = dt.time(7, 30, 0, tzinfo=ZoneInfo("America/New_York"))
    market_close = dt.time(16, 0, 0, tzinfo=ZoneInfo("America/New_York"))

    start = dt.datetime.combine(base_date, market_open)
    end = dt.datetime.combine(base_date, market_close)

    portfolio_history = bear_lake_client.query(
        bl.table("portfolio_history")
        .filter(
            pl.col("timestamp")
            .dt.convert_time_zone("America/New_York")
            .is_between(start, end)
        )
        .filter(pl.col("timestamp").eq(pl.col("timestamp").max()))
    )

    return portfolio_history


def calculate_returns(equity: pl.DataFrame, interval: dt.timedelta) -> pl.DataFrame:
    return (
        equity.sort("timestamp")
        .with_columns(pl.col("timestamp").dt.convert_time_zone("America/New_York"))
        .with_columns(pl.col("equity").pct_change().alias("return_"))
        .with_columns(
            pl.col("return_").add(1).cum_prod().sub(1).alias("cumulative_return")
        )
        .with_columns(
            pl.col("return_").mul(pl.col("equity").first()).alias("return_dollar"),
            pl.col("cumulative_return")
            .mul(pl.col("equity").first())
            .alias("cumulative_return_dollar"),
        )
        .drop_nulls("return_")
        .sort("timestamp")
        .group_by_dynamic(index_column="timestamp", every=interval)
        .agg(
            pl.col("equity").last().alias("value"),
            pl.col("return_").add(1).product().sub(1),
            pl.col("cumulative_return").last(),
            pl.col("return_dollar").last(),
            pl.col("cumulative_return_dollar").last(),
        )
        .sort("timestamp")
    )


def get_equity(period: Literal["TODAY", "5D", "1M", "6M", "1Y", "ALL"]) -> pl.DataFrame:
    timezone = ZoneInfo("America/New_York")
    yesterday = (dt.datetime.now(timezone) - dt.timedelta(days=1)).date()

    match period:
        case "TODAY":
            offset = 1
        case "5D":
            offset = 5
        case "1M":
            offset = 21
        case "6M":
            offset = 21 * 6
        case "1Y":
            offset = 252
        case "ALL":
            start = dt.date(2026, 1, 2)
            base_date = dt.date(2025, 12, 31)

    if period != "ALL":
        start = get_last_market_dates(n=offset)[0]
        base_date = get_last_market_dates(n=offset + 1)[0]

    equity_today = get_portfolio_history_for_today()
    equity_history = get_portfolio_history_between_start_and_end(start, yesterday)
    equity_base = get_portfolio_history_base(base_date)

    return pl.concat([equity_base, equity_history, equity_today])


def get_portfolio_history(
    period: Literal["TODAY", "5D", "1M", "6M", "1Y", "ALL"],
) -> pl.DataFrame:
    equity = get_equity(period)

    match period:
        case "TODAY":
            interval = dt.timedelta(minutes=1)
        case "5D" | "1M":
            interval = dt.timedelta(hours=1)
        case "6M" | "1Y" | "ALL":
            interval = dt.timedelta(days=1)

    return calculate_returns(equity, interval)


def get_portfolio_summary(
    period: Literal["TODAY", "5D", "1M", "6M", "1Y", "ALL"],
) -> dict:
    match period:
        case "TODAY":
            scale = 252 * 16 * 60  # 60 minutes in an hour
        case "5D" | "1M":
            scale = 252 * 16  # 11 hours in extended trading day 4am-8pm
        case "6M" | "1Y" | "ALL":
            scale = 252  # 252 trading days in a year

    returns = get_portfolio_history(period)

    total_return = returns["cumulative_return"].last()
    total_return_dollar = returns["cumulative_return_dollar"].last()
    mean_return_ann = returns["return_"].mean() * scale
    volatility_ann = returns["return_"].std() * (scale**0.5)
    sharpe = mean_return_ann / volatility_ann

    summary = {
        "total_return": total_return,
        "total_return_dollar": total_return_dollar,
        "mean_return_ann": mean_return_ann,
        "volatility_ann": volatility_ann,
        "sharpe": sharpe,
    }

    return summary
