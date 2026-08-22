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


def get_period_bounds(
    period: Literal["TODAY", "5D", "1M", "6M", "1Y", "ALL"],
) -> tuple[dt.date, dt.date, dt.date]:
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
            return dt.date(2026, 1, 2), dt.date(2025, 12, 31), yesterday

    start = get_last_market_dates(n=offset)[0]
    base_date = get_last_market_dates(n=offset + 1)[0]

    return start, base_date, yesterday


def get_equity(period: Literal["TODAY", "5D", "1M", "6M", "1Y", "ALL"]) -> pl.DataFrame:
    start, base_date, yesterday = get_period_bounds(period)

    equity_today = get_portfolio_history_for_today()
    equity_history = get_portfolio_history_between_start_and_end(start, yesterday)
    equity_base = get_portfolio_history_base(base_date)

    return pl.concat([equity_base, equity_history, equity_today])


def get_benchmark_returns(start: dt.date, end: dt.date) -> pl.DataFrame:
    bear_lake_client = get_bear_lake_client()

    benchmark_returns = bear_lake_client.query(
        bl.table("benchmark_returns").filter(pl.col("date").is_between(start, end))
    )

    return benchmark_returns.sort("date")


def add_benchmark_cumulative_return(
    returns: pl.DataFrame, benchmark_returns: pl.DataFrame
) -> pl.DataFrame:
    # Benchmark returns are daily; align them to each (possibly intraday) portfolio
    # snapshot by date and carry the last known value forward within a day.
    benchmark_cumulative = benchmark_returns.select(
        pl.col("date"),
        pl.col("return").add(1).cum_prod().sub(1).alias("benchmark_cumulative_return"),
    )

    return (
        returns.with_columns(pl.col("timestamp").dt.date().alias("date"))
        .join(benchmark_cumulative, on="date", how="left")
        .sort("timestamp")
        .with_columns(pl.col("benchmark_cumulative_return").forward_fill())
        .drop("date")
    )


def get_portfolio_history(
    period: Literal["TODAY", "5D", "1M", "6M", "1Y", "ALL"],
) -> pl.DataFrame:
    equity = get_equity(period)

    match period:
        case "TODAY":
            interval = dt.timedelta(minutes=1)
        case _:
            interval = dt.timedelta(days=1)

    returns = calculate_returns(equity, interval)

    # The benchmark is a daily series, so it is only meaningful for the
    # multi-day periods (everything except TODAY).
    if period == "TODAY":
        return returns

    start, _, yesterday = get_period_bounds(period)
    benchmark_returns = get_benchmark_returns(start, yesterday)

    return add_benchmark_cumulative_return(returns, benchmark_returns)


def annualized_metrics(
    returns: pl.Series, scale: float
) -> tuple[float | None, float | None, float | None]:
    # std() is None with fewer than two observations, which can happen when a
    # short window only has a day or two of data.
    mean = returns.mean()
    std = returns.std()

    mean_ann = mean * scale if mean is not None else None
    volatility_ann = std * (scale**0.5) if std is not None else None
    sharpe = mean_ann / volatility_ann if mean_ann is not None and volatility_ann else None

    return mean_ann, volatility_ann, sharpe


def get_portfolio_summary(
    period: Literal["TODAY", "5D", "1M", "6M", "1Y", "ALL"],
) -> dict:
    match period:
        case "TODAY":
            scale = 252 * 16 * 60  # minutes in an extended trading day
        case _:
            scale = 252  # 252 trading days in a year (daily returns)

    returns = get_portfolio_history(period)

    mean_return_ann, volatility_ann, sharpe = annualized_metrics(
        returns["return_"], scale
    )

    summary = {
        "total_return": returns["cumulative_return"].last(),
        "total_return_dollar": returns["cumulative_return_dollar"].last(),
        "mean_return_ann": mean_return_ann,
        "volatility_ann": volatility_ann,
        "sharpe": sharpe,
    }

    # Benchmark comparison for every period except the intraday TODAY view.
    if period != "TODAY":
        start, _, yesterday = get_period_bounds(period)
        benchmark_returns = get_benchmark_returns(start, yesterday)["return"]

        # Benchmark returns are always daily.
        b_mean_ann, b_volatility_ann, b_sharpe = annualized_metrics(
            benchmark_returns, 252
        )

        summary.update(
            {
                "benchmark_total_return": (benchmark_returns + 1).product() - 1
                if benchmark_returns.len()
                else None,
                "benchmark_mean_return_ann": b_mean_ann,
                "benchmark_volatility_ann": b_volatility_ann,
                "benchmark_sharpe": b_sharpe,
            }
        )

    return summary
