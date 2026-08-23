import datetime as dt

import bear_lake as bl
import polars as pl
from clients import get_bear_lake_client

# Preferred column order for the signal groups in the securities table. Any signal
# not listed here is appended afterwards (alphabetically); "composite" is the
# blended production signal, so it reads best last.
SIGNAL_ORDER = ["momentum", "reversal", "composite"]

# Trading-day lookbacks for the trailing return columns.
RETURN_WINDOWS = {"return_1d": 1, "return_5d": 5, "return_1m": 21}

# Calendar days of returns to pull so the longest window always has enough
# trading days to compound over.
_RETURN_LOOKBACK_DAYS = 45


def get_available_dates() -> list[str]:
    """All dates for which the model produced a cross-section, most recent first."""
    client = get_bear_lake_client()
    dates = (
        client.query(bl.table("alphas").select("date").unique())["date"]
        .sort(descending=True)
        .to_list()
    )
    return [d.isoformat() for d in dates]


def _ordered_signals(signals: list[str]) -> list[str]:
    known = [s for s in SIGNAL_ORDER if s in signals]
    extra = sorted(s for s in signals if s not in SIGNAL_ORDER)
    return known + extra


def _pivot_signal_table(
    client, table: str, value_col: str, date_: dt.date
) -> pl.DataFrame:
    """Pivot a long (ticker, signal, <value>) table into one column per signal,
    suffixed with the metric name so it survives joins with the other tables."""
    df = client.query(
        bl.table(table)
        .filter(pl.col("date").eq(date_))
        .select("ticker", "signal", value_col)
    )
    if df.height == 0:
        return pl.DataFrame(schema={"ticker": pl.String})
    return df.pivot(values=value_col, index="ticker", on="signal").rename(
        lambda c: c if c == "ticker" else f"{c}__{value_col}"
    )


def get_securities(date_: dt.date) -> dict:
    """Assemble the per-security cross-section for a single date: price, trailing
    returns, per-signal value/score/alpha, idiosyncratic vol and portfolio weight."""
    client = get_bear_lake_client()

    universe = client.query(
        bl.table("universe").filter(pl.col("date").eq(date_)).select("ticker")
    )

    price = client.query(
        bl.table("stock_prices")
        .filter(pl.col("date").eq(date_))
        .select("ticker", pl.col("close").alias("price"))
    )
    idio_vol = client.query(
        bl.table("idio_vol")
        .filter(pl.col("date").eq(date_))
        .select("ticker", "idio_vol")
    )
    weight = client.query(
        bl.table("portfolio_weights")
        .filter(pl.col("date").eq(date_))
        .select("ticker", "weight")
    )

    returns = get_trailing_returns(client, date_)

    values = _pivot_signal_table(client, "signals", "value", date_)
    scores = _pivot_signal_table(client, "scores", "score", date_)
    alphas = _pivot_signal_table(client, "alphas", "alpha", date_)

    out = (
        universe.join(price, on="ticker", how="left")
        .join(returns, on="ticker", how="left")
        .join(idio_vol, on="ticker", how="left")
        .join(weight, on="ticker", how="left")
        .join(values, on="ticker", how="left")
        .join(scores, on="ticker", how="left")
        .join(alphas, on="ticker", how="left")
        .sort("ticker")
    )

    signal_names = sorted(
        {
            col.split("__", 1)[0]
            for col in out.columns
            if col.endswith(("__value", "__score", "__alpha"))
        }
    )
    signal_names = _ordered_signals(signal_names)

    rows = []
    for record in out.to_dicts():
        row = {
            "ticker": record["ticker"],
            "price": record.get("price"),
            "return_1d": record.get("return_1d"),
            "return_5d": record.get("return_5d"),
            "return_1m": record.get("return_1m"),
            "idio_vol": record.get("idio_vol"),
            "weight": record.get("weight"),
            "signals": {
                name: {
                    "value": record.get(f"{name}__value"),
                    "score": record.get(f"{name}__score"),
                    "alpha": record.get(f"{name}__alpha"),
                }
                for name in signal_names
            },
        }
        rows.append(row)

    return {"date": date_.isoformat(), "signals": signal_names, "rows": rows}


def get_trailing_returns(client, date_: dt.date) -> pl.DataFrame:
    start = date_ - dt.timedelta(days=_RETURN_LOOKBACK_DAYS)
    window = (
        client.query(
            bl.table("stock_returns")
            .filter(pl.col("date").is_between(start, date_))
            .select("date", "ticker", "return")
        )
        .sort("date")
    )

    # 1d is the return on the selected date itself; 5d/1m compound the trailing
    # window so a ticker with a stale last observation isn't mislabelled.
    one_day = window.filter(pl.col("date").eq(date_)).select(
        "ticker", pl.col("return").alias("return_1d")
    )

    compounded = window.group_by("ticker").agg(
        (pl.col("return").tail(5).add(1).product().sub(1)).alias("return_5d"),
        (pl.col("return").tail(21).add(1).product().sub(1)).alias("return_1m"),
    )

    return one_day.join(compounded, on="ticker", how="full", coalesce=True)
