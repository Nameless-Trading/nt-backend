import datetime as dt
from zoneinfo import ZoneInfo

import pandas_market_calendars as mcal


def get_last_market_date() -> dt.date:
    nyse = mcal.get_calendar("NYSE")
    today = dt.datetime.now(ZoneInfo("America/New_York")).date()

    schedule = nyse.valid_days(
        start_date=today - dt.timedelta(days=10), end_date=today + dt.timedelta(days=10)
    )

    valid_dates = [d.date() for d in schedule if d.date() <= today]

    return valid_dates[-1]
