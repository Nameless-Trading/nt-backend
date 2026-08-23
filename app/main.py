import datetime as dt
import os

import polars as pl
from dotenv import load_dotenv
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from live import get_portfolio_history, get_portfolio_summary
from models import (PortfolioSnapshot, PortfolioSummary, SecuritiesResponse)
from securities import get_available_dates, get_securities

load_dotenv()

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=[os.getenv("ALLOWED_ORIGIN")],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/portfolio_history/{period}", response_model=list[PortfolioSnapshot])
def portfolio_history(period: str):
    return get_portfolio_history(period).cast({"timestamp": pl.String}).to_dicts()


@app.get("/portfolio_summary/{period}", response_model=PortfolioSummary)
def portfolio_summary(period: str):
    return get_portfolio_summary(period)


@app.get("/securities/dates", response_model=list[str])
def securities_dates():
    return get_available_dates()


@app.get("/securities", response_model=SecuritiesResponse)
def securities(date: str | None = None):
    date_ = dt.date.fromisoformat(date) if date else dt.date.fromisoformat(
        get_available_dates()[0]
    )
    return get_securities(date_)
