import os

import polars as pl
from benchmark_history import get_benchmark_history
from dotenv import load_dotenv
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from models import BenchmarkSnapshot, PortfolioSnapshot
from portfolio_history import get_portfolio_history

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


@app.get("/benchmark_history/{period}", response_model=list[BenchmarkSnapshot])
def benchmark_history(period: str):
    return get_benchmark_history(period).cast({"timestamp": pl.String}).to_dicts()
