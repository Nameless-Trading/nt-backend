from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from portfolio_history import get_portfolio_history
from models import PortfolioSnapshot
import polars as pl
import os
from dotenv import load_dotenv

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
