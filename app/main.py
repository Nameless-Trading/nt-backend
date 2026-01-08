from fastapi import FastAPI
from portfolio_history import get_portfolio_history
from models import PortfolioSnapshot
import polars as pl

app = FastAPI()


@app.get("/portfolio_history/{period}", response_model=list[PortfolioSnapshot])
def portfolio_history(period: str):
    return get_portfolio_history(period).cast({'timestamp': pl.String}).to_dicts()
