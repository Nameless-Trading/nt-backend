from pydantic import BaseModel


class PortfolioSnapshot(BaseModel):
    timestamp: str
    value: float
    return_: float
    cumulative_return: float
    return_dollar: float
    cumulative_return_dollar: float


class PortfolioSummary(BaseModel):
    total_return: float
    total_return_dollar: float
    mean_return_ann: float
    volatility_ann: float
    sharpe: float
