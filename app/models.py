from pydantic import BaseModel


class PortfolioSnapshot(BaseModel):
    timestamp: str
    value: float
    return_: float
    cumulative_return: float
    return_dollar: float
    cumulative_return_dollar: float
    benchmark_cumulative_return: float | None = None


class PortfolioSummary(BaseModel):
    total_return: float
    total_return_dollar: float
    mean_return_ann: float
    volatility_ann: float
    sharpe: float
    benchmark_total_return: float | None = None
    benchmark_mean_return_ann: float | None = None
    benchmark_volatility_ann: float | None = None
    benchmark_sharpe: float | None = None
