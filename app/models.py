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
    total_return: float | None = None
    total_return_dollar: float | None = None
    mean_return_ann: float | None = None
    volatility_ann: float | None = None
    sharpe: float | None = None
    benchmark_total_return: float | None = None
    benchmark_mean_return_ann: float | None = None
    benchmark_volatility_ann: float | None = None
    benchmark_sharpe: float | None = None


class SignalMetrics(BaseModel):
    value: float | None = None
    score: float | None = None
    alpha: float | None = None


class SecurityRow(BaseModel):
    ticker: str
    price: float | None = None
    return_1d: float | None = None
    return_5d: float | None = None
    return_1m: float | None = None
    idio_vol: float | None = None
    historical_beta: float | None = None
    predicted_beta: float | None = None
    weight: float | None = None
    benchmark_weight: float | None = None
    signals: dict[str, SignalMetrics]


class SecuritiesResponse(BaseModel):
    date: str
    signals: list[str]
    rows: list[SecurityRow]
