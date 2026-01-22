from pydantic import BaseModel


class PortfolioSnapshot(BaseModel):
    timestamp: str
    value: float
    return_: float
    cumulative_return: float
    return_dollar: float
    cumulative_return_dollar: float
