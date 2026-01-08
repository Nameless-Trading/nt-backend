from pydantic import BaseModel


class PortfolioSnapshot(BaseModel):
    timestamp: str
    value: float
    return_: float
    cumulative_return: float
