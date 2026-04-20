from __future__ import annotations

from datetime import date
from pydantic import BaseModel


class QuarterlyResult(BaseModel):
    symbol: str
    exchange: str = "NSE"
    period: str           # "Q3FY25", "Q4FY25", etc.
    period_end: date
    revenue: float | None = None         # Total income (INR crore)
    ebitda: float | None = None
    ebit: float | None = None
    pbt: float | None = None             # Profit before tax
    pat: float | None = None             # Profit after tax (net profit)
    eps: float | None = None             # Earnings per share
    revenue_growth_yoy: float | None = None   # % YoY
    pat_growth_yoy: float | None = None
    # Estimates vs actuals
    revenue_estimate: float | None = None
    pat_estimate: float | None = None
    beat_miss: str | None = None         # "beat", "miss", "in-line"
    announced_at: date | None = None


class AnnualFinancials(BaseModel):
    symbol: str
    exchange: str = "NSE"
    fiscal_year: str      # "FY25"
    year_end: date
    revenue: float | None = None
    ebitda: float | None = None
    pat: float | None = None
    total_assets: float | None = None
    total_debt: float | None = None
    equity: float | None = None
    roe: float | None = None             # %
    roce: float | None = None            # %
    debt_to_equity: float | None = None
    current_ratio: float | None = None
    eps: float | None = None
    book_value_per_share: float | None = None
    pe_ratio: float | None = None        # Trailing
    pb_ratio: float | None = None


class KeyRatios(BaseModel):
    symbol: str
    pe: float | None = None
    pb: float | None = None
    ev_ebitda: float | None = None
    market_cap: float | None = None      # INR crore
    enterprise_value: float | None = None
    dividend_yield: float | None = None  # %
    face_value: float | None = None
    promoter_holding: float | None = None   # % shareholding
    fii_holding: float | None = None
    dii_holding: float | None = None
    public_holding: float | None = None
    as_of: date | None = None
