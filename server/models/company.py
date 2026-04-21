"""Models for corporate ownership and governance data."""
from __future__ import annotations

from datetime import date, datetime
from typing import Optional

from pydantic import BaseModel, Field


class ShareholdingPattern(BaseModel):
    symbol: str
    exchange: str = "NSE"
    quarter: str                    # e.g. "Dec 2024"
    as_of: datetime

    promoter_pct: float = 0.0
    promoter_pledged_pct: float = 0.0   # % of promoter shares that are pledged
    fii_pct: float = 0.0
    dii_pct: float = 0.0
    mutual_fund_pct: float = 0.0
    public_pct: float = 0.0
    total_shares: int = 0


class DirectorInfo(BaseModel):
    symbol: str
    exchange: str = "NSE"
    name: str
    designation: str = ""
    din: str = ""                   # DIN (Director Identification Number)
    appointment_date: Optional[date] = None
    cessation_date: Optional[date] = None
    is_active: bool = True


class DirectorChange(BaseModel):
    symbol: str
    exchange: str = "NSE"
    name: str
    designation: str = ""
    din: str = ""
    change_type: str                # "appointment" or "cessation"
    effective_date: Optional[date] = None
    announced_at: datetime = Field(default_factory=lambda: __import__("datetime").datetime.now(__import__("datetime").timezone.utc))
