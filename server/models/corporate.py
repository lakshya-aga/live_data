from __future__ import annotations

from datetime import date
from enum import Enum
from pydantic import BaseModel


class CorporateActionType(str, Enum):
    DIVIDEND = "dividend"
    BONUS = "bonus"           # Bonus share issue
    SPLIT = "split"           # Stock split
    RIGHTS = "rights"         # Rights issue
    BUYBACK = "buyback"
    AGM = "agm"               # Annual general meeting
    EGM = "egm"
    EARNINGS = "earnings"     # Results announcement
    MERGER = "merger"
    DEMERGER = "demerger"
    SUSPENSION = "suspension"
    DELISTING = "delisting"
    OTHER = "other"


class CorporateEvent(BaseModel):
    symbol: str
    exchange: str = "NSE"
    event_type: CorporateActionType
    # Human-readable description, e.g. "Dividend - Rs 18/share"
    description: str = ""
    ex_date: date | None = None       # Ex-date (price adjusts)
    record_date: date | None = None
    payment_date: date | None = None
    announced_date: date | None = None
    # Type-specific fields
    dividend_amount: float | None = None    # INR per share
    dividend_type: str | None = None        # "interim", "final", "special"
    bonus_ratio: str | None = None          # e.g. "1:2" (1 share per 2 held)
    split_ratio: str | None = None          # e.g. "5:1" (split into 5)
    rights_ratio: str | None = None
    rights_price: float | None = None
    buyback_price: float | None = None
    buyback_size: float | None = None       # INR crore
    # Earnings-specific
    result_type: str | None = None          # "Q1", "Q2", "Q3", "Q4", "Annual"
    source_url: str = ""
