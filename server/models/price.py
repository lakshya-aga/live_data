from __future__ import annotations

from datetime import datetime
from pydantic import BaseModel


class PriceTick(BaseModel):
    symbol: str           # NSE symbol, e.g. "RELIANCE"
    exchange: str = "NSE"
    ltp: float            # Last traded price (INR)
    change: float         # Absolute change from previous close
    change_pct: float     # Percentage change
    open: float
    high: float
    low: float
    prev_close: float
    volume: int
    avg_price: float | None = None
    bid: float | None = None
    ask: float | None = None
    bid_qty: int | None = None
    ask_qty: int | None = None
    total_buy_qty: int | None = None
    total_sell_qty: int | None = None
    oi: int | None = None         # Open interest (for F&O)
    oi_change: int | None = None
    trade_time: datetime | None = None
    week_52_high: float | None = None
    week_52_low: float | None = None


class OrderBookLevel(BaseModel):
    price: float
    quantity: int
    orders: int | None = None


class OrderBook(BaseModel):
    symbol: str
    exchange: str = "NSE"
    bids: list[OrderBookLevel]   # Best 5 bids, descending by price
    asks: list[OrderBookLevel]   # Best 5 asks, ascending by price
    timestamp: datetime


class IndexData(BaseModel):
    name: str             # "NIFTY 50", "NIFTY BANK", "SENSEX", etc.
    value: float
    change: float
    change_pct: float
    open: float | None = None
    high: float | None = None
    low: float | None = None
    advances: int | None = None
    declines: int | None = None
    unchanged: int | None = None
    timestamp: datetime


class MarketStatus(BaseModel):
    market: str           # "NSE", "BSE"
    # "pre_open", "open", "post_close", "closed", "holiday"
    status: str
    message: str = ""
    timestamp: datetime
