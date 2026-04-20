from __future__ import annotations

from datetime import datetime
from enum import Enum
from typing import Any, Literal

import orjson
from pydantic import BaseModel, Field


class MessageType(str, Enum):
    PRICE_TICK = "price_tick"
    ORDER_BOOK = "order_book"
    INDEX_DATA = "index_data"
    MARKET_STATUS = "market_status"
    NEWS = "news"
    GDELT_EVENT = "gdelt_event"
    FINANCIAL_STATEMENT = "financial_statement"
    CORPORATE_EVENT = "corporate_event"
    INSIDER_TRADE = "insider_trade"
    BULK_DEAL = "bulk_deal"
    BLOCK_DEAL = "block_deal"
    POLITICIAN_DISCLOSURE = "politician_disclosure"
    HEARTBEAT = "heartbeat"


class DataMessage(BaseModel):
    type: MessageType
    source: str
    timestamp: datetime
    # symbols this message relates to (empty = no symbol filter applies)
    symbols: list[str] = Field(default_factory=list)
    data: Any

    def to_json(self) -> bytes:
        return orjson.dumps(self.model_dump(mode="json"))


class SubscribeMessage(BaseModel):
    action: Literal["subscribe", "unsubscribe", "ping"]
    channels: list[MessageType] = Field(default_factory=list)
    # Empty symbols list means "all symbols"
    symbols: list[str] = Field(default_factory=list)


class ServerResponse(BaseModel):
    type: Literal["ack", "error", "pong", "info"]
    message: str = ""

    def to_json(self) -> bytes:
        return orjson.dumps(self.model_dump())
