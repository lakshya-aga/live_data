from __future__ import annotations

from datetime import datetime
from pydantic import BaseModel


class NewsItem(BaseModel):
    title: str
    summary: str = ""
    url: str = ""
    source: str           # "nse_announcement", "bse_announcement", "et", "mc", etc.
    # Symbols mentioned or explicitly tagged (may be empty for macro news)
    symbols: list[str] = []
    published_at: datetime
    category: str = ""    # "corporate", "macro", "policy", "earnings", etc.
    sentiment: float | None = None   # -1.0 to +1.0, if computed
