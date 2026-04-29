from __future__ import annotations

from datetime import datetime
from pydantic import BaseModel


class GdeltArticle(BaseModel):
    url: str
    title: str
    seendate: datetime
    domain: str = ""
    language: str = ""
    # GDELT GKG tone: negative=negative sentiment, range roughly -10 to +10
    tone: float | None = None
    # Comma-separated GDELT themes present in the article
    themes: list[str] = []
    # Named entities: organisations, locations, persons
    organisations: list[str] = []
    locations: list[str] = []
    persons: list[str] = []
    # NSE symbols extracted from article (heuristic)
    symbols: list[str] = []
    image_url: str = ""
    # Provenance: which named GDELT query produced this article. The UI shows
    # the label as a badge and the raw query string in a tooltip so users can
    # see exactly what was searched.
    query_label: str = ""
    query_category: str = ""  # "country" | "stock" | "industry"
    query_string: str = ""


class GdeltToneTimeline(BaseModel):
    """Aggregated tone data for a query over time."""
    query: str
    # List of (datetime, avg_tone) pairs
    series: list[tuple[datetime, float]] = []
    timestamp: datetime
