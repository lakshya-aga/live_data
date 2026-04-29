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
    # Query-level sentiment aggregates from GDELT's tonechart endpoint
    # (mode=tonechart). artlist mode does not return a per-article tone, so
    # the frontend uses these fields — every article in a given query carries
    # the same group-level numbers, and the dashboard reads the latest one.
    query_total: int = 0
    query_positive: int = 0    # bin > 0
    query_neutral: int = 0     # bin == 0
    query_negative: int = 0    # bin < 0


class GdeltToneTimeline(BaseModel):
    """Aggregated tone data for a query over time."""
    query: str
    # List of (datetime, avg_tone) pairs
    series: list[tuple[datetime, float]] = []
    timestamp: datetime
