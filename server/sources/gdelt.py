"""
GDELT V2 data source — polls the Doc and TV APIs for India-relevant events.

GDELT is a free, public API.  No authentication required.

Endpoints
---------
  Article search:
    GET https://api.gdeltproject.org/api/v2/doc/doc
        ?query=<q>&mode=artlist&maxrecords=250&format=json

  Tone timeline (hourly tone for a query over the last 3 months):
    GET https://api.gdeltproject.org/api/v2/doc/doc
        ?query=<q>&mode=timelinetone&format=json

We query with:
  - "India stock market" filtered by GDELT themes in config
  - One query per configured theme so strategies can subscribe per-theme

Results are deduplicated by URL within a polling window.
"""

from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timezone
from urllib.parse import quote_plus

import aiohttp

from server.config import settings
from server.hub import Hub
from server.models.base import DataMessage, MessageType
from server.models.gdelt import GdeltArticle

from .base import BaseSource

logger = logging.getLogger(__name__)

_API = "https://api.gdeltproject.org/api/v2/doc/doc"
_DATE_FMT = "%Y%m%dT%H%M%SZ"


def _parse_date(s: str) -> datetime:
    try:
        return datetime.strptime(s, _DATE_FMT).replace(tzinfo=timezone.utc)
    except Exception:
        return datetime.now(tz=timezone.utc)


class GdeltSource(BaseSource):
    name = "gdelt"

    def __init__(self, hub: Hub) -> None:
        super().__init__(hub)
        self._seen: set[str] = set()

    async def _run(self) -> None:
        async with aiohttp.ClientSession() as session:
            self._session = session
            while True:
                await self._poll()
                await asyncio.sleep(settings.gdelt_poll_interval)

    async def _poll(self) -> None:
        # Build query: India + each configured theme
        base_query = "India stock market OR NSE OR BSE OR Nifty sourcegeography:India"
        theme_clause = " OR ".join(f"theme:{t}" for t in settings.gdelt_themes)
        query = f"({base_query}) ({theme_clause})"
        articles = await self._fetch_articles(query)

        new_seen: set[str] = set()
        for art in articles:
            new_seen.add(art.url)
            if art.url in self._seen:
                continue
            await self.hub.publish(
                DataMessage(
                    type=MessageType.GDELT_EVENT,
                    source=self.name,
                    timestamp=art.seendate,
                    symbols=art.symbols,
                    data=art.model_dump(mode="json"),
                )
            )

        # Keep only URLs seen in the last poll to avoid unbounded memory growth
        self._seen = new_seen
        logger.debug("[gdelt] polled %d articles (%d new)", len(articles), len(articles) - len(self._seen & new_seen))

    async def _fetch_articles(self, query: str) -> list[GdeltArticle]:
        params = {
            "query": query,
            "mode": "artlist",
            "maxrecords": "250",
            "format": "json",
            "sort": "DateDesc",
        }
        url = _API + "?" + "&".join(f"{k}={quote_plus(str(v))}" for k, v in params.items())
        try:
            async with self._session.get(url, timeout=aiohttp.ClientTimeout(total=30)) as r:
                data = await r.json(content_type=None)
        except Exception as exc:
            logger.warning("[gdelt] fetch failed: %s", exc)
            return []

        articles = []
        for item in data.get("articles", []):
            themes_raw: str = item.get("themes", "") or ""
            themes = [t.strip() for t in themes_raw.split(";") if t.strip()]

            orgs_raw: str = item.get("organizations", "") or ""
            orgs = [o.strip() for o in orgs_raw.split(";") if o.strip()]

            locs_raw: str = item.get("locations", "") or ""
            locs = [l.strip() for l in locs_raw.split(";") if l.strip()]

            persons_raw: str = item.get("persons", "") or ""
            persons = [p.strip() for p in persons_raw.split(";") if p.strip()]

            tone_raw = item.get("tone", "")
            tone: float | None = None
            if tone_raw:
                try:
                    tone = float(str(tone_raw).split(",")[0])
                except Exception:
                    pass

            syms = _extract_symbols(item.get("title", "") + " " + item.get("seentitle", ""))

            try:
                art = GdeltArticle(
                    url=item.get("url", ""),
                    title=item.get("title", ""),
                    seendate=_parse_date(item.get("seendate", "")),
                    domain=item.get("domain", ""),
                    language=item.get("language", ""),
                    tone=tone,
                    themes=themes,
                    organisations=orgs,
                    locations=locs,
                    persons=persons,
                    symbols=syms,
                    image_url=item.get("socialimage", ""),
                )
                articles.append(art)
            except Exception as exc:
                logger.debug("[gdelt] article parse error: %s", exc)

        return articles


# Heuristic: extract NSE symbols from text (all-caps words 2-20 chars)
_KNOWN_SYMBOLS = set(settings.watchlist)


def _extract_symbols(text: str) -> list[str]:
    import re
    candidates = re.findall(r"\b[A-Z]{2,20}\b", text)
    return list({c for c in candidates if c in _KNOWN_SYMBOLS})
