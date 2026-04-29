"""
GDELT V2 DOC API source — runs a fixed catalogue of named queries.

Each poll cycle fires the full catalogue (4 countries + 5 stocks + 3 industries
= 12 queries) with ~2s spacing between calls, so we stay comfortably below
GDELT's published "fraction of a QPS" quota even on the cycle's burst.

Every article emitted carries provenance metadata (`query_label`,
`query_category`, `query_string`) so the UI can show *which named indicator*
triggered the article and *exactly what keywords* were searched. That is the
"show what keywords/queries were made" requirement.

GDELT DOC 2.0 query syntax notes (from official blog posts):
  • Use `sourcecountry:<FIPS>` — NOT `sourcegeography:`. FIPS country codes:
    India=IN, USA=US, China=CH, Japan=JA. ISO codes are wrong.
  • Bare common tokens (e.g. "India", "NSE") trip the "too short / too common"
    guard. Use quoted phrases inside `(... OR ...)` groups instead.
  • The API returns HTTP 200 with `text/html` for query-validation failures
    and quota errors, not JSON — we detect that and log the body verbatim.
"""

from __future__ import annotations

import asyncio
import json as _json
import logging
import re
from dataclasses import dataclass
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

# Spacing between successive query firings inside one poll cycle. GDELT's
# documented quota is "a fraction of a QPS"; 2s gives ~0.5 QPS with headroom.
_QUERY_SPACING = 2.0

# Common theme bundles, reused across query definitions.
_MACRO_THEMES = "(theme:ECON_STOCKMARKET OR theme:ECON_INTEREST_RATES OR theme:ECON_INFLATION OR theme:ECON_GDP)"
_EARNINGS_THEMES = "(theme:ECON_STOCKMARKET OR theme:ECON_EARNINGSREPORT OR theme:ECON_TRADE)"
_BANKING_THEMES = "(theme:ECON_STOCKMARKET OR theme:ECON_BANKRUPT OR theme:ECON_INTEREST_RATES)"


@dataclass(frozen=True)
class GdeltQuery:
    """One named GDELT search. The label is what the UI shows the user."""

    label: str          # human-readable, e.g. "India · Economic indicators"
    category: str       # "country" | "stock" | "industry"
    query: str          # raw GDELT DOC 2.0 query string


# ── Country economic indicators ─────────────────────────────────────────
_COUNTRY_QUERIES: list[GdeltQuery] = [
    GdeltQuery(
        label="India · Economic indicators",
        category="country",
        query=(
            '("India economy" OR "Indian economy" OR "Reserve Bank of India" '
            'OR "RBI policy" OR "Sensex" OR "Nifty 50") '
            f'{_MACRO_THEMES} sourcecountry:IN'
        ),
    ),
    GdeltQuery(
        label="US · Economic indicators",
        category="country",
        query=(
            '("US economy" OR "United States economy" OR "Federal Reserve" '
            'OR "Fed rate" OR "S&P 500" OR "Dow Jones") '
            f'{_MACRO_THEMES} sourcecountry:US'
        ),
    ),
    GdeltQuery(
        label="China · Economic indicators",
        category="country",
        query=(
            '("China economy" OR "Chinese economy" OR "PBOC" OR "Yuan" '
            'OR "Shanghai Composite" OR "CSI 300") '
            f'{_MACRO_THEMES} sourcecountry:CH'
        ),
    ),
    GdeltQuery(
        label="Japan · Economic indicators",
        category="country",
        query=(
            '("Japan economy" OR "Japanese economy" OR "Bank of Japan" '
            'OR "BOJ" OR "Nikkei 225" OR "Yen") '
            f'{_MACRO_THEMES} sourcecountry:JA'
        ),
    ),
]


# ── Top 5 Indian stocks ─────────────────────────────────────────────────
_STOCK_QUERIES: list[GdeltQuery] = [
    GdeltQuery(
        label="RELIANCE · Reliance Industries",
        category="stock",
        query=(
            '("Reliance Industries" OR "RIL stock" OR "Mukesh Ambani") '
            f'{_EARNINGS_THEMES}'
        ),
    ),
    GdeltQuery(
        label="TCS · Tata Consultancy Services",
        category="stock",
        query=(
            '("Tata Consultancy" OR "TCS earnings" OR "TCS results" OR "TCS share") '
            f'{_EARNINGS_THEMES}'
        ),
    ),
    GdeltQuery(
        label="INFY · Infosys",
        category="stock",
        query=(
            '("Infosys earnings" OR "Infosys results" OR "Infosys share" '
            'OR "Infosys revenue") '
            f'{_EARNINGS_THEMES}'
        ),
    ),
    GdeltQuery(
        label="HDFCBANK · HDFC Bank",
        category="stock",
        query=(
            '("HDFC Bank earnings" OR "HDFC Bank results" OR "HDFC Bank share" '
            'OR "HDFC Bank loan") '
            f'{_BANKING_THEMES}'
        ),
    ),
    GdeltQuery(
        label="ICICIBANK · ICICI Bank",
        category="stock",
        query=(
            '("ICICI Bank earnings" OR "ICICI Bank results" OR "ICICI Bank share" '
            'OR "ICICI Bank loan") '
            f'{_BANKING_THEMES}'
        ),
    ),
]


# ── 3 major industries ──────────────────────────────────────────────────
_INDUSTRY_QUERIES: list[GdeltQuery] = [
    GdeltQuery(
        label="Technology · Indian IT sector",
        category="industry",
        query=(
            '("Indian IT sector" OR "Indian technology stocks" OR "Indian software exports" '
            'OR "Nasscom") '
            f'{_EARNINGS_THEMES} sourcecountry:IN'
        ),
    ),
    GdeltQuery(
        label="Banking · Indian banks",
        category="industry",
        query=(
            '("Indian banking sector" OR "Indian banks" OR "Indian private banks" '
            'OR "Indian PSU banks") '
            f'{_BANKING_THEMES} sourcecountry:IN'
        ),
    ),
    GdeltQuery(
        label="Energy · Indian oil & gas",
        category="industry",
        query=(
            '("Indian oil and gas" OR "Indian energy sector" OR "ONGC" '
            'OR "Indian Oil Corporation" OR "Bharat Petroleum") '
            f'{_EARNINGS_THEMES} sourcecountry:IN'
        ),
    ),
]


GDELT_CATALOGUE: list[GdeltQuery] = _COUNTRY_QUERIES + _STOCK_QUERIES + _INDUSTRY_QUERIES


def _parse_date(s: str) -> datetime:
    try:
        return datetime.strptime(s, _DATE_FMT).replace(tzinfo=timezone.utc)
    except Exception:
        return datetime.now(tz=timezone.utc)


class GdeltSource(BaseSource):
    name = "gdelt"

    def __init__(self, hub: Hub) -> None:
        super().__init__(hub)
        # URL → first poll cycle in which we've seen it. We keep the seen-set
        # bounded by clearing it whenever it grows beyond a few thousand.
        self._seen: set[str] = set()

    async def _run(self) -> None:
        async with aiohttp.ClientSession() as session:
            self._session = session
            while True:
                await self._poll_all()
                await asyncio.sleep(settings.gdelt_poll_interval)

    async def _poll_all(self) -> None:
        # Bound the seen-set to avoid unbounded growth across long uptime.
        if len(self._seen) > 5000:
            self._seen.clear()

        total_new = 0
        for q in GDELT_CATALOGUE:
            articles = await self._fetch_articles(q)
            new_count = 0
            for art in articles:
                if art.url in self._seen:
                    continue
                self._seen.add(art.url)
                new_count += 1
                await self.hub.publish(
                    DataMessage(
                        type=MessageType.GDELT_EVENT,
                        source=self.name,
                        timestamp=art.seendate,
                        symbols=art.symbols,
                        data=art.model_dump(mode="json"),
                    )
                )
            total_new += new_count
            logger.debug("[gdelt] %s → %d articles (%d new)", q.label, len(articles), new_count)
            await asyncio.sleep(_QUERY_SPACING)

        logger.info("[gdelt] poll cycle complete: %d new articles across %d queries",
                    total_new, len(GDELT_CATALOGUE))

    async def _fetch_articles(self, q: GdeltQuery) -> list[GdeltArticle]:
        params = {
            "query": q.query,
            "mode": "artlist",
            "maxrecords": "75",
            "format": "json",
            "sort": "DateDesc",
        }
        url = _API + "?" + "&".join(f"{k}={quote_plus(str(v))}" for k, v in params.items())
        try:
            async with self._session.get(url, timeout=aiohttp.ClientTimeout(total=30)) as r:
                ct = (r.headers.get("content-type") or "").lower()
                body = await r.text()
                # GDELT returns HTTP 200 with text/html for query-validation
                # failures ("too common / too short keywords") and 429 for
                # quota exhaustion. Surface both at WARN so we see why a
                # given indicator went silent.
                if r.status == 429:
                    logger.warning("[gdelt] rate-limited on %s; body=%s", q.label, body[:200])
                    return []
                if "json" not in ct:
                    logger.warning(
                        "[gdelt] %s: non-JSON response (status=%s, ct=%s): %s",
                        q.label, r.status, ct, body[:240].replace("\n", " "),
                    )
                    return []
                data = _json.loads(body)
        except Exception as exc:
            logger.warning("[gdelt] %s: fetch failed: %s", q.label, exc)
            return []

        articles: list[GdeltArticle] = []
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

            syms = _extract_symbols(
                item.get("title", "") + " " + item.get("seentitle", "")
            )

            try:
                articles.append(GdeltArticle(
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
                    query_label=q.label,
                    query_category=q.category,
                    query_string=q.query,
                ))
            except Exception as exc:
                logger.debug("[gdelt] article parse error: %s", exc)

        return articles


# Heuristic: extract NSE symbols from text (all-caps words 2-20 chars)
_KNOWN_SYMBOLS = set(settings.watchlist)


def _extract_symbols(text: str) -> list[str]:
    candidates = re.findall(r"\b[A-Z]{2,20}\b", text)
    return list({c for c in candidates if c in _KNOWN_SYMBOLS})
