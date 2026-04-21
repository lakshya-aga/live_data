"""
News aggregator source.

Combines:
  1. NSE corporate announcements (exchange filings)
     GET https://www.nseindia.com/api/announcements?index=equities
  2. BSE corporate announcements
     GET https://api.bseindia.com/BseIndiaAPI/api/AnnSubCategoryGetData/w?...
  3. RSS feeds from financial news portals (ET, Moneycontrol, BS, etc.)
"""

from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timezone

import xml.etree.ElementTree as ET
from email.utils import parsedate_to_datetime as _rfc2822

import aiohttp

from server.config import settings
from server.hub import Hub
from server.models.base import DataMessage, MessageType
from server.models.news import NewsItem

from .base import BaseSource

logger = logging.getLogger(__name__)

_NSE_ANNOUNCEMENTS = "https://www.nseindia.com/api/announcements?index=equities&emerging=emerge"
_NSE_HOMEPAGE = "https://www.nseindia.com"
_BSE_ANNOUNCEMENTS = (
    "https://api.bseindia.com/BseIndiaAPI/api/AnnSubCategoryGetData/w"
    "?pageno=1&strCat=-1&strPrevDate=&strScrip=&strSearch=P&strToDate=&strType=C&subcategory=-1"
)
_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json, text/plain, */*",
    "Referer": "https://www.nseindia.com/",
}
_BSE_HEADERS = {
    "User-Agent": "Mozilla/5.0",
    "Origin": "https://www.bseindia.com",
    "Referer": "https://www.bseindia.com/",
}


class NewsSource(BaseSource):
    name = "news"

    def __init__(self, hub: Hub) -> None:
        super().__init__(hub)
        self._seen: set[str] = set()

    async def _run(self) -> None:
        connector = aiohttp.TCPConnector(ssl=False)
        async with aiohttp.ClientSession(headers=_HEADERS, connector=connector) as session:
            self._session = session
            # Seed NSE cookies
            try:
                async with session.get(_NSE_HOMEPAGE, timeout=aiohttp.ClientTimeout(total=10)) as r:
                    await r.read()
            except Exception:
                pass

            while True:
                await asyncio.gather(
                    self._poll_nse_announcements(),
                    self._poll_bse_announcements(),
                    self._poll_rss_feeds(),
                    return_exceptions=True,
                )
                await asyncio.sleep(settings.news_poll_interval)

    # ── NSE announcements ────────────────────────────────────────────────

    async def _poll_nse_announcements(self) -> None:
        try:
            async with self._session.get(
                _NSE_ANNOUNCEMENTS, timeout=aiohttp.ClientTimeout(total=15)
            ) as r:
                data = await r.json(content_type=None)
        except Exception as exc:
            logger.debug("[news] NSE announcements failed: %s", exc)
            return

        for item in data.get("data", []):
            key = item.get("an_dt", "") + item.get("desc", "")
            if key in self._seen:
                continue
            self._seen.add(key)

            sym = (item.get("symbol") or "").upper()
            ts_str = item.get("an_dt", "")
            try:
                ts = datetime.strptime(ts_str, "%d-%b-%Y %H:%M:%S").replace(tzinfo=timezone.utc)
            except Exception:
                ts = datetime.now(tz=timezone.utc)

            news = NewsItem(
                title=item.get("desc", ""),
                summary=item.get("attchmntText", ""),
                url=item.get("attchmntFile", ""),
                source="nse_announcement",
                symbols=[sym] if sym else [],
                published_at=ts,
                category="corporate",
            )
            await self.hub.publish(
                DataMessage(
                    type=MessageType.NEWS,
                    source=self.name,
                    timestamp=ts,
                    symbols=news.symbols,
                    data=news.model_dump(mode="json"),
                )
            )

    # ── BSE announcements ────────────────────────────────────────────────

    async def _poll_bse_announcements(self) -> None:
        try:
            async with self._session.get(
                _BSE_ANNOUNCEMENTS,
                headers=_BSE_HEADERS,
                timeout=aiohttp.ClientTimeout(total=15),
            ) as r:
                data = await r.json(content_type=None)
        except Exception as exc:
            logger.debug("[news] BSE announcements failed: %s", exc)
            return

        for item in data.get("Table", []):
            key = str(item.get("NewsID", "")) or item.get("HEADLINE", "")
            if key in self._seen:
                continue
            self._seen.add(key)

            sym = (item.get("SCRIP_CD") or item.get("short_name") or "").upper()
            ts_str = item.get("DT_TM", "")
            try:
                ts = datetime.strptime(ts_str[:19], "%Y-%m-%dT%H:%M:%S").replace(tzinfo=timezone.utc)
            except Exception:
                ts = datetime.now(tz=timezone.utc)

            news = NewsItem(
                title=item.get("HEADLINE", ""),
                summary=item.get("NEWSSUB", ""),
                url=f"https://www.bseindia.com/xml-data/corpfiling/AttachLive/{item.get('ATTACHMENTNAME', '')}",
                source="bse_announcement",
                symbols=[sym] if sym else [],
                published_at=ts,
                category="corporate",
            )
            await self.hub.publish(
                DataMessage(
                    type=MessageType.NEWS,
                    source=self.name,
                    timestamp=ts,
                    symbols=news.symbols,
                    data=news.model_dump(mode="json"),
                )
            )

    # ── RSS feeds ────────────────────────────────────────────────────────

    async def _poll_rss_feeds(self) -> None:
        for url in settings.rss_feeds:
            try:
                async with self._session.get(url, timeout=aiohttp.ClientTimeout(total=15)) as r:
                    content = await r.text()
                for entry in _parse_rss(content):
                    link = entry.get("link", "")
                    if link in self._seen:
                        continue
                    self._seen.add(link)

                    ts = datetime.now(tz=timezone.utc)
                    if entry.get("published"):
                        try:
                            ts = _rfc2822(entry["published"]).astimezone(timezone.utc)
                        except Exception:
                            pass

                    title = entry.get("title", "")
                    summary = entry.get("summary", "")
                    syms = _symbols_from_text(title + " " + summary)

                    news = NewsItem(
                        title=title,
                        summary=summary[:500],
                        url=link,
                        source=_feed_label(url),
                        symbols=syms,
                        published_at=ts,
                        category="macro",
                    )
                    await self.hub.publish(
                        DataMessage(
                            type=MessageType.NEWS,
                            source=self.name,
                            timestamp=ts,
                            symbols=syms,
                            data=news.model_dump(mode="json"),
                        )
                    )
            except Exception as exc:
                logger.debug("[news] RSS %s failed: %s", url, exc)


def _parse_rss(xml_text: str) -> list[dict]:
    """Parse RSS 2.0 / Atom feeds using stdlib ElementTree — no feedparser needed."""
    entries: list[dict] = []
    try:
        root = ET.fromstring(xml_text)
    except ET.ParseError:
        return entries

    ns = {"atom": "http://www.w3.org/2005/Atom"}

    # RSS 2.0
    for item in root.iter("item"):
        _t = lambda tag: (item.findtext(tag) or "").strip()  # noqa: E731
        entries.append({
            "title": _t("title"),
            "link": _t("link") or _t("guid"),
            "summary": _t("description"),
            "published": _t("pubDate"),
        })

    # Atom
    for entry in root.iter("{http://www.w3.org/2005/Atom}entry"):
        _t = lambda tag: (entry.findtext(tag, namespaces=ns) or "").strip()  # noqa: E731
        link_el = entry.find("{http://www.w3.org/2005/Atom}link")
        link = (link_el.get("href") if link_el is not None else "") or ""
        entries.append({
            "title": _t("{http://www.w3.org/2005/Atom}title"),
            "link": link,
            "summary": _t("{http://www.w3.org/2005/Atom}summary") or _t("{http://www.w3.org/2005/Atom}content"),
            "published": _t("{http://www.w3.org/2005/Atom}published") or _t("{http://www.w3.org/2005/Atom}updated"),
        })

    return entries


def _feed_label(url: str) -> str:
    mapping = {
        "economictimes": "et",
        "moneycontrol": "mc",
        "business-standard": "bs",
        "ndtvprofit": "ndtv",
        "thehindu": "hindu",
    }
    for key, label in mapping.items():
        if key in url:
            return label
    return "rss"


_KNOWN_SYMBOLS = set(settings.watchlist)


def _symbols_from_text(text: str) -> list[str]:
    import re
    candidates = re.findall(r"\b[A-Z]{2,20}\b", text)
    return list({c for c in candidates if c in _KNOWN_SYMBOLS})
