"""
NSE public data source — polls market status + indices.

Price ticks come exclusively from Groww (server.sources.groww). NSE has
been removed from the price path entirely; this module only emits
MarketStatus and IndexData messages.

NSE's website requires a valid session cookie before any API call will
return JSON instead of a redirect. We obtain the cookie by hitting the
homepage once, then reuse it for subsequent calls. The cookie is
refreshed every 30 minutes to avoid expiry.

Endpoints used
--------------
  GET /api/market-status               → MarketStatus
  GET /api/allIndices                  → list[IndexData]
"""

from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timezone

import aiohttp

from server.config import settings
from server.hub import Hub
from server.models.base import DataMessage, MessageType
from server.models.price import IndexData, MarketStatus

from .base import BaseSource

logger = logging.getLogger(__name__)

_BASE = "https://www.nseindia.com"
_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (X11; Linux x86_64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": "https://www.nseindia.com/",
}
_COOKIE_REFRESH_INTERVAL = 1800  # seconds


class NSESource(BaseSource):
    name = "nse"

    def __init__(self, hub: Hub) -> None:
        super().__init__(hub)
        self._session: aiohttp.ClientSession | None = None
        self._cookie_ts: float = 0.0

    async def _run(self) -> None:
        connector = aiohttp.TCPConnector(ssl=False)
        async with aiohttp.ClientSession(headers=_HEADERS, connector=connector) as session:
            self._session = session
            await self._refresh_cookies()
            while True:
                # Price ticks come from Groww only — NSE polls market status
                # and indices but never publishes price_tick messages, even
                # when Groww auth is unavailable.
                await asyncio.gather(
                    self._poll_market_status(),
                    self._poll_indices(),
                )
                await asyncio.sleep(settings.nse_poll_interval)
                import time
                if time.monotonic() - self._cookie_ts > _COOKIE_REFRESH_INTERVAL:
                    await self._refresh_cookies()

    async def _refresh_cookies(self) -> None:
        import time
        try:
            async with self._session.get(_BASE, timeout=aiohttp.ClientTimeout(total=10)) as r:
                await r.read()
            self._cookie_ts = time.monotonic()
            logger.debug("[nse] cookies refreshed")
        except Exception as exc:
            logger.warning("[nse] cookie refresh failed: %s", exc)

    async def _get(self, path: str) -> dict | list | None:
        url = f"{_BASE}{path}"
        try:
            async with self._session.get(url, timeout=aiohttp.ClientTimeout(total=10)) as r:
                if r.content_type == "application/json":
                    return await r.json()
                text = await r.text()
                import json
                return json.loads(text)
        except Exception as exc:
            logger.debug("[nse] GET %s failed: %s", path, exc)
            return None

    async def _poll_market_status(self) -> None:
        data = await self._get("/api/market-status")
        if not data:
            return
        now = datetime.now(tz=timezone.utc)
        # NSE returns a list of market segments; pick equities
        markets = data.get("marketState", [])
        for m in markets:
            if m.get("market") in ("Capital Market", "CM"):
                status_str = m.get("marketStatus", "").lower()
                status = "open" if "open" in status_str else "closed"
                ms = MarketStatus(
                    market="NSE",
                    status=status,
                    message=m.get("tradeDate", ""),
                    timestamp=now,
                )
                await self.hub.publish(
                    DataMessage(
                        type=MessageType.MARKET_STATUS,
                        source=self.name,
                        timestamp=now,
                        data=ms.model_dump(mode="json"),
                    )
                )
                break

    async def _poll_indices(self) -> None:
        data = await self._get("/api/allIndices")
        if not data:
            return
        now = datetime.now(tz=timezone.utc)
        # Build a normalised lookup of allowed names so "NIFTY 50" / "Nifty50"
        # / "NIFTY50" all match the same whitelist entry.
        allowed = {
            "".join(s.split()).upper() for s in settings.indices_watchlist
        }
        for entry in data.get("data", []):
            name = entry.get("index", "")
            if "".join(name.split()).upper() not in allowed:
                continue
            try:
                idx = IndexData(
                    name=entry["index"],
                    value=float(entry.get("last", 0)),
                    change=float(entry.get("variation", 0)),
                    change_pct=float(entry.get("percentChange", 0)),
                    open=float(entry.get("open", 0)) or None,
                    high=float(entry.get("high", 0)) or None,
                    low=float(entry.get("low", 0)) or None,
                    advances=int(entry["advances"]) if entry.get("advances") else None,
                    declines=int(entry["declines"]) if entry.get("declines") else None,
                    unchanged=int(entry["unchanged"]) if entry.get("unchanged") else None,
                    timestamp=now,
                )
                await self.hub.publish(
                    DataMessage(
                        type=MessageType.INDEX_DATA,
                        source=self.name,
                        timestamp=now,
                        symbols=[entry["index"].replace(" ", "_")],
                        data=idx.model_dump(mode="json"),
                    )
                )
            except Exception as exc:
                logger.debug("[nse] index parse error: %s", exc)

