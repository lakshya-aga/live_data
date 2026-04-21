"""
Company governance source.

Polls NSE for:
  1. Shareholding pattern (quarterly filings — promoter %, FII %, DII %, pledging %)
     GET https://www.nseindia.com/api/corporate-shareholding-pattern?symbol={sym}&series=EQ

  2. Board of directors / director changes
     GET https://www.nseindia.com/api/companyDirectors?symbol={sym}&series=EQ
"""

from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timezone
from typing import Any

import aiohttp

from server.config import settings
from server.hub import Hub
from server.models.base import DataMessage, MessageType
from server.models.company import DirectorChange, ShareholdingPattern

from .base import BaseSource

logger = logging.getLogger(__name__)

_NSE_HOME = "https://www.nseindia.com"
_NSE_SHAREHOLDING = "https://www.nseindia.com/api/corporate-shareholding-pattern?symbol={sym}&series=EQ"
_NSE_DIRECTORS = "https://www.nseindia.com/api/companyDirectors?symbol={sym}&series=EQ"

_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json, text/plain, */*",
    "Referer": "https://www.nseindia.com/",
}

# NSE category strings → our field names
_PROMOTER_KEYS = {"Promoter & Promoter Group", "Promoters", "Promoter Group"}
_FII_KEYS = {"Foreign Institutional Investors", "FIIs", "FPI"}
_DII_KEYS = {"Domestic Institutional Investors", "Mutual Funds / UTI", "DIIs"}
_MF_KEYS = {"Mutual Funds / UTI", "Mutual Funds", "UTI"}
_PUBLIC_KEYS = {"Public", "Non-Institutions"}


def _now() -> datetime:
    return datetime.now(tz=timezone.utc)


class CompanySource(BaseSource):
    name = "company"

    def __init__(self, hub: Hub) -> None:
        super().__init__(hub)
        # Track which quarters we've already published to avoid duplicate events
        self._seen_quarters: dict[str, str] = {}
        # Track known directors per symbol to detect changes
        self._known_directors: dict[str, set[str]] = {}

    async def _run(self) -> None:
        connector = aiohttp.TCPConnector(ssl=False)
        async with aiohttp.ClientSession(headers=_HEADERS, connector=connector) as session:
            self._session = session
            # Seed NSE session cookie
            try:
                async with session.get(_NSE_HOME, timeout=aiohttp.ClientTimeout(total=10)) as r:
                    await r.read()
            except Exception:
                pass

            await asyncio.gather(
                self._loop_shareholding(),
                self._loop_directors(),
            )

    # ── Shareholding pattern ─────────────────────────────────────────────

    async def _loop_shareholding(self) -> None:
        while True:
            for sym in settings.watchlist:
                await self._poll_shareholding(sym)
                await asyncio.sleep(0.2)   # gentle rate-limit between symbols
            await asyncio.sleep(settings.shareholding_poll_interval)

    async def _poll_shareholding(self, sym: str) -> None:
        url = _NSE_SHAREHOLDING.format(sym=sym)
        try:
            async with self._session.get(url, timeout=aiohttp.ClientTimeout(total=15)) as r:
                if r.status != 200:
                    return
                data: dict[str, Any] = await r.json(content_type=None)
        except Exception as exc:
            logger.debug("[company] shareholding %s failed: %s", sym, exc)
            return

        # NSE returns an array of quarterly snapshots; use the most recent
        records = data.get("data", [])
        if not records:
            return

        latest = records[0]
        quarter = latest.get("date", "")

        cache_key = f"{sym}:{quarter}"
        if self._seen_quarters.get(sym) == quarter:
            return   # already published this quarter

        self._seen_quarters[sym] = quarter
        pattern = _parse_shareholding(sym, quarter, latest)
        if pattern is None:
            return

        await self.hub.publish(
            DataMessage(
                type=MessageType.SHAREHOLDING_PATTERN,
                source=self.name,
                timestamp=_now(),
                symbols=[sym],
                data=pattern.model_dump(mode="json"),
            )
        )

    # ── Board of directors ───────────────────────────────────────────────

    async def _loop_directors(self) -> None:
        while True:
            for sym in settings.watchlist:
                await self._poll_directors(sym)
                await asyncio.sleep(0.3)
            await asyncio.sleep(settings.board_poll_interval)

    async def _poll_directors(self, sym: str) -> None:
        url = _NSE_DIRECTORS.format(sym=sym)
        try:
            async with self._session.get(url, timeout=aiohttp.ClientTimeout(total=15)) as r:
                if r.status != 200:
                    return
                data: dict[str, Any] = await r.json(content_type=None)
        except Exception as exc:
            logger.debug("[company] directors %s failed: %s", sym, exc)
            return

        directors = data.get("data", [])
        if not directors:
            return

        current_dins = set()
        for d in directors:
            din = str(d.get("din", d.get("DIN", ""))).strip()
            name = (d.get("name") or d.get("Name") or "").strip()
            if not name:
                continue
            key = din or name
            current_dins.add(key)

        prev = self._known_directors.get(sym)
        if prev is None:
            # First poll — record state, don't fire events
            self._known_directors[sym] = current_dins
            return

        # Detect new appointments
        for d in directors:
            din = str(d.get("din", d.get("DIN", ""))).strip()
            name = (d.get("name") or d.get("Name") or "").strip()
            if not name:
                continue
            key = din or name
            if key not in prev:
                change = DirectorChange(
                    symbol=sym,
                    name=name,
                    designation=(d.get("designation") or d.get("Designation") or "").strip(),
                    din=din,
                    change_type="appointment",
                    announced_at=_now(),
                )
                await self.hub.publish(
                    DataMessage(
                        type=MessageType.DIRECTOR_CHANGE,
                        source=self.name,
                        timestamp=_now(),
                        symbols=[sym],
                        data=change.model_dump(mode="json"),
                    )
                )

        # Detect cessations (DINs no longer present)
        for key in prev - current_dins:
            change = DirectorChange(
                symbol=sym,
                name=key,
                change_type="cessation",
                announced_at=_now(),
            )
            await self.hub.publish(
                DataMessage(
                    type=MessageType.DIRECTOR_CHANGE,
                    source=self.name,
                    timestamp=_now(),
                    symbols=[sym],
                    data=change.model_dump(mode="json"),
                )
            )

        self._known_directors[sym] = current_dins


# ── Parsing helpers ──────────────────────────────────────────────────────────

def _pct(val: Any) -> float:
    try:
        return round(float(val), 4)
    except (TypeError, ValueError):
        return 0.0


def _parse_shareholding(sym: str, quarter: str, record: dict) -> ShareholdingPattern | None:
    """
    NSE /api/corporate-shareholding-pattern returns a shareHoldingData array.
    Each entry has shareHolderType and shareHoldingPercentage.
    Promoter pledging comes from the promoterAndPromoterGroupDetails block.
    """
    rows: list[dict] = record.get("shareHoldingData", [])
    if not rows:
        return None

    promoter_pct = fii_pct = dii_pct = mf_pct = public_pct = 0.0
    total_shares = 0

    for row in rows:
        cat = (row.get("shareHolderType") or "").strip()
        pct = _pct(row.get("shareHoldingPercentage", 0))
        shares = int(row.get("noOfShares", 0) or 0)
        total_shares = max(total_shares, shares)

        if cat in _PROMOTER_KEYS:
            promoter_pct += pct
        if cat in _FII_KEYS:
            fii_pct += pct
        if cat in _DII_KEYS:
            dii_pct += pct
        if cat in _MF_KEYS:
            mf_pct += pct
        if cat in _PUBLIC_KEYS:
            public_pct += pct

    # Promoter pledging — nested field when present
    pledged_pct = 0.0
    pledged_raw = record.get("promoterAndPromoterGroupDetails", {})
    if pledged_raw:
        pledged_pct = _pct(pledged_raw.get("pledgePercentage", 0))

    return ShareholdingPattern(
        symbol=sym,
        quarter=quarter,
        as_of=_now(),
        promoter_pct=promoter_pct,
        promoter_pledged_pct=pledged_pct,
        fii_pct=fii_pct,
        dii_pct=dii_pct,
        mutual_fund_pct=mf_pct,
        public_pct=public_pct,
        total_shares=total_shares,
    )
