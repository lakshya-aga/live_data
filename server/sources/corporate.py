"""
Corporate events / actions source.

Polls NSE and BSE corporate action APIs for:
  - Dividends, bonus issues, stock splits, rights issues, buy-backs
  - Board meeting / earnings result dates
  - AGM/EGM notices

NSE endpoints
-------------
  Upcoming corporate actions (30-day window):
    GET https://www.nseindia.com/api/corporateActions
        ?index=equities&from_date=DD-MM-YYYY&to_date=DD-MM-YYYY

  Board meetings:
    GET https://www.nseindia.com/api/boardMeetings
        ?index=equities&from_date=DD-MM-YYYY&to_date=DD-MM-YYYY

BSE endpoint
------------
  GET https://api.bseindia.com/BseIndiaAPI/api/DefaultData/w
      ?Lscrip=&Category=CA&scriptype=Q&strDate=DD/MM/YYYY&endDate=DD/MM/YYYY
"""

from __future__ import annotations

import asyncio
import logging
from datetime import date, datetime, timedelta, timezone

import aiohttp

from server.config import settings
from server.hub import Hub
from server.models.base import DataMessage, MessageType
from server.models.corporate import CorporateActionType, CorporateEvent

from .base import BaseSource

logger = logging.getLogger(__name__)

_NSE_ACTIONS = "https://www.nseindia.com/api/corporateActions"
_NSE_BOARD = "https://www.nseindia.com/api/boardMeetings"
_NSE_BASE = "https://www.nseindia.com"
_HEADERS = {
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 Chrome/124.0.0.0 Safari/537.36",
    "Accept": "application/json",
    "Referer": "https://www.nseindia.com/",
}

_ACTION_MAP: dict[str, CorporateActionType] = {
    "Dividend": CorporateActionType.DIVIDEND,
    "Bonus": CorporateActionType.BONUS,
    "Split": CorporateActionType.SPLIT,
    "Rights": CorporateActionType.RIGHTS,
    "Buy Back": CorporateActionType.BUYBACK,
    "Buy-Back": CorporateActionType.BUYBACK,
    "AGM": CorporateActionType.AGM,
    "EGM": CorporateActionType.EGM,
    "Merger": CorporateActionType.MERGER,
    "Demerger": CorporateActionType.DEMERGER,
}


def _classify(subject: str) -> CorporateActionType:
    for key, val in _ACTION_MAP.items():
        if key.lower() in subject.lower():
            return val
    return CorporateActionType.OTHER


class CorporateSource(BaseSource):
    name = "corporate"

    def __init__(self, hub: Hub) -> None:
        super().__init__(hub)
        self._seen: set[str] = set()

    async def _run(self) -> None:
        connector = aiohttp.TCPConnector(ssl=False)
        async with aiohttp.ClientSession(headers=_HEADERS, connector=connector) as session:
            self._session = session
            try:
                async with session.get(_NSE_BASE, timeout=aiohttp.ClientTimeout(total=10)) as r:
                    await r.read()
            except Exception:
                pass

            while True:
                await asyncio.gather(
                    self._poll_nse_actions(),
                    self._poll_nse_board_meetings(),
                    return_exceptions=True,
                )
                await asyncio.sleep(settings.corporate_events_poll_interval)

    async def _poll_nse_actions(self) -> None:
        today = date.today()
        window_end = today + timedelta(days=30)
        params = (
            f"?index=equities"
            f"&from_date={today.strftime('%d-%m-%Y')}"
            f"&to_date={window_end.strftime('%d-%m-%Y')}"
        )
        try:
            async with self._session.get(
                _NSE_ACTIONS + params, timeout=aiohttp.ClientTimeout(total=15)
            ) as r:
                data = await r.json(content_type=None)
        except Exception as exc:
            logger.debug("[corporate] NSE actions failed: %s", exc)
            return

        now = datetime.now(tz=timezone.utc)
        for item in data.get("data", []):
            key = item.get("symbol", "") + item.get("subject", "") + item.get("exDate", "")
            if key in self._seen:
                continue
            self._seen.add(key)

            symbol = (item.get("symbol") or "").upper()
            subject = item.get("subject", "")
            event_type = _classify(subject)

            ev = CorporateEvent(
                symbol=symbol,
                event_type=event_type,
                description=subject,
                ex_date=_parse_date(item.get("exDate")),
                record_date=_parse_date(item.get("recordDate")),
                payment_date=_parse_date(item.get("paymentDate")),
                announced_date=_parse_date(item.get("bcStartDate")),
            )
            # Extract dividend amount if present in subject, e.g. "Dividend - Rs 5 Per Share"
            if event_type == CorporateActionType.DIVIDEND:
                ev.dividend_amount = _extract_amount(subject)
                ev.dividend_type = _dividend_type(subject)

            await self.hub.publish(
                DataMessage(
                    type=MessageType.CORPORATE_EVENT,
                    source=self.name,
                    timestamp=now,
                    symbols=[symbol] if symbol else [],
                    data=ev.model_dump(mode="json"),
                )
            )

    async def _poll_nse_board_meetings(self) -> None:
        today = date.today()
        window_end = today + timedelta(days=30)
        params = (
            f"?index=equities"
            f"&from_date={today.strftime('%d-%m-%Y')}"
            f"&to_date={window_end.strftime('%d-%m-%Y')}"
        )
        try:
            async with self._session.get(
                _NSE_BOARD + params, timeout=aiohttp.ClientTimeout(total=15)
            ) as r:
                data = await r.json(content_type=None)
        except Exception as exc:
            logger.debug("[corporate] NSE board meetings failed: %s", exc)
            return

        now = datetime.now(tz=timezone.utc)
        for item in data.get("data", []):
            key = item.get("symbol", "") + item.get("meetingDate", "") + item.get("purposeofMeeting", "")
            if key in self._seen:
                continue
            self._seen.add(key)

            symbol = (item.get("symbol") or "").upper()
            purpose = item.get("purposeofMeeting", "")
            event_type = CorporateActionType.EARNINGS if "result" in purpose.lower() else CorporateActionType.OTHER

            ev = CorporateEvent(
                symbol=symbol,
                event_type=event_type,
                description=purpose,
                announced_date=_parse_date(item.get("meetingDate")),
                result_type=_result_quarter(purpose),
            )
            await self.hub.publish(
                DataMessage(
                    type=MessageType.CORPORATE_EVENT,
                    source=self.name,
                    timestamp=now,
                    symbols=[symbol] if symbol else [],
                    data=ev.model_dump(mode="json"),
                )
            )


def _parse_date(s: str | None) -> date | None:
    if not s:
        return None
    for fmt in ("%d-%b-%Y", "%d-%m-%Y", "%Y-%m-%d"):
        try:
            return datetime.strptime(s[:11].strip(), fmt).date()
        except Exception:
            pass
    return None


def _extract_amount(text: str) -> float | None:
    import re
    m = re.search(r"Rs\.?\s*([\d.]+)", text, re.IGNORECASE)
    if m:
        try:
            return float(m.group(1))
        except Exception:
            pass
    return None


def _dividend_type(text: str) -> str:
    t = text.lower()
    if "interim" in t:
        return "interim"
    if "special" in t:
        return "special"
    return "final"


def _result_quarter(text: str) -> str | None:
    import re
    m = re.search(r"\b(Q[1-4]|annual|half[- ]?year)\b", text, re.IGNORECASE)
    return m.group(0).upper() if m else None
