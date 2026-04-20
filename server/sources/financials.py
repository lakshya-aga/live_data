"""
Financial statements source.

Data paths
----------
1. NSE quarterly results feed:
   GET https://www.nseindia.com/api/results-comparision?index=equities&symbol={sym}

2. NSE key ratios / share-holding pattern:
   GET https://www.nseindia.com/api/quote-equity?symbol={sym}&section=fundamentals

3. Screener.in JSON (if SCREENER_API_KEY is set):
   GET https://www.screener.in/api/company/{symbol}/?format=json
   Authorization: Token <api_key>

All sources are polled at FINANCIALS_POLL_INTERVAL (default 1 hour).
"""

from __future__ import annotations

import asyncio
import logging
from datetime import date, datetime, timezone

import aiohttp

from server.config import settings
from server.hub import Hub
from server.models.base import DataMessage, MessageType
from server.models.financials import KeyRatios, QuarterlyResult

from .base import BaseSource

logger = logging.getLogger(__name__)

_NSE_BASE = "https://www.nseindia.com"
_SCREENER_BASE = "https://www.screener.in"
_HEADERS = {
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 Chrome/124.0.0.0 Safari/537.36",
    "Accept": "application/json",
    "Referer": "https://www.nseindia.com/",
}


class FinancialsSource(BaseSource):
    name = "financials"

    async def _run(self) -> None:
        connector = aiohttp.TCPConnector(ssl=False)
        async with aiohttp.ClientSession(headers=_HEADERS, connector=connector) as session:
            self._session = session
            # Seed NSE session cookies
            try:
                async with session.get(_NSE_BASE, timeout=aiohttp.ClientTimeout(total=10)) as r:
                    await r.read()
            except Exception:
                pass

            while True:
                for symbol in settings.watchlist:
                    await self._fetch_nse_fundamentals(symbol)
                    if settings.screener_api_key:
                        await self._fetch_screener(symbol)
                    await asyncio.sleep(0.5)   # rate-limit courtesy
                await asyncio.sleep(settings.financials_poll_interval)

    async def _fetch_nse_fundamentals(self, symbol: str) -> None:
        url = f"{_NSE_BASE}/api/quote-equity?symbol={symbol}&section=fundamentals"
        try:
            async with self._session.get(url, timeout=aiohttp.ClientTimeout(total=15)) as r:
                data = await r.json(content_type=None)
        except Exception as exc:
            logger.debug("[financials] NSE fundamentals %s: %s", symbol, exc)
            return

        now = datetime.now(tz=timezone.utc)
        try:
            info = data.get("securityInfo", {})
            fin = data.get("industryInfo", {})
            ratios = KeyRatios(
                symbol=symbol,
                pe=_f(info.get("pe")),
                pb=_f(info.get("pb")),
                market_cap=_f(info.get("marketCap")),
                face_value=_f(info.get("faceValue")),
                dividend_yield=_f(info.get("divYield")),
                as_of=date.today(),
            )
            await self.hub.publish(
                DataMessage(
                    type=MessageType.FINANCIAL_STATEMENT,
                    source=self.name,
                    timestamp=now,
                    symbols=[symbol],
                    data={"kind": "key_ratios", **ratios.model_dump(mode="json")},
                )
            )
        except Exception as exc:
            logger.debug("[financials] NSE parse %s: %s", symbol, exc)

        # Quarterly results
        url2 = f"{_NSE_BASE}/api/results-comparision?index=equities&symbol={symbol}"
        try:
            async with self._session.get(url2, timeout=aiohttp.ClientTimeout(total=15)) as r:
                data2 = await r.json(content_type=None)
        except Exception:
            return

        for period_data in (data2.get("data") or [])[:4]:   # last 4 quarters
            try:
                period_end_str = period_data.get("toDate") or period_data.get("date", "")
                period_end = _parse_date(period_end_str)
                qr = QuarterlyResult(
                    symbol=symbol,
                    period=period_data.get("name", ""),
                    period_end=period_end,
                    revenue=_f(period_data.get("totalIncome")),
                    pat=_f(period_data.get("netProfit")),
                    eps=_f(period_data.get("eps")),
                )
                await self.hub.publish(
                    DataMessage(
                        type=MessageType.FINANCIAL_STATEMENT,
                        source=self.name,
                        timestamp=now,
                        symbols=[symbol],
                        data={"kind": "quarterly_result", **qr.model_dump(mode="json")},
                    )
                )
            except Exception as exc:
                logger.debug("[financials] quarterly parse %s: %s", symbol, exc)

    async def _fetch_screener(self, symbol: str) -> None:
        url = f"{_SCREENER_BASE}/api/company/{symbol}/?format=json"
        headers = {**_HEADERS, "Authorization": f"Token {settings.screener_api_key}"}
        try:
            async with self._session.get(
                url, headers=headers, timeout=aiohttp.ClientTimeout(total=15)
            ) as r:
                data = await r.json(content_type=None)
        except Exception as exc:
            logger.debug("[financials] screener %s: %s", symbol, exc)
            return

        now = datetime.now(tz=timezone.utc)
        try:
            ratios = KeyRatios(
                symbol=symbol,
                pe=_f(data.get("ratios", {}).get("pe")),
                pb=_f(data.get("ratios", {}).get("pb")),
                market_cap=_f(data.get("ratios", {}).get("marketCap")),
                roe=_f(data.get("ratios", {}).get("roe")),
                roce=_f(data.get("ratios", {}).get("roce")),
                dividend_yield=_f(data.get("ratios", {}).get("dividendYield")),
                promoter_holding=_f(data.get("shareholding", {}).get("promoter")),
                fii_holding=_f(data.get("shareholding", {}).get("fii")),
                dii_holding=_f(data.get("shareholding", {}).get("dii")),
                as_of=date.today(),
            )
            await self.hub.publish(
                DataMessage(
                    type=MessageType.FINANCIAL_STATEMENT,
                    source=self.name,
                    timestamp=now,
                    symbols=[symbol],
                    data={"kind": "key_ratios_screener", **ratios.model_dump(mode="json")},
                )
            )
        except Exception as exc:
            logger.debug("[financials] screener parse %s: %s", symbol, exc)


def _f(val) -> float | None:
    try:
        v = float(str(val).replace(",", ""))
        return v if v != 0.0 else None
    except Exception:
        return None


def _parse_date(s: str) -> date:
    for fmt in ("%d-%b-%Y", "%Y-%m-%d", "%d/%m/%Y"):
        try:
            return datetime.strptime(s[:10], fmt).date()
        except Exception:
            pass
    return date.today()
