"""
Holdings / insider data source.

Three datasets polled on a configurable schedule:

1. SEBI insider trading disclosures (Form C — promoter/director/KMP trades)
   GET https://www.sebi.gov.in/sebiweb/other/OtherAction.do?doInsidertrading=yes
   Returns HTML; we parse the table rows.

2. NSE bulk deals (≥0.5% of listed shares traded by a single entity)
   GET https://www.nseindia.com/api/bulk-deals-uploads?from={date}&to={date}

3. NSE block deals (negotiated, executed in the pre-market block-deal window)
   GET https://www.nseindia.com/api/block-deals-uploads?from={date}&to={date}

4. NSE FII/DII daily participation
   GET https://www.nseindia.com/api/fiidiiTradeReact

Politician/Parliament member holdings are fetched from Lok Sabha's declared
asset data (annual) — implemented as a stub that reads a locally-cached CSV
if placed at data/lok_sabha_holdings.csv, so you can update it annually.
"""

from __future__ import annotations

import asyncio
import csv
import logging
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import aiohttp

from server.config import settings
from server.hub import Hub
from server.models.base import DataMessage, MessageType
from server.models.holdings import BlockDeal, BulkDeal, FIIDIIFlow, InsiderTrade, PoliticianDisclosure

from .base import BaseSource

logger = logging.getLogger(__name__)

_NSE_BASE = "https://www.nseindia.com"
_SEBI_INSIDER = (
    "https://www.sebi.gov.in/sebiweb/other/OtherAction.do"
    "?doInsidertrading=yes&intmId=7"
)
_HEADERS = {
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 Chrome/124.0.0.0 Safari/537.36",
    "Accept": "application/json, text/html, */*",
    "Referer": "https://www.nseindia.com/",
}
_DATA_DIR = Path(__file__).parent.parent.parent / "data"


class HoldingsSource(BaseSource):
    name = "holdings"

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
                    self._poll_bulk_deals(),
                    self._poll_block_deals(),
                    self._poll_fii_dii(),
                    self._poll_sebi_insider(),
                    self._emit_politician_holdings(),
                    return_exceptions=True,
                )
                await asyncio.sleep(settings.holdings_poll_interval)

    # ── NSE bulk deals ───────────────────────────────────────────────────

    async def _poll_bulk_deals(self) -> None:
        today = date.today()
        window = today - timedelta(days=7)
        url = (
            f"{_NSE_BASE}/api/bulk-deals-uploads"
            f"?from={window.strftime('%d-%m-%Y')}&to={today.strftime('%d-%m-%Y')}"
        )
        try:
            async with self._session.get(url, timeout=aiohttp.ClientTimeout(total=15)) as r:
                data = await r.json(content_type=None)
        except Exception as exc:
            logger.debug("[holdings] bulk deals failed: %s", exc)
            return

        now = datetime.now(tz=timezone.utc)
        for item in data.get("data", []):
            key = f"bulk_{item.get('symbol')}_{item.get('clientName')}_{item.get('tradDate')}"
            if key in self._seen:
                continue
            self._seen.add(key)
            try:
                deal = BulkDeal(
                    symbol=(item.get("symbol") or "").upper(),
                    exchange=item.get("exchange", "NSE").upper(),
                    client_name=item.get("clientName", ""),
                    deal_type=item.get("buyOrSell", "").upper(),
                    quantity=int(item.get("quantityTraded", 0)),
                    price=float(item.get("tradePrice", 0)),
                    deal_date=_parse_date(item.get("tradDate")),
                )
                await self.hub.publish(
                    DataMessage(
                        type=MessageType.BULK_DEAL,
                        source=self.name,
                        timestamp=now,
                        symbols=[deal.symbol],
                        data=deal.model_dump(mode="json"),
                    )
                )
            except Exception as exc:
                logger.debug("[holdings] bulk deal parse: %s", exc)

    # ── NSE block deals ──────────────────────────────────────────────────

    async def _poll_block_deals(self) -> None:
        today = date.today()
        window = today - timedelta(days=7)
        url = (
            f"{_NSE_BASE}/api/block-deals-uploads"
            f"?from={window.strftime('%d-%m-%Y')}&to={today.strftime('%d-%m-%Y')}"
        )
        try:
            async with self._session.get(url, timeout=aiohttp.ClientTimeout(total=15)) as r:
                data = await r.json(content_type=None)
        except Exception as exc:
            logger.debug("[holdings] block deals failed: %s", exc)
            return

        now = datetime.now(tz=timezone.utc)
        for item in data.get("data", []):
            key = f"block_{item.get('symbol')}_{item.get('clientName')}_{item.get('tradDate')}"
            if key in self._seen:
                continue
            self._seen.add(key)
            try:
                deal = BlockDeal(
                    symbol=(item.get("symbol") or "").upper(),
                    exchange=item.get("exchange", "NSE").upper(),
                    client_name=item.get("clientName", ""),
                    deal_type=item.get("buyOrSell", "").upper(),
                    quantity=int(item.get("quantityTraded", 0)),
                    price=float(item.get("tradePrice", 0)),
                    deal_date=_parse_date(item.get("tradDate")),
                )
                await self.hub.publish(
                    DataMessage(
                        type=MessageType.BLOCK_DEAL,
                        source=self.name,
                        timestamp=now,
                        symbols=[deal.symbol],
                        data=deal.model_dump(mode="json"),
                    )
                )
            except Exception as exc:
                logger.debug("[holdings] block deal parse: %s", exc)

    # ── FII/DII daily flow ───────────────────────────────────────────────

    async def _poll_fii_dii(self) -> None:
        url = f"{_NSE_BASE}/api/fiidiiTradeReact"
        try:
            async with self._session.get(url, timeout=aiohttp.ClientTimeout(total=15)) as r:
                data = await r.json(content_type=None)
        except Exception as exc:
            logger.debug("[holdings] FII/DII failed: %s", exc)
            return

        now = datetime.now(tz=timezone.utc)
        for item in data if isinstance(data, list) else []:
            try:
                key = f"fiidii_{item.get('date')}_{item.get('category')}"
                if key in self._seen:
                    continue
                self._seen.add(key)
                category = item.get("category", "").upper()
                net = float(item.get("netPurchases", 0) or 0)
                buy = float(item.get("grossPurchase", 0) or 0)
                sell = float(item.get("grossSales", 0) or 0)
                flow = FIIDIIFlow(
                    date=_parse_date(item.get("date")),
                    fii_buy=buy if "FII" in category else 0,
                    fii_sell=sell if "FII" in category else 0,
                    fii_net=net if "FII" in category else 0,
                    dii_buy=buy if "DII" in category else 0,
                    dii_sell=sell if "DII" in category else 0,
                    dii_net=net if "DII" in category else 0,
                )
                await self.hub.publish(
                    DataMessage(
                        type=MessageType.INSIDER_TRADE,
                        source=self.name,
                        timestamp=now,
                        data={"kind": "fii_dii_flow", **flow.model_dump(mode="json")},
                    )
                )
            except Exception as exc:
                logger.debug("[holdings] FII/DII parse: %s", exc)

    # ── SEBI insider trading disclosures ─────────────────────────────────

    async def _poll_sebi_insider(self) -> None:
        """
        SEBI's website serves HTML with a table.  We parse it with basic
        string processing rather than a heavy HTML parser dependency.
        """
        try:
            async with self._session.get(
                _SEBI_INSIDER,
                headers={**_HEADERS, "Accept": "text/html"},
                timeout=aiohttp.ClientTimeout(total=20),
            ) as r:
                html = await r.text()
        except Exception as exc:
            logger.debug("[holdings] SEBI insider failed: %s", exc)
            return

        now = datetime.now(tz=timezone.utc)
        rows = _parse_sebi_table(html)
        for row in rows:
            key = "_".join(str(v) for v in row.values())
            if key in self._seen:
                continue
            self._seen.add(key)
            try:
                trade = InsiderTrade(
                    symbol=row.get("symbol", "").upper(),
                    acquirer_name=row.get("name", ""),
                    acquirer_category=row.get("category", ""),
                    transaction_type=row.get("transaction", ""),
                    shares=_int(row.get("shares")),
                    value=_float(row.get("value")),
                    trade_date=_parse_date(row.get("trade_date")),
                    disclosure_date=_parse_date(row.get("disclosure_date")),
                    source_url=_SEBI_INSIDER,
                )
                await self.hub.publish(
                    DataMessage(
                        type=MessageType.INSIDER_TRADE,
                        source=self.name,
                        timestamp=now,
                        symbols=[trade.symbol] if trade.symbol else [],
                        data=trade.model_dump(mode="json"),
                    )
                )
            except Exception as exc:
                logger.debug("[holdings] SEBI insider parse: %s", exc)

    # ── Politician holdings (annual CSV) ─────────────────────────────────

    async def _emit_politician_holdings(self) -> None:
        """
        Reads data/lok_sabha_holdings.csv if present.
        Expected columns: member_name, house, constituency, party,
                          symbol, company_name, shares, value,
                          declaration_year, source_url
        Download the latest declarations from loksabha.nic.in and convert
        to this CSV format.  The server emits all rows once per poll cycle.
        """
        csv_path = _DATA_DIR / "lok_sabha_holdings.csv"
        if not csv_path.exists():
            return

        now = datetime.now(tz=timezone.utc)
        try:
            with csv_path.open() as f:
                reader = csv.DictReader(f)
                for row in reader:
                    sym = (row.get("symbol") or "").upper()
                    disc = PoliticianDisclosure(
                        member_name=row.get("member_name", ""),
                        house=row.get("house", "Lok Sabha"),
                        constituency=row.get("constituency", ""),
                        party=row.get("party", ""),
                        symbol=sym or None,
                        company_name=row.get("company_name", ""),
                        shares=_int(row.get("shares")),
                        value=_float(row.get("value")),
                        declaration_year=_int(row.get("declaration_year")),
                        source_url=row.get("source_url", ""),
                        fetched_at=now,
                    )
                    await self.hub.publish(
                        DataMessage(
                            type=MessageType.POLITICIAN_DISCLOSURE,
                            source=self.name,
                            timestamp=now,
                            symbols=[sym] if sym else [],
                            data=disc.model_dump(mode="json"),
                        )
                    )
        except Exception as exc:
            logger.warning("[holdings] politician CSV read failed: %s", exc)


# ── Helpers ───────────────────────────────────────────────────────────────

def _parse_date(s: str | None) -> date:
    if not s:
        return date.today()
    for fmt in ("%d-%b-%Y", "%d-%m-%Y", "%Y-%m-%d", "%d/%m/%Y"):
        try:
            return datetime.strptime(s[:11].strip(), fmt).date()
        except Exception:
            pass
    return date.today()


def _int(v) -> int | None:
    try:
        return int(str(v).replace(",", ""))
    except Exception:
        return None


def _float(v) -> float | None:
    try:
        return float(str(v).replace(",", ""))
    except Exception:
        return None


def _parse_sebi_table(html: str) -> list[dict]:
    """
    Minimal table parser — extracts <td> content from SEBI's disclosure table.
    Column order (approximate): name, company, symbol, category, shares,
    value, transaction, trade_date, disclosure_date
    """
    import re
    rows_html = re.findall(r"<tr[^>]*>(.*?)</tr>", html, re.DOTALL | re.IGNORECASE)
    results = []
    for row_html in rows_html:
        cells = re.findall(r"<td[^>]*>(.*?)</td>", row_html, re.DOTALL | re.IGNORECASE)
        cells = [re.sub(r"<[^>]+>", "", c).strip() for c in cells]
        if len(cells) >= 9:
            results.append({
                "name": cells[0],
                "company": cells[1],
                "symbol": cells[2],
                "category": cells[3],
                "shares": cells[4],
                "value": cells[5],
                "transaction": cells[6],
                "trade_date": cells[7],
                "disclosure_date": cells[8],
            })
    return results
