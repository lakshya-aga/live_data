"""
Holdings / insider data source.

Four datasets:

1. SEBI insider trading disclosures (Form C — promoter/director/KMP trades)
   GET https://www.sebi.gov.in/sebiweb/other/OtherAction.do?doInsidertrading=yes
   Returns HTML; we parse the table rows.

2. NSE bulk deals (≥0.5% of listed shares traded by a single entity)
   GET https://www.nseindia.com/api/bulk-deals-uploads?from={date}&to={date}

3. NSE block deals (negotiated, executed in the pre-market block-deal window)
   GET https://www.nseindia.com/api/block-deals-uploads?from={date}&to={date}

4. NSE FII/DII daily participation
   GET https://www.nseindia.com/api/fiidiiTradeReact

5. Politician shareholdings — scraped from MyNeta.info (ADR project).
   MyNeta processes Election Commission of India (ECI) affidavits filed by
   candidates when contesting elections.  Each affidavit contains a section
   on Movable Assets that lists shares / bonds / debentures held.

   ECI mandates disclosure of all listed and unlisted company shares.
   We scrape the most recent Lok Sabha and current Rajya Sabha elections,
   match company names to NSE symbols, and emit PoliticianDisclosure events.

   MyNeta pages used:
     Candidate list: https://myneta.info/{election}/index.php?action=show_candidates&sort=default&state_id=S&constituency_id=0
     Summary table:  https://myneta.info/{election}/index.php?action=show_candidates&sort=default (all India)
     Candidate affidavit: https://myneta.info/{election}/candidate.php?candidate_id={id}

   Data is annual-ish (tied to elections) so we poll once per day and cache
   in data/politician_holdings_cache.json.  Falls back to
   data/lok_sabha_holdings.csv if the scraper fails.
"""

from __future__ import annotations

import asyncio
import csv
import json
import logging
import re
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
_MYNETA_BASE = "https://myneta.info"

# Most recent general elections to scrape (slug → display name)
_ELECTIONS = {
    "LokSabha2024": "Lok Sabha 2024",
    "LokSabha2019": "Lok Sabha 2019",
}

_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,*/*",
}
_DATA_DIR = Path(__file__).parent.parent.parent / "data"
_CACHE_FILE = _DATA_DIR / "politician_holdings_cache.json"
_CSV_FALLBACK = _DATA_DIR / "lok_sabha_holdings.csv"

# Minimum share value (INR) to consider significant
_MIN_SHARE_VALUE = 100_000


class HoldingsSource(BaseSource):
    name = "holdings"

    def __init__(self, hub: Hub) -> None:
        super().__init__(hub)
        self._seen: set[str] = set()

    async def _run(self) -> None:
        connector = aiohttp.TCPConnector(ssl=False)
        async with aiohttp.ClientSession(headers=_HEADERS, connector=connector) as session:
            self._session = session
            # Seed NSE cookies for bulk/block deal endpoints
            try:
                async with session.get(
                    _NSE_BASE,
                    headers={**_HEADERS, "Referer": "https://www.nseindia.com/"},
                    timeout=aiohttp.ClientTimeout(total=10),
                ) as r:
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
        nse_headers = {**_HEADERS, "Referer": "https://www.nseindia.com/", "Accept": "application/json"}
        url = (
            f"{_NSE_BASE}/api/bulk-deals-uploads"
            f"?from={window.strftime('%d-%m-%Y')}&to={today.strftime('%d-%m-%Y')}"
        )
        try:
            async with self._session.get(
                url, headers=nse_headers, timeout=aiohttp.ClientTimeout(total=15)
            ) as r:
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
        nse_headers = {**_HEADERS, "Referer": "https://www.nseindia.com/", "Accept": "application/json"}
        url = (
            f"{_NSE_BASE}/api/block-deals-uploads"
            f"?from={window.strftime('%d-%m-%Y')}&to={today.strftime('%d-%m-%Y')}"
        )
        try:
            async with self._session.get(
                url, headers=nse_headers, timeout=aiohttp.ClientTimeout(total=15)
            ) as r:
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
        nse_headers = {**_HEADERS, "Referer": "https://www.nseindia.com/", "Accept": "application/json"}
        url = f"{_NSE_BASE}/api/fiidiiTradeReact"
        try:
            async with self._session.get(
                url, headers=nse_headers, timeout=aiohttp.ClientTimeout(total=15)
            ) as r:
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
        try:
            async with self._session.get(
                _SEBI_INSIDER,
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

    # ── Politician holdings (MyNeta.info) ────────────────────────────────

    async def _emit_politician_holdings(self) -> None:
        """
        Emits PoliticianDisclosure events from:
          1. MyNeta.info scraper (live, cached daily in data/politician_holdings_cache.json)
          2. data/lok_sabha_holdings.csv (manual fallback / override)

        MyNeta.info aggregates ECI affidavits — the only legally mandated
        public disclosure of shares held by Indian political candidates.
        Affidavits are filed at election time (not continuously), so data
        is refreshed once per election cycle (every 5 years for Lok Sabha).
        """
        records = await self._load_myneta_holdings()
        if not records:
            records = _load_csv_holdings()

        now = datetime.now(tz=timezone.utc)
        for rec in records:
            sym = (rec.get("symbol") or "").upper()
            key = f"pol_{rec.get('member_name')}_{sym}_{rec.get('declaration_year')}"
            if key in self._seen:
                continue
            self._seen.add(key)
            try:
                disc = PoliticianDisclosure(
                    member_name=rec.get("member_name", ""),
                    house=rec.get("house", "Lok Sabha"),
                    constituency=rec.get("constituency", ""),
                    party=rec.get("party", ""),
                    symbol=sym or None,
                    company_name=rec.get("company_name", ""),
                    shares=_int(rec.get("shares")),
                    value=_float(rec.get("value")),
                    declaration_year=_int(rec.get("declaration_year")),
                    source_url=rec.get("source_url", ""),
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
                logger.debug("[holdings] politician emit: %s", exc)

    async def _load_myneta_holdings(self) -> list[dict]:
        """
        Returns cached holdings if fresh (< 24 h), otherwise scrapes MyNeta.
        """
        _DATA_DIR.mkdir(exist_ok=True)

        # Check cache freshness
        if _CACHE_FILE.exists():
            try:
                cache = json.loads(_CACHE_FILE.read_text())
                cached_at = datetime.fromisoformat(cache.get("cached_at", "2000-01-01"))
                if datetime.now(tz=timezone.utc) - cached_at < timedelta(hours=24):
                    logger.debug("[holdings] using cached MyNeta data (%d records)", len(cache["records"]))
                    return cache["records"]
            except Exception:
                pass

        logger.info("[holdings] scraping MyNeta.info for politician holdings…")
        records: list[dict] = []
        for election_slug, election_name in _ELECTIONS.items():
            try:
                new_records = await self._scrape_election(election_slug, election_name)
                records.extend(new_records)
                logger.info("[holdings] %s: scraped %d share declarations", election_name, len(new_records))
            except Exception as exc:
                logger.warning("[holdings] MyNeta scrape failed for %s: %s", election_slug, exc)

        if records:
            try:
                _CACHE_FILE.write_text(json.dumps({
                    "cached_at": datetime.now(tz=timezone.utc).isoformat(),
                    "records": records,
                }))
            except Exception:
                pass

        return records

    async def _scrape_election(self, slug: str, election_name: str) -> list[dict]:
        """
        Scrapes the full candidate list for one election from MyNeta.info,
        then fetches each candidate's affidavit to extract share declarations.

        MyNeta HTML structure (as of 2024):
          Candidate list table: <table class="w3-table-all"> with rows of
            [#, Candidate, Party, Criminal, Education, Total Assets, Liabilities]
          Affidavit page: tables with class "tablesorter" listing asset rows.
          Shares appear in a table headed "Movable Assets" with rows like:
            | Shares in Companies | Company Name Ltd | 1000 | Rs 5,00,000 |
        """
        list_url = f"{_MYNETA_BASE}/{slug}/index.php?action=show_candidates&sort=assets"
        try:
            async with self._session.get(list_url, timeout=aiohttp.ClientTimeout(total=30)) as r:
                html = await r.text()
        except Exception as exc:
            logger.debug("[holdings] candidate list fetch failed: %s", exc)
            return []

        # Extract candidate IDs and names from the table
        # Links look like: candidate.php?candidate_id=1234
        candidates = _parse_candidate_list(html, slug, election_name)
        logger.debug("[holdings] %s: found %d candidates", slug, len(candidates))

        # Scrape top 200 by declared assets (sorted by assets, already sorted)
        records: list[dict] = []
        for cand in candidates[:200]:
            try:
                cand_records = await self._scrape_candidate_affidavit(cand, slug)
                records.extend(cand_records)
                await asyncio.sleep(0.3)  # polite crawl rate
            except Exception as exc:
                logger.debug("[holdings] affidavit scrape for %s: %s", cand.get("name"), exc)

        return records

    async def _scrape_candidate_affidavit(self, cand: dict, slug: str) -> list[dict]:
        """Fetch one candidate's affidavit page and extract share rows."""
        url = f"{_MYNETA_BASE}/{slug}/candidate.php?candidate_id={cand['candidate_id']}"
        try:
            async with self._session.get(url, timeout=aiohttp.ClientTimeout(total=20)) as r:
                html = await r.text()
        except Exception as exc:
            logger.debug("[holdings] affidavit fetch failed: %s", exc)
            return []

        share_rows = _parse_share_rows(html)
        results = []
        for row in share_rows:
            company = row.get("company", "")
            value = _float(row.get("value"))
            shares = _int(row.get("shares"))
            if not company:
                continue
            if value and value < _MIN_SHARE_VALUE:
                continue  # skip trivially small holdings

            sym = _company_to_symbol(company)
            results.append({
                "member_name": cand.get("name", ""),
                "house": cand.get("house", "Lok Sabha"),
                "constituency": cand.get("constituency", ""),
                "party": cand.get("party", ""),
                "symbol": sym,
                "company_name": company,
                "shares": shares,
                "value": value,
                "declaration_year": cand.get("election_year"),
                "source_url": url,
            })
        return results


# ── HTML parsers ──────────────────────────────────────────────────────────

def _parse_candidate_list(html: str, slug: str, election_name: str) -> list[dict]:
    """
    Extracts candidate entries from MyNeta's all-candidates table.
    Returns list of dicts with keys: candidate_id, name, party, constituency, house, election_year.
    """
    year_match = re.search(r"\d{4}", slug)
    year = int(year_match.group()) if year_match else 0
    house = "Rajya Sabha" if "Rajya" in election_name else "Lok Sabha"

    candidates = []
    # Find all candidate links
    for m in re.finditer(
        r'candidate\.php\?candidate_id=(\d+)[^"]*"[^>]*>\s*([^<]+)</a>',
        html,
        re.IGNORECASE,
    ):
        cand_id = m.group(1)
        name = m.group(2).strip()
        if not name or name.lower() in ("candidate", "winner", "name"):
            continue
        candidates.append({
            "candidate_id": cand_id,
            "name": name,
            "house": house,
            "election_year": year,
            "party": "",
            "constituency": "",
        })

    # Try to enrich party + constituency from surrounding <td> context
    rows = re.findall(r"<tr[^>]*>(.*?)</tr>", html, re.DOTALL | re.IGNORECASE)
    for row in rows:
        id_m = re.search(r"candidate_id=(\d+)", row)
        if not id_m:
            continue
        cand_id = id_m.group(1)
        cells = re.findall(r"<td[^>]*>(.*?)</td>", row, re.DOTALL | re.IGNORECASE)
        cells_clean = [re.sub(r"<[^>]+>", "", c).strip() for c in cells]
        for c in candidates:
            if c["candidate_id"] == cand_id and len(cells_clean) >= 3:
                c["constituency"] = cells_clean[0] if cells_clean else ""
                c["party"] = cells_clean[2] if len(cells_clean) > 2 else ""
                break

    return candidates


def _parse_share_rows(html: str) -> list[dict]:
    """
    Parses the affidavit HTML for share/bond/debenture rows.

    MyNeta affidavit tables contain rows like:
      <tr><td>Shares in listed companies</td><td>Reliance Industries Ltd</td>
          <td>100</td><td>Rs 2,50,000/-</td></tr>
    """
    share_rows: list[dict] = []

    # Find sections mentioning shares/bonds
    sections = re.split(
        r"(?i)(shares|bonds|debentures|mutual fund|movable asset)",
        html,
    )

    # Scan all table rows for patterns matching share declarations
    rows = re.findall(r"<tr[^>]*>(.*?)</tr>", html, re.DOTALL | re.IGNORECASE)
    for row in rows:
        cells = re.findall(r"<td[^>]*>(.*?)</td>", row, re.DOTALL | re.IGNORECASE)
        cells_clean = [re.sub(r"<[^>]+>", " ", c).strip() for c in cells]

        if len(cells_clean) < 2:
            continue

        row_text = " ".join(cells_clean).lower()
        if not any(kw in row_text for kw in ("share", "listed", "debenture", "bond", "equity")):
            continue
        if "total" in row_text or "grand total" in row_text:
            continue

        # Try to extract: company name, number of shares, value
        company = ""
        shares = None
        value = None

        # Heuristic: company is the cell containing "Ltd" or "Limited"
        for cell in cells_clean:
            if re.search(r"\b(ltd|limited|pvt|inc|corp|llp)\b", cell, re.IGNORECASE):
                company = cell
                break

        # Look for numeric cells for shares and value
        for cell in cells_clean:
            clean = cell.replace(",", "").replace("Rs", "").replace("/-", "").strip()
            nums = re.findall(r"\d+(?:\.\d+)?", clean)
            if not nums:
                continue
            val = float(nums[0])
            if val > 10_000_000:   # > 1cr → likely value in rupees
                value = val
            elif val > 0 and shares is None:
                shares = int(val)

        if company:
            share_rows.append({"company": company, "shares": shares, "value": value})

    return share_rows


# ── Symbol lookup ─────────────────────────────────────────────────────────

# Partial-name → NSE symbol mapping for common companies
_COMPANY_SYMBOL_MAP: dict[str, str] = {
    "reliance": "RELIANCE",
    "tcs": "TCS",
    "tata consultancy": "TCS",
    "infosys": "INFY",
    "hdfc bank": "HDFCBANK",
    "hdfc ltd": "HDFC",
    "icici bank": "ICICIBANK",
    "kotak mahindra": "KOTAKBANK",
    "state bank": "SBIN",
    "bajaj finance": "BAJFINANCE",
    "bharti airtel": "BHARTIARTL",
    "larsen": "LT",
    "asian paints": "ASIANPAINT",
    "maruti suzuki": "MARUTI",
    "titan": "TITAN",
    "ultratech": "ULTRACEMCO",
    "nestle": "NESTLEIND",
    "wipro": "WIPRO",
    "hcl tech": "HCLTECH",
    "tech mahindra": "TECHM",
    "adani enterprises": "ADANIENT",
    "adani ports": "ADANIPORTS",
    "sun pharma": "SUNPHARMA",
    "dr reddy": "DRREDDY",
    "tata motors": "TATAMOTORS",
    "tata steel": "TATASTEEL",
    "ongc": "ONGC",
    "itc": "ITC",
    "axis bank": "AXISBANK",
    "indusind": "INDUSINDBK",
    "cipla": "CIPLA",
    "eicher": "EICHERMOT",
    "hero motocorp": "HEROMOTOCO",
    "britannia": "BRITANNIA",
    "divi": "DIVISLAB",
    "pidilite": "PIDILITIND",
    "zomato": "ZOMATO",
    "nykaa": "FSN",
    "paytm": "PAYTM",
}


def _company_to_symbol(name: str) -> str | None:
    lower = name.lower()
    for fragment, symbol in _COMPANY_SYMBOL_MAP.items():
        if fragment in lower:
            return symbol
    return None


# ── CSV fallback ──────────────────────────────────────────────────────────

def _load_csv_holdings() -> list[dict]:
    if not _CSV_FALLBACK.exists():
        return []
    try:
        with _CSV_FALLBACK.open() as f:
            reader = csv.DictReader(f)
            return [row for row in reader if not row.get("member_name", "").startswith("#")]
    except Exception:
        return []


# ── Shared helpers ────────────────────────────────────────────────────────

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
        return int(str(v).replace(",", "").split(".")[0])
    except Exception:
        return None


def _float(v) -> float | None:
    try:
        return float(str(v).replace(",", "").replace("Rs", "").replace("/-", "").strip())
    except Exception:
        return None


def _parse_sebi_table(html: str) -> list[dict]:
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
