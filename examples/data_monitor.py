"""
data_monitor.py — prints every message received from the data server.

Usage:
  python examples/data_monitor.py [ws://localhost:8765]

Useful for verifying the server is running and data is flowing.
"""

from __future__ import annotations

import asyncio
import sys
from datetime import datetime

from client import BaseStrategy
from server.models.base import MessageType
from server.models.corporate import CorporateEvent
from server.models.gdelt import GdeltArticle
from server.models.holdings import BlockDeal, BulkDeal, InsiderTrade
from server.models.news import NewsItem
from server.models.price import IndexData, MarketStatus, PriceTick


class DataMonitor(BaseStrategy):
    channels = list(MessageType)  # subscribe to everything
    symbols = []                  # all symbols

    def __init__(self, url: str) -> None:
        super().__init__(server_url=url)
        self._counts: dict[str, int] = {}

    def _log(self, label: str, text: str) -> None:
        self._counts[label] = self._counts.get(label, 0) + 1
        ts = datetime.now().strftime("%H:%M:%S.%f")[:-3]
        print(f"[{ts}] [{label:20s}] {text}")

    async def on_price_tick(self, tick: PriceTick) -> None:
        self._log(
            "PRICE_TICK",
            f"{tick.symbol:15s}  LTP={tick.ltp:>10.2f}  "
            f"chg={tick.change_pct:+.2f}%  vol={tick.volume:,}",
        )

    async def on_index_data(self, index: IndexData) -> None:
        self._log(
            "INDEX",
            f"{index.name:25s}  {index.value:>10.2f}  {index.change_pct:+.2f}%",
        )

    async def on_market_status(self, status: MarketStatus) -> None:
        self._log("MARKET_STATUS", f"{status.market} → {status.status}  {status.message}")

    async def on_news(self, item: NewsItem) -> None:
        syms = ",".join(item.symbols) if item.symbols else "—"
        self._log("NEWS", f"[{item.source:15s}] [{syms:10s}] {item.title[:80]}")

    async def on_gdelt_event(self, event: GdeltArticle) -> None:
        tone = f"tone={event.tone:+.1f}" if event.tone is not None else ""
        self._log("GDELT", f"{tone:12s} {event.title[:80]}")

    async def on_financial_statement(self, data: dict) -> None:
        kind = data.get("kind", "?")
        sym = data.get("symbol", "?")
        self._log("FINANCIALS", f"[{kind}] {sym}")

    async def on_corporate_event(self, event: CorporateEvent) -> None:
        self._log(
            "CORP_EVENT",
            f"{event.symbol:12s}  {event.event_type.value:12s}  {event.description[:60]}",
        )

    async def on_insider_trade(self, data: dict) -> None:
        kind = data.get("kind", "insider")
        sym = data.get("symbol") or data.get("date", "")
        self._log("INSIDER", f"[{kind}] {sym}")

    async def on_bulk_deal(self, deal: BulkDeal) -> None:
        self._log(
            "BULK_DEAL",
            f"{deal.symbol:12s}  {deal.deal_type:4s}  {deal.quantity:>10,}  @ {deal.price:.2f}  {deal.client_name[:40]}",
        )

    async def on_block_deal(self, deal: BlockDeal) -> None:
        self._log(
            "BLOCK_DEAL",
            f"{deal.symbol:12s}  {deal.deal_type:4s}  {deal.quantity:>10,}  @ {deal.price:.2f}  {deal.client_name[:40]}",
        )

    async def on_politician_disclosure(self, disc) -> None:
        sym = disc.symbol or "—"
        self._log("POLITICIAN", f"{disc.member_name:30s}  {sym:12s}  {disc.party}")

    async def on_connect(self) -> None:
        print(f"\n=== Data Monitor connected to {self.server_url} ===")
        print("Press Ctrl+C to stop\n")


if __name__ == "__main__":
    url = sys.argv[1] if len(sys.argv) > 1 else "ws://localhost:8765"
    asyncio.run(DataMonitor(url).run())
