"""
Base strategy class.

Strategies subclass BaseStrategy, override on_<message_type>() handlers,
declare which channels and symbols they care about, then call .run().

Example
-------
    class MomentumStrategy(BaseStrategy):
        channels = [MessageType.PRICE_TICK]
        symbols  = ["RELIANCE", "TCS", "INFY"]

        async def on_price_tick(self, tick: PriceTick) -> None:
            if tick.change_pct > 2.0:
                print(f"Strong move: {tick.symbol} +{tick.change_pct:.2f}%")

    asyncio.run(MomentumStrategy().run())
"""

from __future__ import annotations

import asyncio
import logging
from typing import Any

import orjson
import websockets

from server.models.base import DataMessage, MessageType, SubscribeMessage
from server.models.corporate import CorporateEvent
from server.models.financials import KeyRatios, QuarterlyResult
from server.models.gdelt import GdeltArticle
from server.models.holdings import BlockDeal, BulkDeal, FIIDIIFlow, InsiderTrade, PoliticianDisclosure
from server.models.news import NewsItem
from server.models.price import IndexData, MarketStatus, PriceTick

logger = logging.getLogger(__name__)


class BaseStrategy:
    # Override in subclasses
    channels: list[MessageType] = list(MessageType)   # all by default
    symbols: list[str] = []     # empty = all symbols

    server_url: str = "ws://localhost:8765"

    def __init__(self, server_url: str | None = None) -> None:
        if server_url:
            self.server_url = server_url

    # ── Main loop ─────────────────────────────────────────────────────────

    async def run(self) -> None:
        while True:
            try:
                await self._connect()
            except (websockets.ConnectionClosed, OSError) as exc:
                logger.warning("Disconnected from server (%s), reconnecting in 3s…", exc)
                await asyncio.sleep(3)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logger.error("Unexpected error: %s, reconnecting in 5s…", exc)
                await asyncio.sleep(5)

    async def _connect(self) -> None:
        logger.info("Connecting to %s", self.server_url)
        async with websockets.connect(self.server_url) as ws:
            # Send subscription
            sub = SubscribeMessage(
                action="subscribe",
                channels=self.channels,
                symbols=self.symbols,
            )
            await ws.send(orjson.dumps(sub.model_dump(mode="json")))

            await self.on_connect()
            async for raw in ws:
                try:
                    data = orjson.loads(raw)
                    # Server ack/info/pong messages have no "type" in MessageType enum
                    if data.get("type") in ("ack", "error", "pong", "info"):
                        continue
                    msg = DataMessage(**data)
                    await self._dispatch(msg)
                except Exception as exc:
                    logger.debug("Message parse error: %s", exc)

    async def _dispatch(self, msg: DataMessage) -> None:
        t = msg.type
        d = msg.data

        try:
            if t == MessageType.PRICE_TICK:
                await self.on_price_tick(PriceTick(**d))
            elif t == MessageType.ORDER_BOOK:
                from server.models.price import OrderBook
                await self.on_order_book(OrderBook(**d))
            elif t == MessageType.INDEX_DATA:
                await self.on_index_data(IndexData(**d))
            elif t == MessageType.MARKET_STATUS:
                await self.on_market_status(MarketStatus(**d))
            elif t == MessageType.NEWS:
                await self.on_news(NewsItem(**d))
            elif t == MessageType.GDELT_EVENT:
                await self.on_gdelt_event(GdeltArticle(**d))
            elif t == MessageType.FINANCIAL_STATEMENT:
                await self.on_financial_statement(d)
            elif t == MessageType.CORPORATE_EVENT:
                await self.on_corporate_event(CorporateEvent(**d))
            elif t == MessageType.INSIDER_TRADE:
                await self.on_insider_trade(d)
            elif t == MessageType.BULK_DEAL:
                await self.on_bulk_deal(BulkDeal(**d))
            elif t == MessageType.BLOCK_DEAL:
                await self.on_block_deal(BlockDeal(**d))
            elif t == MessageType.POLITICIAN_DISCLOSURE:
                await self.on_politician_disclosure(PoliticianDisclosure(**d))
            elif t == MessageType.HEARTBEAT:
                await self.on_heartbeat(d)
            else:
                await self.on_message(msg)
        except Exception as exc:
            logger.warning("Handler error for %s: %s", t, exc)

    # ── Override these ────────────────────────────────────────────────────

    async def on_connect(self) -> None:
        logger.info("%s connected to data server", self.__class__.__name__)

    async def on_price_tick(self, tick: PriceTick) -> None:
        pass

    async def on_order_book(self, book: Any) -> None:
        pass

    async def on_index_data(self, index: IndexData) -> None:
        pass

    async def on_market_status(self, status: MarketStatus) -> None:
        pass

    async def on_news(self, item: NewsItem) -> None:
        pass

    async def on_gdelt_event(self, event: GdeltArticle) -> None:
        pass

    async def on_financial_statement(self, data: dict) -> None:
        pass

    async def on_corporate_event(self, event: CorporateEvent) -> None:
        pass

    async def on_insider_trade(self, data: dict) -> None:
        pass

    async def on_bulk_deal(self, deal: BulkDeal) -> None:
        pass

    async def on_block_deal(self, deal: BlockDeal) -> None:
        pass

    async def on_politician_disclosure(self, disc: PoliticianDisclosure) -> None:
        pass

    async def on_heartbeat(self, data: dict) -> None:
        pass

    async def on_message(self, msg: DataMessage) -> None:
        """Catch-all for unknown message types."""
        pass
