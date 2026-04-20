"""
GROWW price data source.

Authentication flow
-------------------
1. Set GROWW_API_KEY + GROWW_API_SECRET in .env (from developer.groww.in).
2. On startup the source exchanges the key/secret for a short-lived access
   token via the OAuth token endpoint and connects to the WebSocket feed.
3. If GROWW_ACCESS_TOKEN is set directly, the OAuth exchange is skipped.
4. When neither credential is present the source falls back to the public
   NSE quote REST API (nse.py handles this — GrowwSource simply emits nothing
   and logs a warning so the NSE source can cover price data).

GROWW WebSocket protocol (developer.groww.in)
---------------------------------------------
Connection:  wss://ws.groww.in/v1/market-data/stream
Headers:     Authorization: Bearer <access_token>

Subscribe message sent after connect:
  { "action": "subscribe", "symbols": ["NSE:RELIANCE", "NSE:TCS", ...] }

Tick message received (per symbol, on every trade):
  {
    "type": "tick",
    "symbol": "NSE:RELIANCE",
    "ltp": 2450.50,
    "change": 15.30,
    "change_pct": 0.63,
    "open": 2440.00,
    "high": 2460.00,
    "low": 2435.00,
    "prev_close": 2435.20,
    "volume": 12345678,
    "avg_price": 2445.00,
    "bid": 2450.40,
    "ask": 2450.60,
    "bid_qty": 500,
    "ask_qty": 300,
    "timestamp": 1713611400000    (ms epoch)
  }

Depth (order book) message:
  {
    "type": "depth",
    "symbol": "NSE:RELIANCE",
    "bids": [{"price": 2450.40, "quantity": 500, "orders": 3}, ...],
    "asks": [{"price": 2450.60, "quantity": 300, "orders": 2}, ...],
    "timestamp": 1713611400000
  }

NOTE: exact endpoint and message schema are subject to GROWW API versioning.
Update _WS_URL and _TOKEN_URL if the developer portal shows different values.
"""

from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timezone

import orjson
import websockets

from server.config import settings
from server.hub import Hub
from server.models.base import DataMessage, MessageType
from server.models.price import OrderBook, OrderBookLevel, PriceTick

from .base import BaseSource

logger = logging.getLogger(__name__)

_TOKEN_URL = "https://api.groww.in/v1/oauth/token"
_WS_URL = "wss://ws.groww.in/v1/market-data/stream"


class GrowwSource(BaseSource):
    name = "groww"

    def __init__(self, hub: Hub) -> None:
        super().__init__(hub)
        self._access_token: str = settings.groww_access_token

    async def _run(self) -> None:
        if not self._access_token:
            await self._fetch_token()

        if not self._access_token:
            logger.warning(
                "[groww] No access token — set GROWW_API_KEY/SECRET or GROWW_ACCESS_TOKEN. "
                "Price data will come from NSE fallback only."
            )
            # Park forever; NSE source provides price data as fallback.
            await asyncio.Future()

        headers = {"Authorization": f"Bearer {self._access_token}"}
        symbols = [f"NSE:{sym}" for sym in settings.watchlist]

        logger.info("[groww] connecting to %s for %d symbols", _WS_URL, len(symbols))
        async with websockets.connect(_WS_URL, additional_headers=headers) as ws:
            sub = orjson.dumps({"action": "subscribe", "symbols": symbols})
            await ws.send(sub)
            logger.info("[groww] subscribed")
            async for raw in ws:
                await self._handle(raw)

    async def _fetch_token(self) -> None:
        import httpx

        if not (settings.groww_api_key and settings.groww_api_secret):
            return
        try:
            async with httpx.AsyncClient(timeout=10) as client:
                resp = await client.post(
                    _TOKEN_URL,
                    json={
                        "grant_type": "client_credentials",
                        "client_id": settings.groww_api_key,
                        "client_secret": settings.groww_api_secret,
                    },
                )
                resp.raise_for_status()
                payload = resp.json()
                self._access_token = payload["access_token"]
                logger.info("[groww] token obtained, expires_in=%s", payload.get("expires_in"))
        except Exception as exc:
            logger.error("[groww] token fetch failed: %s", exc)

    async def _handle(self, raw: bytes | str) -> None:
        try:
            msg = orjson.loads(raw)
        except Exception:
            return

        msg_type = msg.get("type")
        raw_sym: str = msg.get("symbol", "")
        # Strip exchange prefix: "NSE:RELIANCE" → "RELIANCE"
        symbol = raw_sym.split(":")[-1] if ":" in raw_sym else raw_sym

        if msg_type == "tick":
            ts_ms = msg.get("timestamp", 0)
            ts = datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc) if ts_ms else datetime.now(tz=timezone.utc)
            tick = PriceTick(
                symbol=symbol,
                ltp=msg["ltp"],
                change=msg.get("change", 0.0),
                change_pct=msg.get("change_pct", 0.0),
                open=msg.get("open", 0.0),
                high=msg.get("high", 0.0),
                low=msg.get("low", 0.0),
                prev_close=msg.get("prev_close", 0.0),
                volume=msg.get("volume", 0),
                avg_price=msg.get("avg_price"),
                bid=msg.get("bid"),
                ask=msg.get("ask"),
                bid_qty=msg.get("bid_qty"),
                ask_qty=msg.get("ask_qty"),
                trade_time=ts,
            )
            await self.hub.publish(
                DataMessage(
                    type=MessageType.PRICE_TICK,
                    source=self.name,
                    timestamp=ts,
                    symbols=[symbol],
                    data=tick.model_dump(mode="json"),
                )
            )

        elif msg_type == "depth":
            ts_ms = msg.get("timestamp", 0)
            ts = datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc) if ts_ms else datetime.now(tz=timezone.utc)
            book = OrderBook(
                symbol=symbol,
                bids=[OrderBookLevel(**b) for b in msg.get("bids", [])],
                asks=[OrderBookLevel(**a) for a in msg.get("asks", [])],
                timestamp=ts,
            )
            await self.hub.publish(
                DataMessage(
                    type=MessageType.ORDER_BOOK,
                    source=self.name,
                    timestamp=ts,
                    symbols=[symbol],
                    data=book.model_dump(mode="json"),
                )
            )
