from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

from .models.base import DataMessage, MessageType, ServerResponse, SubscribeMessage

if TYPE_CHECKING:
    import websockets

logger = logging.getLogger(__name__)


@dataclass(eq=False)   # use object identity for hashing so _Client is set-safe
class _Client:
    ws: object
    channels: set[MessageType] = field(default_factory=set)
    symbols: set[str] = field(default_factory=set)   # empty = all symbols

    def wants(self, msg: DataMessage) -> bool:
        if self.channels and msg.type not in self.channels:
            return False
        if self.symbols and msg.symbols and not (self.symbols & set(msg.symbols)):
            return False
        return True


class Hub:
    """
    Central hub:
    - Data sources call hub.publish() to push DataMessage objects.
    - WebSocket strategy clients connect, send SubscribeMessage, receive DataMessages.
    """

    def __init__(self) -> None:
        self._clients: set[_Client] = set()
        self._lock = asyncio.Lock()
        self._queue: asyncio.Queue[DataMessage] = asyncio.Queue(maxsize=50_000)

    # ── Called by data sources ────────────────────────────────────────────

    async def publish(self, message: DataMessage) -> None:
        try:
            self._queue.put_nowait(message)
        except asyncio.QueueFull:
            logger.warning("Hub queue full — dropping %s message", message.type)

    # ── WebSocket handler (one coroutine per connected strategy) ─────────

    async def handle_connection(self, ws) -> None:
        client = _Client(ws=ws)
        async with self._lock:
            self._clients.add(client)

        addr = getattr(ws, "remote_address", "unknown")
        logger.info("Strategy connected: %s", addr)
        ack = ServerResponse(type="info", message="connected to live-data hub")
        try:
            await ws.send(ack.to_json())
            await self._read_subscriptions(client)
        except Exception as exc:
            logger.debug("Connection from %s closed: %s", addr, exc)
        finally:
            async with self._lock:
                self._clients.discard(client)
            logger.info("Strategy disconnected: %s", addr)

    async def _read_subscriptions(self, client: _Client) -> None:
        import orjson

        async for raw in client.ws:
            try:
                data = orjson.loads(raw)
                msg = SubscribeMessage(**data)
            except Exception as exc:
                err = ServerResponse(type="error", message=str(exc))
                await client.ws.send(err.to_json())
                continue

            if msg.action == "ping":
                await client.ws.send(ServerResponse(type="pong").to_json())
                continue

            upper_syms = {s.upper() for s in msg.symbols}
            if msg.action == "subscribe":
                client.channels.update(msg.channels)
                client.symbols.update(upper_syms)
            elif msg.action == "unsubscribe":
                client.channels -= set(msg.channels)
                client.symbols -= upper_syms

            ack = ServerResponse(
                type="ack",
                message=f"{msg.action} ok — channels={len(client.channels)} symbols={len(client.symbols)}",
            )
            await client.ws.send(ack.to_json())

    # ── Broadcast loop (run as background task) ──────────────────────────

    async def run(self) -> None:
        while True:
            message = await self._queue.get()
            await self._broadcast(message)

    async def _broadcast(self, message: DataMessage) -> None:
        payload = message.to_json()
        async with self._lock:
            targets = [c for c in self._clients if c.wants(message)]
        if not targets:
            return
        results = await asyncio.gather(
            *[c.ws.send(payload) for c in targets],
            return_exceptions=True,
        )
        for exc in results:
            if isinstance(exc, Exception):
                logger.debug("Send error: %s", exc)
