from __future__ import annotations

import asyncio
import logging
from collections import deque
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Deque

from .models.base import DataMessage, MessageType, ServerResponse, SubscribeMessage

if TYPE_CHECKING:
    import websockets

logger = logging.getLogger(__name__)


# Per-channel replay buffer sizes. When a client connects the hub replays the
# most recent N messages of each subscribed channel so the UI never has to
# stare at an empty news/gdelt panel waiting for the next poll cycle. Sizes
# are tuned so a fresh connection sees enough context (a screen's worth of
# news / GDELT) without flooding the client with stale ticks.
_REPLAY_SIZES: dict[MessageType, int] = {
    MessageType.PRICE_TICK: 20,             # ~latest tick per watched symbol
    MessageType.INDEX_DATA: 6,              # 2× the 3-index whitelist
    MessageType.MARKET_STATUS: 1,
    MessageType.NEWS: 50,
    MessageType.GDELT_EVENT: 50,
    MessageType.FINANCIAL_STATEMENT: 30,
    MessageType.CORPORATE_EVENT: 30,
    MessageType.INSIDER_TRADE: 30,
    MessageType.BULK_DEAL: 30,
    MessageType.BLOCK_DEAL: 30,
    MessageType.SHAREHOLDING_PATTERN: 30,
    MessageType.DIRECTOR_CHANGE: 30,
}
_DEFAULT_REPLAY_SIZE = 20


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
        # Per-channel replay buffer. Keyed by MessageType so each channel can
        # have a different retention policy (see _REPLAY_SIZES above).
        self._history: dict[MessageType, Deque[DataMessage]] = {}

    # ── Called by data sources ────────────────────────────────────────────

    async def publish(self, message: DataMessage) -> None:
        # Append to the replay buffer first — even if the broadcast queue is
        # saturated, late-connecting clients will still see this message when
        # they subscribe.
        buf = self._history.get(message.type)
        if buf is None:
            size = _REPLAY_SIZES.get(message.type, _DEFAULT_REPLAY_SIZE)
            buf = deque(maxlen=size)
            self._history[message.type] = buf
        buf.append(message)
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
            newly_subscribed: set[MessageType] = set()
            if msg.action == "subscribe":
                # Track which channels are new on *this* subscribe so we only
                # replay history for those, not the full set every time.
                newly_subscribed = set(msg.channels) - client.channels
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

            # Replay buffered messages for newly-subscribed channels so the
            # client doesn't have to wait for the next poll cycle to see
            # anything. Filtered by symbols if the client requested a subset.
            if newly_subscribed:
                await self._replay_history(client, newly_subscribed)

    # ── Replay buffer ────────────────────────────────────────────────────

    async def _replay_history(
        self,
        client: _Client,
        channels: set[MessageType],
    ) -> None:
        """Send buffered messages for the given channels to one client.

        Messages are sent oldest-first (deque iteration order) so the client's
        UI sees a coherent timeline. Symbol filters on the client are honoured
        — if the client subscribed to specific symbols, replay is filtered
        the same way live broadcast is.
        """
        sent = 0
        for ch in channels:
            buf = self._history.get(ch)
            if not buf:
                continue
            for message in list(buf):
                if not client.wants(message):
                    continue
                try:
                    await client.ws.send(message.to_json())
                    sent += 1
                except Exception as exc:
                    logger.debug("Replay send error: %s", exc)
                    return
        if sent:
            logger.info("Replayed %d buffered messages to new subscriber", sent)

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
