"""
Data server entry point.

Starts all data source workers as background asyncio tasks, then opens a
WebSocket server on HOST:PORT.  Strategies connect to this server and receive
a filtered stream of DataMessage objects.

Usage
-----
  python -m server.main            # from repo root
  data-server                      # if installed via pip install -e .

Environment / config: see .env.example
"""

from __future__ import annotations

import asyncio
import logging
import signal
import sys

import structlog
import websockets

from .config import settings
from .hub import Hub
from .sources import (
    CompanySource,
    CorporateSource,
    FinancialsSource,
    GdeltSource,
    GrowwSource,
    HoldingsSource,
    NewsSource,
    NSESource,
)

logger = structlog.get_logger(__name__)


class _DropTcpProbeHandshakeFailures(logging.Filter):
    """Drop the multi-frame stack trace produced by TCP-only health probes.

    Render / Railway / Cloud Run / Kubernetes liveness probes open a TCP
    socket and close it without sending an HTTP request line. The websockets
    library treats that as an `InvalidMessage` and dumps the entire
    `EOFError → InvalidMessage → opening handshake failed` traceback at
    ERROR level, once per probe — about 60–120 times an hour on most hosts.
    The connection itself is not a problem (no client could ever consume
    this socket anyway), it's just log noise. We filter out exactly this
    exception class so genuine handshake failures still surface.
    """

    def filter(self, record: logging.LogRecord) -> bool:
        exc = record.exc_info[1] if record.exc_info else None
        if exc is None:
            return True
        # Match by class name to avoid importing websockets.exceptions twice
        # in case the library reorganises submodules between minor versions.
        return type(exc).__name__ != "InvalidMessage"


def _configure_logging() -> None:
    level = getattr(logging, settings.log_level.upper(), logging.INFO)
    processors = [
        structlog.contextvars.merge_contextvars,
        structlog.processors.add_log_level,
        structlog.processors.TimeStamper(fmt="iso"),
    ]
    if settings.log_format == "json":
        processors.append(structlog.processors.JSONRenderer())
    else:
        processors.append(structlog.dev.ConsoleRenderer())

    structlog.configure(
        processors=processors,
        wrapper_class=structlog.make_filtering_bound_logger(level),
        context_class=dict,
        logger_factory=structlog.PrintLoggerFactory(),
    )
    logging.basicConfig(level=level, stream=sys.stdout)
    # Silence noisy libraries
    for noisy in ("aiohttp", "websockets", "feedparser", "httpx"):
        logging.getLogger(noisy).setLevel(logging.WARNING)
    # Drop the TCP-probe traceback floods specifically.
    logging.getLogger("websockets.server").addFilter(_DropTcpProbeHandshakeFailures())


async def _main() -> None:
    _configure_logging()
    hub = Hub()

    sources = [
        GrowwSource(hub),    # Sole price source: live GROWW WebSocket
        NSESource(hub),      # Market status + indices only — no price ticks
        GdeltSource(hub),
        NewsSource(hub),
        FinancialsSource(hub),
        CorporateSource(hub),
        HoldingsSource(hub),
        CompanySource(hub),      # Shareholding patterns + director changes
    ]

    # Start all source tasks
    source_tasks = [src.start() for src in sources]
    # Start hub broadcast loop
    hub_task = asyncio.create_task(hub.run(), name="hub-broadcast")

    logger.info("data-server starting", host=settings.host, port=settings.port)

    async def _ws_handler(ws) -> None:
        await hub.handle_connection(ws)

    stop_event = asyncio.Event()

    def _shutdown(sig_name: str) -> None:
        logger.info("shutdown signal received", signal=sig_name)
        stop_event.set()

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, _shutdown, sig.name)
        except NotImplementedError:
            pass  # Windows

    async with websockets.serve(_ws_handler, settings.host, settings.port):
        logger.info(
            "WebSocket server ready",
            url=f"ws://{settings.host}:{settings.port}",
            sources=[s.name for s in sources],
        )
        await stop_event.wait()

    logger.info("shutting down sources")
    for src in sources:
        await src.stop()
    hub_task.cancel()
    await asyncio.gather(*source_tasks, hub_task, return_exceptions=True)
    logger.info("shutdown complete")


def run() -> None:
    asyncio.run(_main())


if __name__ == "__main__":
    run()
