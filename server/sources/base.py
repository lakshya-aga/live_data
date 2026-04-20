from __future__ import annotations

import asyncio
import logging
from abc import ABC, abstractmethod
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from server.hub import Hub

logger = logging.getLogger(__name__)


class BaseSource(ABC):
    """
    All data sources inherit from this.  Implement `_run()` to fetch/stream
    data and call `self.hub.publish(msg)` for each DataMessage produced.
    """

    name: str = "base"

    def __init__(self, hub: "Hub") -> None:
        self.hub = hub
        self._task: asyncio.Task | None = None

    def start(self) -> asyncio.Task:
        self._task = asyncio.create_task(self._supervised_run(), name=self.name)
        return self._task

    async def stop(self) -> None:
        if self._task and not self._task.done():
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass

    async def _supervised_run(self) -> None:
        """Restart source on unexpected errors with exponential back-off."""
        delay = 1.0
        while True:
            try:
                logger.info("[%s] starting", self.name)
                await self._run()
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logger.error("[%s] crashed: %s — retrying in %.0fs", self.name, exc, delay)
                await asyncio.sleep(delay)
                delay = min(delay * 2, 60)
            else:
                # _run() returned cleanly; restart after a short pause
                await asyncio.sleep(5)

    @abstractmethod
    async def _run(self) -> None: ...
