"""
simple_strategy.py — skeleton strategy demonstrating the client API.

This strategy:
  - Subscribes only to PRICE_TICK and NEWS for a focused watchlist
  - Tracks a simple momentum signal (% change from open)
  - Prints a signal when a stock moves >1.5% from open on above-average volume
  - Reacts to news by flagging the relevant symbol

Run:
  python examples/simple_strategy.py [ws://localhost:8765]
"""

from __future__ import annotations

import asyncio
import sys
from collections import defaultdict
from datetime import datetime

from client import BaseStrategy
from server.models.base import MessageType
from server.models.news import NewsItem
from server.models.price import MarketStatus, PriceTick

WATCHLIST = ["RELIANCE", "TCS", "INFY", "HDFCBANK", "ICICIBANK", "SBIN", "BHARTIARTL"]
MOMENTUM_THRESHOLD = 1.5    # % move from open triggers signal
VOLUME_MULTIPLIER = 1.5     # signal only when volume > 1.5× avg


class SimpleStrategy(BaseStrategy):
    channels = [MessageType.PRICE_TICK, MessageType.NEWS, MessageType.MARKET_STATUS]
    symbols = WATCHLIST

    def __init__(self, url: str) -> None:
        super().__init__(server_url=url)
        self._avg_volume: dict[str, float] = {}
        self._volume_samples: dict[str, list[int]] = defaultdict(list)
        self._market_open = False
        self._signal_count = 0

    async def on_market_status(self, status: MarketStatus) -> None:
        was_open = self._market_open
        self._market_open = status.status == "open"
        if not was_open and self._market_open:
            print(f"\n{'='*60}")
            print("  MARKET OPENED — strategy active")
            print(f"{'='*60}\n")

    async def on_price_tick(self, tick: PriceTick) -> None:
        if not self._market_open:
            return

        # Update rolling average volume (last 20 ticks per symbol)
        samples = self._volume_samples[tick.symbol]
        samples.append(tick.volume)
        if len(samples) > 20:
            samples.pop(0)
        avg_vol = sum(samples) / len(samples)
        self._avg_volume[tick.symbol] = avg_vol

        # Momentum signal
        if tick.open and tick.open > 0:
            pct_from_open = (tick.ltp - tick.open) / tick.open * 100
        else:
            pct_from_open = tick.change_pct

        volume_ok = tick.volume >= avg_vol * VOLUME_MULTIPLIER

        if abs(pct_from_open) >= MOMENTUM_THRESHOLD and volume_ok:
            direction = "▲ LONG" if pct_from_open > 0 else "▼ SHORT"
            self._signal_count += 1
            ts = datetime.now().strftime("%H:%M:%S")
            print(
                f"[{ts}] SIGNAL #{self._signal_count:03d}  {direction}  "
                f"{tick.symbol:12s}  "
                f"LTP={tick.ltp:>9.2f}  "
                f"from_open={pct_from_open:+.2f}%  "
                f"vol={tick.volume:>10,}  "
                f"(avg={avg_vol:>10,.0f})"
            )

    async def on_news(self, item: NewsItem) -> None:
        relevant = [s for s in item.symbols if s in WATCHLIST]
        if not relevant:
            return
        ts = datetime.now().strftime("%H:%M:%S")
        print(
            f"[{ts}] NEWS [{','.join(relevant):15s}]  "
            f"[{item.source:15s}]  {item.title[:80]}"
        )

    async def on_connect(self) -> None:
        print(f"\nSimpleStrategy connected to {self.server_url}")
        print(f"Watching: {', '.join(WATCHLIST)}")
        print(f"Signal threshold: >{MOMENTUM_THRESHOLD}% from open + {VOLUME_MULTIPLIER}× avg volume\n")


if __name__ == "__main__":
    url = sys.argv[1] if len(sys.argv) > 1 else "ws://localhost:8765"
    asyncio.run(SimpleStrategy(url).run())
