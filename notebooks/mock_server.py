"""
mock_server.py — synthetic data emitter for notebook testing.

Emits all message types on realistic schedules so every notebook cell
can be validated without external network access.
"""
import asyncio
import json
import math
import random
import signal
from datetime import date, datetime, timezone

import websockets

HOST = "localhost"
PORT = 8765

SYMBOLS = ["RELIANCE", "TCS", "INFY", "HDFCBANK", "ICICIBANK"]

BASE_PRICES = {
    "RELIANCE": 2850.0,
    "TCS": 3920.0,
    "INFY": 1780.0,
    "HDFCBANK": 1620.0,
    "ICICIBANK": 1240.0,
}

# Connected clients: ws → set of subscribed channel strings (empty = all)
_clients: dict[object, set[str]] = {}


def _now():
    return datetime.now(tz=timezone.utc).isoformat()


def _msg(type_, symbols, data):
    return json.dumps({"type": type_, "source": "mock", "timestamp": _now(),
                        "symbols": symbols, "data": data}).encode()


# ── Generators ────────────────────────────────────────────────────────────

async def _emit_price_ticks():
    """Emit a price tick for each symbol every 0.5 s with a random walk."""
    prices = dict(BASE_PRICES)
    opens  = dict(BASE_PRICES)
    vols   = {s: random.randint(500_000, 2_000_000) for s in SYMBOLS}
    t = 0
    while True:
        for sym in SYMBOLS:
            # Sinusoidal drift + noise
            drift = math.sin(t / 30) * 0.3
            chg = random.gauss(drift, 0.15)
            prices[sym] = max(prices[sym] * (1 + chg / 100), 1.0)
            vols[sym] += random.randint(-10_000, 50_000)
            ltp = round(prices[sym], 2)
            prev = opens[sym]
            tick = {
                "symbol": sym, "exchange": "NSE",
                "ltp": ltp,
                "change": round(ltp - prev, 2),
                "change_pct": round((ltp - prev) / prev * 100, 3),
                "open": round(prev, 2),
                "high": round(max(ltp, prev) * 1.002, 2),
                "low":  round(min(ltp, prev) * 0.998, 2),
                "prev_close": round(prev, 2),
                "volume": max(vols[sym], 0),
                "avg_price": round((ltp + prev) / 2, 2),
                "bid": round(ltp - 0.05, 2), "ask": round(ltp + 0.05, 2),
                "bid_qty": random.randint(100, 1000),
                "ask_qty": random.randint(100, 1000),
                "trade_time": _now(),
            }
            await _broadcast("price_tick", [sym], tick)
        t += 1
        await asyncio.sleep(0.5)


async def _emit_indices():
    """Emit Nifty 50 index every 2 s."""
    base = 22500.0
    t = 0
    while True:
        val = base + math.sin(t / 20) * 150 + random.gauss(0, 20)
        idx = {
            "name": "NIFTY 50", "value": round(val, 2),
            "change": round(val - base, 2),
            "change_pct": round((val - base) / base * 100, 3),
            "open": base, "high": base + 200, "low": base - 100,
            "advances": random.randint(25, 45),
            "declines": random.randint(5, 25),
            "unchanged": random.randint(0, 5),
            "timestamp": _now(),
        }
        await _broadcast("index_data", ["NIFTY_50"], idx)
        t += 1
        await asyncio.sleep(2)


async def _emit_market_status():
    """Emit market status once on startup and every 60 s."""
    ms = {"market": "NSE", "status": "open", "message": "Mon Apr 21 2026", "timestamp": _now()}
    await _broadcast("market_status", [], ms)
    while True:
        await asyncio.sleep(60)
        ms["timestamp"] = _now()
        await _broadcast("market_status", [], ms)


async def _emit_news():
    """Emit a news item every 8 s."""
    templates = [
        ("RELIANCE", "Reliance Industries Q4 profit up 18% YoY to Rs 21,930 cr", "nse_announcement", "corporate"),
        ("TCS",      "TCS bags $2.5bn multi-year deal from UK retailer", "et", "macro"),
        ("INFY",     "Infosys raises FY26 revenue guidance to 5-7%", "mc", "corporate"),
        ("HDFCBANK", "HDFC Bank Q4 NII grows 10% YoY; asset quality stable", "bs", "corporate"),
        ("ICICIBANK","ICICI Bank board approves Rs 5 per share dividend", "nse_announcement", "corporate"),
        ("",         "RBI keeps repo rate unchanged at 6.5%; stance stays withdrawal of accommodation", "et", "macro"),
        ("",         "India FY26 GDP forecast revised up to 6.8% by IMF", "mc", "macro"),
        ("RELIANCE", "Reliance Retail to acquire Metro Cash & Carry India operations", "bs", "corporate"),
    ]
    i = 0
    while True:
        sym, title, src, cat = templates[i % len(templates)]
        news = {
            "title": title, "summary": title + ". Details awaited.",
            "url": f"https://example.com/news/{i}",
            "source": src, "symbols": [sym] if sym else [],
            "published_at": _now(), "category": cat, "sentiment": None,
        }
        await _broadcast("news", [sym] if sym else [], news)
        i += 1
        await asyncio.sleep(8)


async def _emit_gdelt():
    """Emit a GDELT article every 20 s."""
    articles = [
        ("RBI policy stance weighs on Indian market sentiment", -1.2, ["ECON_STOCKMARKET"]),
        ("India manufacturing PMI hits 3-month high boosting equities", 3.5, ["ECON_STOCKMARKET"]),
        ("Govt unveils $12bn infrastructure push — Adani, L&T seen as key beneficiaries", 4.1, ["EPU_POLICY_INDIA"]),
        ("Moody's upgrades India outlook to positive citing fiscal consolidation", 5.8, ["ECON_DEBT"]),
        ("Global recession fears drag Asian markets including NSE Nifty lower", -3.1, ["ECON_FRICTIONS"]),
    ]
    i = 0
    while True:
        title, tone, themes = articles[i % len(articles)]
        art = {
            "url": f"https://example.com/gdelt/{i}", "title": title,
            "seendate": _now(), "domain": "example.com",
            "language": "English", "tone": tone,
            "themes": themes, "organisations": [], "locations": ["India"],
            "persons": [], "symbols": [], "image_url": "",
        }
        await _broadcast("gdelt_event", [], art)
        i += 1
        await asyncio.sleep(20)


async def _emit_corporate_events():
    """Emit one corporate event per symbol on startup."""
    events = [
        ("RELIANCE", "dividend",   {"dividend_amount": 10.0, "dividend_type": "final",
                                    "ex_date": "2026-04-25", "description": "Dividend - Rs 10 Per Share"}),
        ("TCS",      "bonus",      {"bonus_ratio": "1:1", "ex_date": "2026-05-10",
                                    "description": "Bonus Issue 1:1"}),
        ("INFY",     "earnings",   {"result_type": "Q4", "description": "Board meeting to consider Q4FY26 results",
                                    "announced_date": "2026-04-17"}),
        ("HDFCBANK", "split",      {"split_ratio": "2:1", "ex_date": "2026-06-01",
                                    "description": "Stock Split 2:1"}),
    ]
    for sym, etype, extra in events:
        ev = {"symbol": sym, "exchange": "NSE", "event_type": etype,
              "source_url": "", **extra}
        await _broadcast("corporate_event", [sym], ev)
        await asyncio.sleep(0.1)
    await asyncio.sleep(300)  # then repeat every 5 min


async def _emit_insider_trades():
    """Emit bulk deals and insider trades every 15 s."""
    bulk = [
        ("RELIANCE", "BUY",  "LIC OF INDIA",       2_500_000, 2842.50),
        ("TCS",      "SELL", "HDFC MF",            1_200_000, 3915.00),
        ("INFY",     "BUY",  "SBI MUTUAL FUND",      800_000, 1775.00),
        ("HDFCBANK", "BUY",  "ICICI PRUDENTIAL MF", 1_500_000, 1618.00),
    ]
    insider = [
        ("RELIANCE", "Mukesh D Ambani",    "Promoter", "Buy",  50000, 142_500_000.0),
        ("TCS",      "N Chandrasekaran",   "Director", "Sell",  5000,  19_600_000.0),
    ]
    i = 0
    while True:
        # bulk deal
        sym, side, client, qty, price = bulk[i % len(bulk)]
        deal = {"symbol": sym, "exchange": "NSE", "client_name": client,
                "deal_type": side, "quantity": qty, "price": price,
                "value": qty * price, "deal_date": str(date.today())}
        await _broadcast("bulk_deal", [sym], deal)

        # insider every other cycle
        if i % 2 == 0:
            sym, name, cat, tx, shares, val = insider[(i // 2) % len(insider)]
            it = {"symbol": sym, "exchange": "NSE", "acquirer_name": name,
                  "acquirer_category": cat, "transaction_type": tx,
                  "shares": shares, "value": val,
                  "pre_holding_pct": 50.2, "post_holding_pct": 50.4,
                  "trade_date": str(date.today()), "disclosure_date": str(date.today()),
                  "source_url": "https://www.sebi.gov.in/"}
            await _broadcast("insider_trade", [sym], it)

        i += 1
        await asyncio.sleep(15)


async def _emit_shareholding():
    """Emit shareholding pattern for each symbol on startup, then every 30 s."""
    patterns = [
        ("RELIANCE",  50.49, 0.0,  22.81, 14.20, 9.12, 12.50),
        ("TCS",       72.19, 0.0,  12.40,  8.30, 6.20,  7.11),
        ("INFY",      14.77, 0.0,  34.12, 20.15, 14.30, 30.96),
        ("HDFCBANK",   0.00, 0.0,  27.60, 24.30, 18.20, 48.10),
        ("ICICIBANK",  0.00, 0.0,  43.50, 16.80, 12.40, 39.70),
    ]
    i = 0
    while True:
        sym, promo, pledged, fii, dii, mf, pub = patterns[i % len(patterns)]
        sp = {
            "symbol": sym, "exchange": "NSE", "quarter": "Dec 2024",
            "as_of": _now(),
            "promoter_pct": promo, "promoter_pledged_pct": pledged,
            "fii_pct": fii, "dii_pct": dii, "mutual_fund_pct": mf,
            "public_pct": pub, "total_shares": random.randint(50_000_000, 5_000_000_000),
        }
        await _broadcast("shareholding_pattern", [sym], sp)
        i += 1
        await asyncio.sleep(30)


async def _emit_director_changes():
    """Emit a director change every 60 s (rare events in practice)."""
    changes = [
        ("RELIANCE",  "Hital R Meswani",   "Executive Director",  "00001699", "appointment"),
        ("TCS",       "K Krithivasan",      "CEO & MD",            "08452290", "appointment"),
        ("INFY",      "Salil Parekh",       "CEO & MD",            "01876159", "appointment"),
        ("HDFCBANK",  "Sashidhar Jagdishan","MD & CEO",            "00004514", "appointment"),
    ]
    i = 0
    first = True
    while True:
        if not first:
            await asyncio.sleep(60)
        first = False
        sym, name, desig, din, ctype = changes[i % len(changes)]
        dc = {
            "symbol": sym, "exchange": "NSE", "name": name,
            "designation": desig, "din": din, "change_type": ctype,
            "effective_date": None, "announced_at": _now(),
        }
        await _broadcast("director_change", [sym], dc)
        i += 1


# ── Hub ───────────────────────────────────────────────────────────────────

async def _broadcast(type_, symbols, data):
    if not _clients:
        return
    payload = json.dumps({"type": type_, "source": "mock",
                           "timestamp": _now(), "symbols": symbols, "data": data}).encode()
    dead = []
    for ws, channels in list(_clients.items()):
        # Respect subscription: empty set = all channels
        if channels and type_ not in channels:
            continue
        try:
            await ws.send(payload)
        except Exception:
            dead.append(ws)
    for ws in dead:
        _clients.pop(ws, None)


async def _handler(ws):
    _clients[ws] = set()
    try:
        await ws.send(json.dumps({"type": "info", "message": "connected to mock server"}).encode())
        async for raw in ws:
            msg = json.loads(raw)
            action = msg.get("action", "")
            if action == "ping":
                await ws.send(json.dumps({"type": "pong"}).encode())
            elif action == "subscribe":
                _clients[ws].update(msg.get("channels", []))
            elif action == "unsubscribe":
                _clients[ws] -= set(msg.get("channels", []))
            await ws.send(json.dumps({"type": "ack", "message": "ok"}).encode())
    except Exception:
        pass
    finally:
        _clients.pop(ws, None)


async def main():
    stop = asyncio.Event()
    for sig in (signal.SIGINT, signal.SIGTERM):
        asyncio.get_event_loop().add_signal_handler(sig, stop.set)

    async with websockets.serve(_handler, HOST, PORT):
        print(f"Mock server running on ws://{HOST}:{PORT}")
        await asyncio.gather(
            _emit_price_ticks(),
            _emit_indices(),
            _emit_market_status(),
            _emit_news(),
            _emit_gdelt(),
            _emit_corporate_events(),
            _emit_insider_trades(),
            _emit_shareholding(),
            _emit_director_changes(),
            stop.wait(),
            return_exceptions=True,
        )

if __name__ == "__main__":
    asyncio.run(main())
