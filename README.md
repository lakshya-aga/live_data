# live-data — Indian equity market data server

A WebSocket aggregation server that pulls live data from multiple sources and fans it out to trading strategies. Strategies connect to one endpoint and receive a filtered stream of typed messages — price ticks, news, GDELT sentiment, insider deals, shareholding patterns, and more.

```
┌─────────────────────────────────────────────────────────┐
│                     Data sources                        │
│  GROWW WS ─┐                                            │
│  NSE REST ─┤                                            │
│  GDELT ────┼──► Hub (queue + subscription filter) ─────┼──► Strategy A (WS)
│  News RSS ─┤                                            │──► Strategy B (WS)
│  SEBI ─────┤                                            │──► Strategy C (WS)
│  NSE corp ─┘                                            │
└─────────────────────────────────────────────────────────┘
```

---

## Data channels

| Channel | Source | Frequency |
|---|---|---|
| `price_tick` | GROWW WebSocket / NSE REST fallback | ~0.5 s |
| `index_data` | NSE REST | ~5 s |
| `market_status` | NSE REST | on change |
| `news` | NSE/BSE announcements + 5 RSS feeds | 60 s poll |
| `gdelt_event` | GDELT V2 doc API | 5 min poll |
| `financial_statement` | Screener.in (optional) | 1 h poll |
| `corporate_event` | NSE corporate actions | 5 min poll |
| `insider_trade` | SEBI insider trading disclosures | 1 h poll |
| `bulk_deal` | NSE bulk deals | 15 min poll |
| `block_deal` | NSE block deals | 15 min poll |
| `politician_disclosure` | MyNeta.info (ECI affidavits) | daily |
| `shareholding_pattern` | NSE quarterly filings | 6 h poll |
| `director_change` | NSE board composition delta | daily |

---

## Quickstart (local Python)

```bash
git clone https://github.com/lakshya-aga/live_data.git
cd live_data

python -m venv .venv && source .venv/bin/activate
pip install -e .

cp .env.example .env
# Fill in GROWW_API_KEY / GROWW_API_SECRET (optional — NSE fallback works without them)

data-server
# WebSocket now listening on ws://localhost:8765
```

Open the walkthrough notebook in a second terminal:

```bash
pip install jupyter pandas matplotlib nest_asyncio
jupyter notebook notebooks/strategy_walkthrough.ipynb
```

---

## Docker deployment

### Option 1 — pull the pre-built image (recommended)

```bash
# Pull the latest image built by CI
docker pull ghcr.io/lakshya-aga/live-data:main

# Run with your credentials
docker run -d \
  --name live-data \
  -p 8765:8765 \
  -e GROWW_API_KEY=<your_key> \
  -e GROWW_API_SECRET=<your_secret> \
  -v $(pwd)/data:/app/data \
  ghcr.io/lakshya-aga/live-data:main
```

### Option 2 — docker compose (with persistent data volume)

```bash
cp .env.example .env   # fill in credentials
docker compose up -d
```

Logs:

```bash
docker compose logs -f live-data
```

Stop:

```bash
docker compose down
```

### Option 3 — build locally

```bash
docker build -t live-data .
docker run -d -p 8765:8765 --env-file .env live-data
```

---

## CI/CD

### `build-push.yml` — automatic image build

Triggers automatically on **every push to any branch** and on version tags:

| Event | Image tags produced |
|---|---|
| Push to any branch | `:<branch-name>`, `:sha-<short>` |
| Version tag `v1.2.3` | `:1.2.3`, `:1.2`, `:latest`, `:sha-<short>` |

The image is published to `ghcr.io/lakshya-aga/live-data` and visible under **Packages** on the GitHub repository page after each push.  
No secrets are needed — it uses `GITHUB_TOKEN` automatically.

To release a pinned version:

```bash
git tag v1.0.0
git push origin v1.0.0
```

### `run-server.yml` — manually triggered run

Go to **Actions → Run data server → Run workflow** in the GitHub UI.

Inputs:

| Input | Default | Description |
|---|---|---|
| `image_tag` | `main` | Which tag to pull and run |
| `duration` | `120` | Seconds to keep the server alive |
| `log_level` | `INFO` | Log verbosity |

The workflow:
1. Pulls the specified image
2. Starts the container
3. Runs a WebSocket smoke test (ping → pong)
4. Keeps the server alive for `duration` seconds (useful on a self-hosted runner with exposed ports)
5. Prints logs and stops the container

**On a self-hosted runner:** if your runner runs on a VPS or workstation with a public IP, the server's port 8765 is reachable at `<runner-host>:8765` for the duration of the job. Point your strategies there.

**Required repository secrets** (Settings → Secrets → Actions):

| Secret | Required | Notes |
|---|---|---|
| `GROWW_API_KEY` | No | NSE fallback works without it |
| `GROWW_API_SECRET` | No | |
| `SCREENER_API_KEY` | No | Enables Screener.in financial data |

---

## Writing a strategy

Subclass `BaseStrategy`, declare which channels and symbols you want, and override `on_*` handlers:

```python
# examples/momentum.py
import asyncio
from client.base import BaseStrategy
from server.models.base import MessageType
from server.models.price import PriceTick

class MomentumStrategy(BaseStrategy):
    channels = [MessageType.PRICE_TICK, MessageType.NEWS]
    symbols  = ["RELIANCE", "TCS", "INFY"]

    async def on_price_tick(self, tick: PriceTick) -> None:
        if tick.change_pct > 2.0:
            print(f"Strong move: {tick.symbol} {tick.change_pct:+.2f}%  ltp={tick.ltp}")

    async def on_news(self, item) -> None:
        print(f"[news] {item.source}: {item.title}")

asyncio.run(MomentumStrategy().run())
```

The strategy auto-reconnects on disconnect. It only receives messages matching both `channels` and `symbols` filters (empty `symbols` = all symbols).

---

## Environment variables

Copy `.env.example` to `.env` and fill in values. All variables are optional except where noted.

| Variable | Default | Description |
|---|---|---|
| `HOST` | `0.0.0.0` | Bind address for the WebSocket server |
| `PORT` | `8765` | WebSocket port |
| `GROWW_API_KEY` | | GROWW developer API key |
| `GROWW_API_SECRET` | | GROWW developer API secret |
| `NSE_POLL_INTERVAL` | `5` | Seconds between NSE quote polls |
| `GDELT_POLL_INTERVAL` | `300` | Seconds between GDELT polls |
| `NEWS_POLL_INTERVAL` | `60` | Seconds between news polls |
| `BULK_BLOCK_POLL_INTERVAL` | `900` | Seconds between bulk/block deal polls |
| `SEBI_INSIDER_POLL_INTERVAL` | `3600` | Seconds between SEBI insider filing polls |
| `SHAREHOLDING_POLL_INTERVAL` | `21600` | Seconds between shareholding pattern polls |
| `BOARD_POLL_INTERVAL` | `86400` | Seconds between board composition polls |
| `SCREENER_API_KEY` | | Screener.in API key (enables financial statement data) |
| `WATCHLIST` | Nifty-50 subset | Comma-separated NSE symbols to track |
| `LOG_LEVEL` | `INFO` | `DEBUG` / `INFO` / `WARNING` |
| `LOG_FORMAT` | `console` | `console` (human) or `json` (structured) |

---

## Running tests

```bash
pip install -e ".[dev]"
pytest tests/ -v
```

See the [test guide](notebooks/strategy_walkthrough.ipynb) for a full list of what to test and sample test snippets for the hub, subscription filter, shareholding parser, and source backoff logic.
