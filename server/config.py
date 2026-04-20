from __future__ import annotations

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict

_DEFAULT_WATCHLIST = [
    "RELIANCE", "TCS", "INFY", "HDFCBANK", "ICICIBANK",
    "KOTAKBANK", "SBIN", "BAJFINANCE", "BHARTIARTL", "LT",
    "ASIANPAINT", "MARUTI", "TITAN", "ULTRACEMCO", "NESTLEIND",
    "WIPRO", "HCLTECH", "TECHM", "POWERGRID", "NTPC",
    "AXISBANK", "ITC", "SUNPHARMA", "DRREDDY", "TATAMOTORS",
    "TATASTEEL", "HINDALCO", "JSWSTEEL", "ONGC", "BPCL",
    "ADANIENT", "ADANIPORTS", "BAJAJFINSV", "BRITANNIA", "CIPLA",
    "DIVISLAB", "EICHERMOT", "GRASIM", "HDFCLIFE", "HEROMOTOCO",
    "INDUSINDBK", "LTIM", "MM", "PIDILITIND", "SBILIFE",
    "SHREECEM", "TATACONSUM", "UPL", "VEDL", "ZOMATO",
]

_DEFAULT_GDELT_THEMES = [
    "ECON_STOCKMARKET",
    "ECON_DEBT",
    "ECON_BANKRUPT",
    "ECON_FRICTIONS",
    "EPU_POLICY_INDIA",
    "GOV_TAXES_GENERAL",
    "CRISISLEX_C07_VIOLENCE_CONFLICT",
]

_DEFAULT_RSS_FEEDS = [
    "https://economictimes.indiatimes.com/markets/rssfeeds/1977021501.cms",
    "https://www.moneycontrol.com/rss/marketreports.xml",
    "https://www.business-standard.com/rss/markets-106.rss",
    "https://feeds.feedburner.com/ndtvprofit-latest",
    "https://www.thehindu.com/business/markets/feeder/default.rss",
]


def _parse_list(v: str | list) -> list[str]:
    if isinstance(v, list):
        return v
    return [x.strip() for x in v.replace(",", " ").split() if x.strip()]


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    # ── Server ──────────────────────────────────────────────────────────
    host: str = "0.0.0.0"
    port: int = 8765

    # ── GROWW ────────────────────────────────────────────────────────────
    groww_api_key: str = ""
    groww_api_secret: str = ""
    groww_access_token: str = ""

    # ── NSE ──────────────────────────────────────────────────────────────
    nse_poll_interval: int = 5   # seconds between quote polls

    # ── GDELT ────────────────────────────────────────────────────────────
    gdelt_poll_interval: int = 300
    gdelt_themes: list[str] = Field(default_factory=lambda: _DEFAULT_GDELT_THEMES)

    # ── News ─────────────────────────────────────────────────────────────
    news_poll_interval: int = 60
    extra_rss_feeds: list[str] = Field(default_factory=list)

    @property
    def rss_feeds(self) -> list[str]:
        return _DEFAULT_RSS_FEEDS + self.extra_rss_feeds

    # ── Financials ───────────────────────────────────────────────────────
    financials_poll_interval: int = 3600
    screener_api_key: str = ""

    # ── Corporate events ─────────────────────────────────────────────────
    corporate_events_poll_interval: int = 300

    # ── Holdings / insider activity ──────────────────────────────────────
    # Bulk and block deals settle intraday — poll frequently during market hours
    bulk_block_poll_interval: int = 900    # 15 minutes
    # SEBI insider trading disclosures are filed within 2 trading days of trade
    sebi_insider_poll_interval: int = 3600  # 1 hour
    # FII/DII data is published once at end of day
    fii_dii_poll_interval: int = 3600      # 1 hour
    # Politician/MyNeta holdings are election-cycle data; once a day is plenty
    politician_holdings_poll_interval: int = 86400

    # ── Watchlist ────────────────────────────────────────────────────────
    watchlist: list[str] = Field(default_factory=lambda: _DEFAULT_WATCHLIST)

    # ── Logging ──────────────────────────────────────────────────────────
    log_level: str = "INFO"
    log_format: str = "console"   # "console" or "json"


settings = Settings()
