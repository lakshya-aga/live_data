from __future__ import annotations

from datetime import date, datetime
from pydantic import BaseModel


class InsiderTrade(BaseModel):
    """SEBI SAST/Insider Trading disclosures (Form C/D/E/F)."""
    symbol: str
    exchange: str = "NSE"
    acquirer_name: str
    acquirer_category: str = ""   # "Promoter", "Director", "KMP", etc.
    transaction_type: str         # "Buy", "Sell", "Pledge", "Revoke"
    shares: int | None = None
    value: float | None = None    # INR
    pre_holding_pct: float | None = None
    post_holding_pct: float | None = None
    trade_date: date | None = None
    disclosure_date: date | None = None
    source_url: str = ""


class BulkDeal(BaseModel):
    """NSE/BSE bulk deal (>=0.5% of total listed shares in a single trade)."""
    symbol: str
    exchange: str
    client_name: str
    deal_type: str          # "BUY" or "SELL"
    quantity: int
    price: float            # Weighted average price
    value: float | None = None
    deal_date: date


class BlockDeal(BaseModel):
    """NSE/BSE block deal (negotiated large trade on block-deal window)."""
    symbol: str
    exchange: str
    client_name: str
    deal_type: str          # "BUY" or "SELL"
    quantity: int
    price: float
    value: float | None = None
    deal_date: date


class FIIDIIFlow(BaseModel):
    """Daily FII/DII net investment data published by NSE/SEBI."""
    date: date
    fii_buy: float          # INR crore
    fii_sell: float
    fii_net: float
    dii_buy: float
    dii_sell: float
    dii_net: float
    segment: str = "equity"  # "equity" or "debt"


class PoliticianDisclosure(BaseModel):
    """
    Parliament member asset/shareholding declaration.
    Source: loksabha.nic.in / rajyasabha.nic.in annual declarations.
    """
    member_name: str
    house: str              # "Lok Sabha" or "Rajya Sabha"
    constituency: str = ""
    party: str = ""
    symbol: str | None = None    # If a specific listed company is mentioned
    company_name: str = ""
    shares: int | None = None
    value: float | None = None   # INR
    declaration_year: int | None = None
    source_url: str = ""
    fetched_at: datetime | None = None
