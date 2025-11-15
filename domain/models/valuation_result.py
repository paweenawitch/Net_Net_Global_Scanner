# domain/models/valuation_result.py
from dataclasses import dataclass
from typing import Optional, List

@dataclass
class ValuationResult:
    # --- Identity / listing info ---
    ticker: str
    exchange: Optional[str]
    country_iso: Optional[str]
    sector: Optional[str]
    industry: Optional[str]

    # Trading / quote currency for this equity.
    # netnet_analysis maps listing_ccy_for_ticker(core) into this field.
    reporting_currency: Optional[str]

    # Latest financial statement date we actually used (string from core period, e.g. "2024-12-31")
    latest_fs_date: Optional[str]

    # --- Balance sheet / solvency / liquidity ---
    current_ratio: Optional[float]        # current assets / current liabilities
    debt_to_equity: Optional[float]       # total liabilities / (assets - liabilities)

    # --- NCAV math ---
    ncav_total_native: Optional[float]    # Net Current Asset Value in native FS currency
    ncav_total_usd: Optional[float]       # Same NCAV converted to USD
    ncav_per_share: Optional[float]       # NCAV per share (recomputed from shares_out)
    ncav_ps_shortlist: Optional[float]    # ncav_ps that came from shortlist/meta, if any
    shares_out: Optional[float]           # share count used to compute NCAV/share

    # --- Price snapshot & valuation ---
    last_price: Optional[float]           # latest market price (in reporting_currency)
    price_to_ncavps: Optional[float]      # last_price / ncav_per_share
    margin_of_safety: Optional[float]     # 1 - price_to_ncavps

    # --- FX diagnostic / bookkeeping ---
    fx_rate_used: Optional[float]         # reporting_currency -> USD rate actually applied
    fx_source: Optional[str]              # e.g. "cache", "fetched", etc.
    ncavps_fx_note: Optional[str]         # any comment like "fallback rate", etc.

    # --- NCAV change over time (burn or growth) ---
    # percentage change in total NCAV across paired periods
    ncav_change_qoq: Optional[float]      # quarter-over-quarter %
    ncav_change_hoh: Optional[float]      # half-over-half (≈6mo) %
    ncav_change_yoy: Optional[float]      # year-over-year %

    # --- Dilution / buyback tracking ---
    dilution_qoq: Optional[float]         # % change in shares_out QoQ (positive = issued)
    dilution_hoh: Optional[float]         # % change in shares_out HoH
    dilution_yoy: Optional[float]         # % change in shares_out YoY

    max_dilution_1y: Optional[float]      # worst (most positive) issuance in ~1y window
    max_issue_3y: Optional[float]         # worst issuance within ~3y
    max_buyback_3y: Optional[float]       # best buyback (most negative change) within ~3y

    # --- Data quality / recency ---
    is_outdated: bool                     # True if FS is considered stale
    data_age_days: Optional[int]          # age of the latest FS in days
    fs_source: Optional[str]              # e.g. "quarterly", "annual", etc.
    note: Optional[str]                   # any diagnostic note from screening / fetch

    # --- Signals / flags ---
    insider_signal: Optional[str]         # "Buy", "Sell", "Net Buy", "Net Sell", etc.
    green_flags: List[str]                # positives like "Trading ≤ 2/3 NCAV"
    red_flags: List[str]                  # risks like "Issued >8% in last 12m"

    # --- Debug / tracing ---
    core_period_count: Optional[int]      # how many financial periods we loaded
    insider_records: Optional[float]      # we currently pass total_buy_trades (can be float-y)
    latest_period_label: Optional[str]    # e.g. "2024-12-31"
    listing_note: Optional[str]           # ADR board / GEM board note etc.

    # --- Convenience booleans for UI / downstream sorting ---
    passes_price_to_ncav_rule: Optional[bool]  # True if price_to_ncavps <= ~0.67
    has_recent_buyback: Optional[bool]         # True if buyback in last 3y < -5%
    has_recent_dilution: Optional[bool]        # True if issuance in last 3y > 8%
