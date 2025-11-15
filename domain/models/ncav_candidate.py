#domain/models/ncav_candidate.py --> for evaluating ncav compatible into first shortlist
from dataclasses import dataclass
from typing import Optional


@dataclass
class NcavCandidate:
    """
    Result of phase 1 (build_ncav_shortlist):
    - Fundamental snapshot
    - Price snapshot
    - FX-normalized NCAV/share in trading currency
    - Screen pass based on Graham "price < NCAV/share"
    """

    ticker: str                    # internal house ticker, e.g. "0591.HK" or "ABC.US"
    yahoo_symbol: Optional[str]    # symbol we used for yfinance

    # Market identity
    country: Optional[str]         # from universe CSV if present
    mic: Optional[str]             # primary MIC/venue code if present
    target_ccy: Optional[str]      # trading / quote currency we compare in

    # Fundamentals at snapshot time
    fs_date: Optional[str]         # statement_date we used for NCAV calc
    data_age_days: Optional[int]   # staleness in days
    fs_source: Optional[str]       # "quarterly", "annual", etc.
    note: Optional[str]            # diagnostic from cache/build_or_update

    # Core NCAV math
    ncav_total: Optional[float]    # current assets - total liab (native ccy)
    shares_out: Optional[float]    # shares we divided with
    ncav_ps_native: Optional[float]# NCAV/share in the financial reporting currency
    ncav_ps_target: Optional[float]# same per-share, converted to target_ccy

    # Price snapshot
    last_price: Optional[float]    # latest close in target_ccy
    price_date: Optional[str]      # ISO date of that last_price

    # Valuation ratio
    price_vs_ncavps: Optional[float]  # last_price / ncav_ps_target

    # Quality gates from shortlist build
    within_2y: Optional[bool]      # FS <= max_fs_age_days
    ncav_positive: Optional[bool]  # ncav_total > 0
    ncavps_positive: Optional[bool]# ncav_ps_target > 0
    is_ncav_netnet: Optional[bool] # True if it passed Graham cheapness test (< 1.0)
