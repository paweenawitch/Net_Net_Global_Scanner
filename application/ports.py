from __future__ import annotations
from typing import Protocol, Dict, Any, List, Optional, Tuple
from dataclasses import dataclass
import pandas as pd

# =========================
# Universe building
# =========================

class TickerSource(Protocol):
    """Port: fetch a normalized listing table for one market."""
    market_code: str  # e.g. "US", "JP", "HK"

    def fetch(self) -> pd.DataFrame:
        """
        Returns columns: ["ticker_base","ticker","name","country","mic"]
        May be empty but never None.
        """
        ...

class UniverseRepository(Protocol):
    """Port: persist per-market and global universes + metadata/provenance."""
    def write_market(self, market: str, df: pd.DataFrame, meta: Dict[str, Any]) -> None: ...
    def write_global(self, df: pd.DataFrame, meta: Dict[str, Any]) -> None: ...

class UniverseBuilder(Protocol):
    """Use case: orchestrate sources -> repository outputs."""
    def run(self) -> Dict[str, Any]: ...


# =========================
# Shortlist building
# =========================

class ShortlistUniverseRepository(Protocol):
    """Port: read a list of tickers (house tickers) to evaluate for NCAV."""
    def load_tickers(self) -> List[Dict[str, Any]]:
        """
        Return rows with at least {'ticker': str} and optionally
        {'name': str, 'country': str, 'mic': str}.
        """
        ...

class FundamentalsRepository(Protocol):
    """Port: supply NCAV fundamentals, from cache or by fetching/updating."""
    def get_or_update(self, house_ticker: str, fetch_timeout: int) -> Dict[str, Any]:
        """
        Return a dict with keys compatible with shortlist pipeline:
          ticker, y_symbol, fs_date, currency, assets_current, liab_total, ncav,
          shares_out, ncav_ps, data_age_days, fs_source, fs_selected_col, note
        """
        ...

    def get_cached(self, house_ticker: str) -> Optional[Dict[str, Any]]:
        """Return the same dict as above if cached; otherwise None."""
        ...

class PriceClient(Protocol):
    """Port: get latest close prices for Yahoo symbols (throttled/batched internally)."""
    def latest_closes(self, y_symbols: List[str], batch_size: int
                      ) -> Dict[str, Tuple[Optional[float], Optional[str]]]:
        """
        Map yahoo_symbol -> (price, price_date_iso).
        If unavailable, values may be (None, None).
        """
        ...

class FxProvider(Protocol):
    """Port: get FX rates as USD per currency code (e.g., {'USD':1.0,'JPY':0.0067})."""
    def usd_per_ccy(self, currencies: List[str]) -> Dict[str, float]:
        ...

class ShortlistRepository(Protocol):
    """Port: persist shortlist outputs (all rows, filtered shortlist, metadata)."""
    def save_all(self, df: pd.DataFrame): ...
    def save_shortlist(self, df: pd.DataFrame): ...
    def save_meta(self, payload: Dict[str, Any]): ...

@dataclass(frozen=True)
class ShortlistConfig:
    """Configuration for the shortlist use case."""
    max_workers: int = 4
    fetch_timeout: int = 15
    prices_batch: int = 40
    max_fs_age_days: int = 730
    prices_only: bool = False
    limit: Optional[int] = None
