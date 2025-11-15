## infrastructure/repositories/ncav_cache_repository.py
from __future__ import annotations
import logging
from typing import Optional, Dict, Any
from application.ports import FundamentalsRepository

# Reuse your existing cache/builder
from tools.ncav_cache import build_or_update, load_cached  # type: ignore

_log = logging.getLogger("shortlist.fundamentals")

class NcavCacheRepository(FundamentalsRepository):
    def get_or_update(self, house_ticker: str, fetch_timeout: int) -> Dict[str, Any]:
        _log.debug("fetching fundamentals (update) for %s", house_ticker)
        rec = build_or_update(house_ticker, fetch_timeout)
        d = rec.__dict__.copy()
        out = {
            "ticker": d.get("ticker"),
            "y_symbol": d.get("y_symbol"),
            "fs_date": d.get("statement_date"),
            "currency": d.get("currency"),
            "assets_current": d.get("assets_current"),
            "liab_total": d.get("liab_total"),
            "ncav": d.get("ncav"),
            "shares_out": d.get("shares_out"),
            "ncav_ps": d.get("ncav_ps"),
            "data_age_days": d.get("data_age_days"),
            "fs_source": d.get("fs_source"),
            "fs_selected_col": d.get("fs_selected_col"),
            "note": d.get("note"),
        }
        _log.debug("fundamentals ready for %s (date=%s, note=%s)", out["ticker"], out["fs_date"], out["note"])
        return out

    def get_cached(self, house_ticker: str) -> Optional[Dict[str, Any]]:
        rec = load_cached(house_ticker)
        if rec is None:
            _log.debug("cache miss: %s", house_ticker)
            return None
        d = rec.__dict__.copy()
        out = {
            "ticker": d.get("ticker"),
            "y_symbol": d.get("y_symbol"),
            "fs_date": d.get("statement_date"),
            "currency": d.get("currency"),
            "assets_current": d.get("assets_current"),
            "liab_total": d.get("liab_total"),
            "ncav": d.get("ncav"),
            "shares_out": d.get("shares_out"),
            "ncav_ps": d.get("ncav_ps"),
            "data_age_days": d.get("data_age_days"),
            "fs_source": d.get("fs_source"),
            "fs_selected_col": d.get("fs_selected_col"),
            "note": d.get("note"),
        }
        _log.debug("cache hit: %s (date=%s)", out["ticker"], out["fs_date"])
        return out
