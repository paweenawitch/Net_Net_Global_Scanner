# infrastructure/fx/exchangerate_host_provider.py

from __future__ import annotations
import json
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Dict

import requests

from application.screening_service import FxProvider

class ExchangerateHostFxProvider(FxProvider):
    """
    Fetch FX from exchangerate.host with base=USD and convert to
    the format domain expects: { 'JPY': rate_JPY_to_USD, ... }.

    exchangerate.host with base=USD returns:
        "rates": { "JPY": 150.0, ... }  # 1 USD = 150 JPY

    We invert that to get:
        "JPY": 1 / 150.0  # 1 JPY = 0.006666... USD
    """

    FX_URL = "https://api.exchangerate.host/latest?base=USD"

    def __init__(self, cache_file: Path, ttl: timedelta | None = None) -> None:
        self._cache_file = cache_file
        self._ttl = ttl or timedelta(hours=24)

    # ---------- Cache helpers ----------

    def _load_cache_raw(self) -> Dict[str, float] | None:
        if not self._cache_file.exists():
            return None
        try:
            st = datetime.fromtimestamp(self._cache_file.stat().st_mtime, tz=timezone.utc)
            if datetime.now(timezone.utc) - st > self._ttl:
                return None
            obj = json.loads(self._cache_file.read_text(encoding="utf-8"))
            return obj.get("rates") or None
        except Exception:
            return None

    def _save_cache_raw(self, rates: Dict[str, float]) -> None:
        try:
            self._cache_file.parent.mkdir(parents=True, exist_ok=True)
            payload = {"rates": rates, "source": "exchangerate.host", "base": "USD"}
            self._cache_file.write_text(json.dumps(payload, indent=2), encoding="utf-8")
        except Exception:
            pass

    # ---------- Remote fetch ----------

    def _fetch_raw(self) -> Dict[str, float]:
        r = requests.get(self.FX_URL, timeout=30)
        r.raise_for_status()
        data = r.json()
        return data.get("rates") or {}

    # ---------- Normalization ----------

    @staticmethod
    def _normalize_ccy_to_usd(usd_base_rates: Dict[str, float]) -> Dict[str, float]:
        """
        Convert {ccy: units_per_USD} -> {ccy: USD_per_unit}.
        Also ensures USD is present at 1.0.
        """
        out: Dict[str, float] = {}
        for ccy, quoted in (usd_base_rates or {}).items():
            if quoted in (None, 0):
                continue
            try:
                out[str(ccy).upper()] = 1.0 / float(quoted)
            except Exception:
                continue
        out.setdefault("USD", 1.0)
        return out

    # ---------- Public API ----------

    def get_rates_ccy_to_usd(self) -> Dict[str, float]:
        raw = self._load_cache_raw()
        if raw is None:
            try:
                raw = self._fetch_raw()
                self._save_cache_raw(raw)
            except Exception:
                # crude hard-coded fallbacks if HTTP fails
                raw = {
                    "USD": 1.0,
                    "JPY": 150.0,
                    "HKD": 7.8,
                    "CNY": 7.2,
                }
        return self._normalize_ccy_to_usd(raw)
