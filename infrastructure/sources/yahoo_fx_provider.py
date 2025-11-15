## infrastructure/sources/yahoo_fx_provider.py

from __future__ import annotations
from typing import Dict, List, Optional
import pandas as pd
import yfinance as yf

from application.ports import FxProvider


class YahooFxProvider(FxProvider):
    FX_BASE = "USD"

    @staticmethod
    def _pairs(codes: List[str]) -> List[str]:
        out = []
        for c in sorted(set(codes + [YahooFxProvider.FX_BASE])):
            c = (c or "USD").upper()
            out.append("USDUSD=X" if c == "USD" else f"{c}{YahooFxProvider.FX_BASE}=X")
        return out

    def usd_per_ccy(self, currencies: List[str]) -> Dict[str, float]:
        pairs = self._pairs(currencies)
        out: Dict[str, float] = {"USD": 1.0}
        try:
            df = yf.download(pairs, period="7d", interval="1d", progress=False, group_by="ticker", threads=True)
        except Exception:
            df = None

        def last_close(d: pd.DataFrame) -> Optional[float]:
            if d is None or d.empty or "Close" not in d.columns:
                return None
            d = d.sort_index()
            return float(d.iloc[-1]["Close"])

        if isinstance(df, pd.DataFrame) and isinstance(df.columns, pd.MultiIndex):
            for p in pairs:
                ccy = "USD" if p == "USDUSD=X" else p.replace("=X","" ).replace(self.FX_BASE, "")
                try:
                    v = last_close(df[p])
                    if v is not None:
                        out[ccy] = v
                except Exception:
                    pass
        elif isinstance(df, pd.DataFrame) and len(pairs) == 1:
            p = pairs[0]
            ccy = "USD" if p == "USDUSD=X" else p.replace("=X","" ).replace(self.FX_BASE, "")
            v = last_close(df)
            if v is not None:
                out[ccy] = v
        out.setdefault("USD", 1.0)
        return out
