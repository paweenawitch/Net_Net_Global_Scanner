from __future__ import annotations
from dataclasses import dataclass
from typing import List, Dict, Any
from datetime import datetime, timezone
import pandas as pd

from application.ports import TickerSource, UniverseRepository, UniverseBuilder

def _normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    want = ["ticker_base","ticker","name","country","mic"]
    if df is None or df.empty:
        return pd.DataFrame(columns=want)

    out = df.copy()
    # lightweight coercions
    if "ticker_base" in out.columns:
        out["ticker_base"] = (
            out["ticker_base"].astype(str).str.extract(r"([A-Za-z0-9]{1,10})")[0]
        )
    # Ensure presence
    for c in want:
        if c not in out.columns:
            out[c] = None
    out = out[want].dropna(subset=["ticker_base","ticker"]).drop_duplicates(subset=["ticker"]).reset_index(drop=True)
    return out

def _dedupe_global(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return df
    # Same policy as your current builder: drop dup by (country, ticker_base) then (country, name)
    df = df.drop_duplicates(subset=["country","ticker_base"], keep="first")
    df = df.drop_duplicates(subset=["country","name"], keep="first")
    return df.reset_index(drop=True)

@dataclass
class BuildUniverseService(UniverseBuilder):
    sources: List[TickerSource]
    repo: UniverseRepository

    def run(self) -> Dict[str, Any]:
        prov: Dict[str, Any] = {}
        frames: List[pd.DataFrame] = []

        for src in self.sources:
            df = _normalize_columns(src.fetch())
            meta = {
                "source": getattr(src, "source_label", src.__class__.__name__),
                "generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
                "rows": int(len(df)),
            }
            self.repo.write_market(src.market_code, df, meta)
            prov[src.market_code] = meta
            if not df.empty:
                frames.append(df)

        all_df = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame(columns=["ticker_base","ticker","name","country","mic"])
        all_df = _dedupe_global(all_df)

        global_meta = {
            "generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
            "markets": prov,
            "total_rows": int(len(all_df)),
        }
        self.repo.write_global(all_df, global_meta)
        return {"meta": global_meta, "rows": int(len(all_df))}
