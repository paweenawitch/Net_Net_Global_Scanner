## infrastructure/repositories/local_shortlist_repository.py

from __future__ import annotations
from pathlib import Path
import json
from typing import Any

class LocalShortlistRepository:
    def __init__(self, root: Path) -> None:
        self.root = Path(root)
        self.data_dir = self.root / "data" / "tickers"
        self.data_dir.mkdir(parents=True, exist_ok=True)
        self.out_all = self.data_dir / "ncav_all.csv"
        self.out_short = self.data_dir / "ncav_shortlist.csv"
        self.out_meta = self.data_dir / "ncav_shortlist.meta.json"

    def save_all(self, df):
        df.to_csv(self.out_all, index=False)
        return self.out_all

    def save_shortlist(self, df):
        df.to_csv(self.out_short, index=False)
        return self.out_short

    def save_meta(self, payload: Any):
        self.out_meta.write_text(json.dumps(payload, indent=2), encoding="utf-8")
        return self.out_meta
