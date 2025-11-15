#infrastructure/repositories/csv_universe_writer_repository.py

from __future__ import annotations
from typing import Dict, Any
from pathlib import Path
import json
import pandas as pd
from application.ports import UniverseRepository

class CsvUniverseWriterRepository(UniverseRepository):
    """
    Writes per-market universe files and global full list with metadata.
    Location: data/tickers/{us|jp|hk}_full.csv + meta JSONs.
    """

    def __init__(self, project_root: Path) -> None:
        self.root = Path(project_root)
        self.tickers_dir = self.root / "data" / "tickers"
        self.tickers_dir.mkdir(parents=True, exist_ok=True)

    def _write_pair(self, csv_path: Path, meta_path: Path, df: pd.DataFrame, meta: Dict[str, Any]) -> None:
        df.to_csv(csv_path, index=False)
        meta_out = dict(meta)
        meta_out["path"] = str(csv_path)
        meta_path.write_text(json.dumps(meta_out, indent=2), encoding="utf-8")

    def write_market(self, market: str, df: pd.DataFrame, meta: Dict[str, Any]) -> None:
        """Save universe for one market."""
        market = market.upper()
        csv = self.tickers_dir / f"{market.lower()}_full.csv"
        meta_p = self.tickers_dir / f"{market.lower()}_full.meta.json"
        self._write_pair(csv, meta_p, df, meta)

    def write_global(self, df: pd.DataFrame, meta: Dict[str, Any]) -> None:
        """Save combined global universe."""
        csv = self.tickers_dir / "global_full.csv"
        meta_p = self.tickers_dir / "global_full.meta.json"
        self._write_pair(csv, meta_p, df, meta)
