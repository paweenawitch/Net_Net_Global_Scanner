#infrastructure/repositories/csv_universe_loader_repository.py
from __future__ import annotations
from pathlib import Path
from typing import List, Dict
import pandas as pd

from application.ports import ShortlistUniverseRepository


class CsvUniverseLoaderRepository(ShortlistUniverseRepository):
    """
    Reads an existing universe CSV (e.g. data/tickers/global_full.csv)
    and exposes it as a list of dict rows.
    """

    def __init__(self, csv_path: Path) -> None:
        self.csv_path = Path(csv_path)
        if not self.csv_path.exists():
            raise FileNotFoundError(f"Universe CSV not found: {self.csv_path}")

    def load_tickers(self) -> List[Dict[str, str]]:
        df = pd.read_csv(self.csv_path)
        if "ticker" not in df.columns:
            raise ValueError(f"{self.csv_path} must have a 'ticker' column")

        keep = [c for c in ["ticker", "name", "country", "mic"] if c in df.columns]
        return df[keep].to_dict(orient="records")
