# infrastructure/repositories/csv_shortlist_reader_repository.py

from __future__ import annotations
from pathlib import Path
from typing import List
import pandas as pd

from application.screening_service import ShortlistRepository, ShortlistItem


class CsvShortlistReaderRepository(ShortlistRepository):
    """
    Read the NCAV shortlist produced by the previous stage.

    Expected columns:
        - ticker
        - price  (last market price)
    """

    def __init__(self, price_column: str = "price") -> None:
        self._price_column = price_column

    def load_shortlist(self, path: Path) -> List[ShortlistItem]:
        df = pd.read_csv(path)

        if "ticker" not in df.columns:
            raise ValueError("shortlist CSV is missing required column 'ticker'")
        if self._price_column not in df.columns:
            raise ValueError(f"shortlist CSV is missing required column '{self._price_column}'")

        df[self._price_column] = pd.to_numeric(df[self._price_column], errors="coerce")

        items: List[ShortlistItem] = []
        for _, row in df.iterrows():
            ticker = str(row["ticker"]).upper().strip()
            price = row[self._price_column]
            if pd.isna(price):
                continue
            items.append(ShortlistItem(ticker=ticker, last_price=float(price)))
        return items
