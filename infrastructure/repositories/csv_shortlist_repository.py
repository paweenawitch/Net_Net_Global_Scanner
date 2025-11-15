#infrastructure/repositories/csv_shortlist_repository.py
from pathlib import Path
import pandas as pd

class CsvShortlistRepo:
    """Tiny helper if you need to inspect shortlist composition in other commands."""
    def __init__(self, csv_path: Path):
        self.csv_path = csv_path

    def rows(self) -> list[dict]:
        df = pd.read_csv(self.csv_path)
        # allow flexible column names for yahoo symbol
        ycols = ["y_symbol","y_ticker","yahoo","yf_symbol","YahooSymbol","Y_symbol"]
        ycol = next((c for c in ycols if c in df.columns), None)
        out = []
        for _, r in df.iterrows():
            t = str(r.get("ticker") or r.get("symbol") or "").upper().strip()
            if not t: continue
            out.append({"ticker": t, "y_symbol": (str(r.get(ycol)).strip() if ycol and r.get(ycol) else "")})
        return out
