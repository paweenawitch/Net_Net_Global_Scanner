## infrastructure/sources/yahoo_price_client.py

from __future__ import annotations
from typing import Dict, List, Optional, Tuple
import os, random, time
import pandas as pd
import yfinance as yf

from application.ports import PriceClient


PRICE_RPS = float(os.environ.get("YF_PRICE_RPS", "0.8"))
SLEEP_BASE = 1.0 / max(0.1, PRICE_RPS)


def _sleep_between_batches():
    time.sleep(SLEEP_BASE * (1.0 + random.uniform(-0.15, 0.15)))


class YahooPriceClient(PriceClient):
    def latest_closes(self, y_symbols: List[str], batch_size: int) -> Dict[str, Tuple[Optional[float], Optional[str]]]:
        out: Dict[str, Tuple[Optional[float], Optional[str]]] = {s: (None, None) for s in y_symbols}
        def last_close(d: pd.DataFrame):
            if d is None or d.empty or "Close" not in d.columns:
                return (None, None)
            d = d.sort_index()
            today = pd.Timestamp(pd.Timestamp.today().normalize())
            idx = d.index[-1]
            if isinstance(idx, pd.Timestamp) and idx.normalize() >= today and len(d) >= 2:
                row = d.iloc[-2]
                return (float(row["Close"]), d.index[-2].date().isoformat())
            row = d.iloc[-1]
            date = row.name.date().isoformat() if isinstance(row.name, pd.Timestamp) else None
            return (float(row["Close"]), date)

        # batching
        def chunks(seq, n):
            for i in range(0, len(seq), n):
                yield seq[i:i+n]

        for chunk in chunks(list(y_symbols), batch_size):
            df = None
            for attempt in range(4):
                try:
                    df = yf.download(
                        chunk, period="7d", interval="1d",
                        auto_adjust=False, group_by="ticker",
                        progress=False, threads=True,
                    )
                    break
                except Exception:
                    if attempt == 3:
                        df = None
                        break
                    time.sleep(min(6.0, (0.8 * (2 ** attempt)) * (1.0 + random.uniform(-0.2, 0.2))))
            if isinstance(df, pd.DataFrame) and isinstance(df.columns, pd.MultiIndex):
                for s in chunk:
                    try:
                        out[s] = last_close(df[s])
                    except Exception:
                        pass
            elif isinstance(df, pd.DataFrame) and len(chunk) == 1:
                out[chunk[0]] = last_close(df)
            _sleep_between_batches()
        return out
