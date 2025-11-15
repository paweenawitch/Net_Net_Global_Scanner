# tools/ncav_cache.py
from __future__ import annotations
from pathlib import Path
from dataclasses import dataclass, asdict
from typing import Optional, List, Tuple, Callable, Dict
from datetime import datetime, timezone, timedelta
from concurrent.futures import ThreadPoolExecutor, TimeoutError
import json, hashlib, os, time, random, re

import pandas as pd
import yfinance as yf

# ---------- Paths ----------
ROOT = Path(__file__).resolve().parents[1]
CACHE = ROOT / "cache" / "ncav"
CACHE.mkdir(parents=True, exist_ok=True)

# ---------- Rate limit & retry (Yahoo throttles) ----------
class RateLimiter:
    def __init__(self, rps: float = 2.0):
        self.dt = 1.0 / max(0.1, rps)
        self.next_t = 0.0
    def wait(self):
        now = time.monotonic()
        if now < self.next_t:
            time.sleep(self.next_t - now)
        self.next_t = time.monotonic() + self.dt + random.uniform(0, self.dt * 0.15)

YF_RPS = float(os.environ.get("YF_RPS", "2.0"))
_RL = RateLimiter(YF_RPS)

def _retry(fn: Callable[[], any], attempts=4, base=1.2):
    last = None
    for i in range(attempts):
        try:
            _RL.wait()
            return fn()
        except Exception as e:
            last = e
            s = str(e).lower()
            if not any(t in s for t in ["401","403","429","500","502","503","504","timed out"]):
                break
            time.sleep(min(12.0, base * (2 ** i) * (1 + random.uniform(-0.2, 0.2))))
    raise last if last else RuntimeError("yfinance error")

# ---------- Mapping (house → Yahoo) ----------
def to_yahoo(sym: str) -> str:
    s = (sym or "").strip().upper()
    if not s: return s
    if s.endswith(".US"): return s[:-3]               # AAPL.US -> AAPL
    if s.endswith(".JP"): return s[:-3] + ".T"        # 7203.JP -> 7203.T
    if s.endswith(".HK"): return s                    # 0005.HK -> 0005.HK
    if s.endswith(".UK"): return s[:-3] + ".L"        # PSH.UK -> PSH.L
    if s.endswith(".PL"): return s[:-3] + ".WA"       # KGH.PL -> KGH.WA
    if s.endswith(".FR"): return s[:-3] + ".PA"       # AI.FR  -> AI.PA
    if s.endswith(".TH"): return s[:-3] + ".BK"       # PTTEP.TH -> PTTEP.BK
    return s

# ---------- Coercers ----------
def _f(x) -> Optional[float]:
    """Coerce to float; treat any NaN-like as None."""
    try:
        if x is None:
            return None
        try:
            if pd.isna(x):
                return None
        except Exception:
            pass
        if isinstance(x, (int, float)):
            return None if pd.isna(x) else float(x)
        s = str(x).strip()
        if not s or s.lower() == "nan":
            return None
        v = float(s)
        return None if pd.isna(v) else v
    except Exception:
        return None

def _norm_date(x) -> Optional[str]:
    try:
        return pd.to_datetime(x).date().isoformat()
    except Exception:
        return None

# ---------- Row picker (robust) ----------
def _pick(df: pd.DataFrame, names: List[str]) -> Optional[pd.Series]:
    if df is None or df.empty: return None
    def norm(s: str) -> str:
        s = s.strip().lower()
        s = re.sub(r"[\s\-_]+", "", s)
        s = re.sub(r"[^\w]", "", s)
        return s
    idxmap = {norm(str(i)): i for i in df.index}
    syn = {
        "totalcurrentassets": ["totalcurrentassets","currentassets","currentassetstotal","totalcurrentasset"],
        "totalliabilities": ["totalliabilities","totalliab","liabilitiestotal","totalliabilitiesnetminorityinterest"],
        "totalcurrentliabilities": ["totalcurrentliabilities","currentliabilities","currentliabilitiestotal"],
        "totalnoncurrentliabilities": ["totalnoncurrentliabilities","noncurrentliabilities","noncurrentliabilitiestotal"],
        "totalassets": ["totalassets"],
        "noncurrentassets": ["noncurrentassets","totalnoncurrentassets","non-currentassets","noncurrentassetstotal"],
        "workingcapital": ["workingcapital"],
    }
    expanded = []
    for n in names:
        n0 = norm(n); expanded.append(n0)
        for key, arr in syn.items():
            if n0 == key or n0 in arr: expanded += arr
    # exact
    for n0 in expanded:
        if n0 in idxmap: return df.loc[idxmap[n0]]
    # contains
    for n0 in expanded:
        for k, orig in idxmap.items():
            if n0 in k: return df.loc[orig]
    return None

# ---------- Per-column extraction with derivations ----------
def _values_for_column(df: pd.DataFrame, col) -> Dict[str, Optional[float]]:
    ca_s  = _pick(df, ["Total Current Assets","Current Assets"])
    ta_s  = _pick(df, ["Total Assets"])
    nca_s = _pick(df, ["Non Current Assets","Total Non Current Assets","Non-Current Assets","Noncurrent Assets"])
    tl_s  = _pick(df, ["Total Liab","Total Liabilities","Total Liabilities Net Minority Interest","Liabilities Total"])
    cl_s  = _pick(df, ["Total Current Liabilities","Current Liabilities"])
    ncl_s = _pick(df, ["Total Non-Current Liabilities","Non Current Liabilities","Non-Current Liabilities"])
    wc_s  = _pick(df, ["Working Capital"])

    ta  = _f(ta_s.get(col))  if ta_s  is not None else None
    nca = _f(nca_s.get(col)) if nca_s is not None else None
    tl  = _f(tl_s.get(col))  if tl_s  is not None else None
    cl  = _f(cl_s.get(col))  if cl_s  is not None else None
    ncl = _f(ncl_s.get(col)) if ncl_s is not None else None
    wc  = _f(wc_s.get(col))  if wc_s  is not None else None

    ca  = _f(ca_s.get(col))  if ca_s  is not None else None
    if cl is None and tl is not None and ncl is not None:
        cl = tl - ncl
    if ca is None and wc is not None and cl is not None:
        ca = wc + cl
    if ca is None and ta is not None and nca is not None:
        ca = ta - nca
    if tl is None and cl is not None and ncl is not None:
        tl = cl + ncl

    return {"assets_current": ca, "liab_total": tl}

# ---------- Build unified candidate list (annual + quarterly), newest → oldest ----------
def _collect_candidates(bs_a: pd.DataFrame, bs_q: pd.DataFrame) -> List[dict]:
    cands: List[dict] = []
    for source, df in (("annual", bs_a), ("quarterly", bs_q)):
        if df is None or df.empty:
            continue
        for col in df.columns:
            di = _norm_date(col)
            try:
                dt = pd.to_datetime(di)
            except Exception:
                dt = None
            vals = _values_for_column(df, col)  # NaN-safe via _f
            cands.append({
                "date_iso": di,
                "dt": dt,
                "vals": vals,
                "source": source,
            })
    # newest → oldest across both; if tie on date, prefer quarterly
    def _key(c):
        dt = c["dt"] if c["dt"] is not None else pd.Timestamp.min
        prio = 1 if c["source"] == "quarterly" else 0
        return (dt, prio)
    cands.sort(key=_key, reverse=True)
    return cands

# ---------- Select newest viable within 2y that yields calculable ncav_ps ----------
def _select_latest_viable_ncavps(
    bs_a: pd.DataFrame,
    bs_q: pd.DataFrame,
    shares_out: Optional[float],
    max_age_days: int = 730
) -> Tuple[Optional[str], dict, Optional[str]]:
    """
    Walk reverse-chronologically across quarterly/half-year/annual columns.
    Pick the first (newest) within `max_age_days` where:
      - CA and TL both resolve (NaN treated as missing),
      - shares_out > 0 so NCAVps is calculable.
    """
    if shares_out is None or shares_out <= 0:
        return None, {"assets_current": None, "liab_total": None}, None

    cands = _collect_candidates(bs_a, bs_q)
    if not cands:
        return None, {"assets_current": None, "liab_total": None}, None

    cutoff = datetime.now(timezone.utc).date() - timedelta(days=max_age_days)

    for c in cands:
        dt = c["dt"]
        if dt is None or dt.date() < cutoff:
            continue
        ca = c["vals"].get("assets_current")
        tl = c["vals"].get("liab_total")
        if ca is None or tl is None:
            continue
        # NCAVps calculable?
        try:
            ncav = float(ca) - float(tl)
            _ = ncav / float(shares_out)
        except Exception:
            continue
        return c["date_iso"], c["vals"], c["source"]

    return None, {"assets_current": None, "liab_total": None}, None

def _financial_currency(t: yf.Ticker) -> str:
    try:
        info = _retry(lambda: (t.info or {}))
        return str(info.get("financialCurrency") or "USD").upper()
    except Exception:
        return "USD"

# ---------- Cache record ----------
@dataclass
class NcavRecord:
    ticker: str
    y_symbol: str
    statement_date: Optional[str]          # selected date (within 2y) or None
    currency: str                          # FS currency
    assets_current: Optional[float]
    liab_total: Optional[float]
    ncav: Optional[float]
    shares_out: Optional[float]
    ncav_ps: Optional[float]
    source: str
    cached_at: str
    statement_sig: str
    data_age_days: Optional[int] = None    # staleness
    fs_source: Optional[str] = None        # "annual" or "quarterly"
    fs_selected_col: Optional[str] = None  # column date used
    note: Optional[str] = None             # reason when selection fails

    @staticmethod
    def from_yahoo(house_ticker: str) -> "NcavRecord":
        y = to_yahoo(house_ticker)
        t = yf.Ticker(y)

        # 1) shares_out first (needed to validate NCAVps calculability)
        shares_out: Optional[float] = None
        try:
            info = _retry(lambda: (t.info or {}))
            so = info.get("sharesOutstanding")
            if so is not None and float(so) > 0:
                shares_out = float(so)
        except Exception:
            pass
        if shares_out is None:
            try:
                series = _retry(lambda: t.get_shares_full())
                if series is not None and len(series) > 0:
                    shares_out = float(pd.Series(series).sort_index().iloc[-1])
            except Exception:
                pass
        if shares_out is not None and shares_out <= 0:
            shares_out = None

        # 2) fetch FS frames
        try: bs_a = _retry(lambda: t.balance_sheet)
        except Exception: bs_a = pd.DataFrame()
        try: bs_q = _retry(lambda: t.quarterly_balance_sheet)
        except Exception: bs_q = pd.DataFrame()

        # 3) select newest viable column within 2y that yields calculable NCAVps
        sel_date, comp, src = _select_latest_viable_ncavps(bs_a, bs_q, shares_out, max_age_days=730)

        cur = _financial_currency(t)
        ca = comp.get("assets_current")
        tl = comp.get("liab_total")
        ncav = (ca - tl) if (ca is not None and tl is not None) else None

        ncav_ps: Optional[float] = None
        if (ncav is not None) and (shares_out is not None) and (shares_out > 0):
            try:
                ncav_ps = float(ncav) / float(shares_out)
            except Exception:
                ncav_ps = None

        data_age_days = None
        note = None
        if sel_date:
            try:
                data_age_days = (datetime.now(timezone.utc).date() - pd.to_datetime(sel_date).date()).days
            except Exception:
                pass
        else:
            if shares_out is None:
                note = "no shares_out"
            else:
                note = "no viable FS column (CA & TL present) within 2y"

        sig = hashlib.sha256(f"{sel_date}|{cur}|{ca}|{tl}|{shares_out}".encode()).hexdigest()[:16]

        return NcavRecord(
            ticker=house_ticker, y_symbol=y, statement_date=sel_date, currency=cur,
            assets_current=ca, liab_total=tl, ncav=ncav,
            shares_out=shares_out, ncav_ps=ncav_ps,
            source="yahoo", cached_at=datetime.now(timezone.utc).isoformat(timespec="seconds"),
            statement_sig=sig, data_age_days=data_age_days,
            fs_source=src, fs_selected_col=sel_date, note=note
        )

# ---------- JSON I/O ----------
def _cache_path(h: str) -> Path:
    return CACHE / f"{h.replace('/', '_')}.json"

def load_cached(h: str) -> Optional[NcavRecord]:
    p = _cache_path(h)
    if not p.exists(): return None
    try:
        return NcavRecord(**json.loads(p.read_text(encoding="utf-8")))
    except Exception:
        return None

def _nan_to_none(obj):
    if isinstance(obj, dict):  return {k:_nan_to_none(v) for k,v in obj.items()}
    if isinstance(obj, list):  return [_nan_to_none(v) for v in obj]
    try:
        if pd.isna(obj): return None
    except Exception: pass
    return obj

def save_cache(rec: NcavRecord) -> None:
    payload = _nan_to_none(asdict(rec))
    _cache_path(rec.ticker).write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

# ---------- Public API (Windows-safe timeout) ----------
def build_or_update(house_ticker: str, fetch_timeout: int = 15) -> NcavRecord:
    prev = load_cached(house_ticker)
    def _do(): return NcavRecord.from_yahoo(house_ticker)
    try:
        with ThreadPoolExecutor(max_workers=1) as ex:
            cur = ex.submit(_do).result(timeout=fetch_timeout)
    except TimeoutError:
        if prev:
            prev.cached_at = datetime.now(timezone.utc).isoformat(timespec="seconds"); save_cache(prev); return prev
        cur = NcavRecord(house_ticker, to_yahoo(house_ticker), None, "", None, None, None, None, None,
                         "yahoo", datetime.now(timezone.utc).isoformat(timespec="seconds"), "", None, None, None, "timeout")
    except Exception:
        if prev:
            prev.cached_at = datetime.now(timezone.utc).isoformat(timespec="seconds"); save_cache(prev); return prev
        cur = NcavRecord(house_ticker, to_yahoo(house_ticker), None, "", None, None, None, None, None,
                         "yahoo", datetime.now(timezoneutc).isoformat(timespec="seconds"), "", None, None, None, "error")
    if prev and prev.statement_sig == cur.statement_sig:
        prev.cached_at = datetime.now(timezone.utc).isoformat(timespec="seconds"); save_cache(prev); return prev
    save_cache(cur); return cur

# ---------- CLI ----------
if __name__ == "__main__":
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument("--one", type=str)
    args = ap.parse_args()
    if args.one:
        r = build_or_update(args.one)
        print(json.dumps(asdict(r), indent=2))
