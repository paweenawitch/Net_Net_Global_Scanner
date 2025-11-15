#tools/non_us_fetch_companyfact.py
from __future__ import annotations
"""
Non-US Companyfacts Fetcher (Step 2 – Yahoo path)

- Reads data/ncav_shortlist.csv; uses 'y_symbol' for Yahoo/yfinance (e.g., 1420.JP -> 1420.T)
- Skips .US tickers entirely
- Token-bucket pacing across Yahoo JSON/HTML and yfinance properties
- Insider % fallback: query2 (with referer/region/lang) -> yfinance.major_holders DF -> holders HTML bootstrap
- Shares outstanding:
    * Pull **full time series** via Yahoo fundamentals-timeseries (query2)
    * Fallback to yfinance.get_shares_full()/get_shares()
    * Map shares to **EVERY** period (nearest match with smart window) to detect dilution
    * Fallback to .info.sharesOutstanding only if no series points exist
- Outputs:
    cache/sec_core/{TICKER}_core.json
    cache/sec_insider/{TICKER}.json
"""

import argparse
import json
import logging
import os
import random
import re
import threading
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
import requests

try:
    import yfinance as yf  # type: ignore
except Exception:  # pragma: no cover
    yf = None

# -----------------------------------------------------------------------------
# Logging / paths
# -----------------------------------------------------------------------------
LOGGER = logging.getLogger("nonus_fetch")

ROOT = Path(__file__).resolve().parents[1]
CORE_DIR = ROOT / "cache" / "sec_core"
INS_DIR  = ROOT / "cache" / "sec_insider"
CORE_DIR.mkdir(parents=True, exist_ok=True)
INS_DIR.mkdir(parents=True, exist_ok=True)

FRESH_ANNUAL_DAYS    = 450    # lenient for semiannual markets
FRESH_QUARTERLY_DAYS = 210

SHORTLIST_DEFAULT = "data/tickers/ncav_shortlist.csv"

# -----------------------------------------------------------------------------
# Token-bucket pacing (centralized throttling)
# -----------------------------------------------------------------------------
# Tune via env if needed. Start slow for reliability.
YF_RPS_JSON = float(os.environ.get("YF_RPS_JSON", "0.4"))   # quoteSummary + holders HTML + fundamentals-timeseries
YF_RPS_INFO = float(os.environ.get("YF_RPS_INFO", "0.4"))   # yfinance properties (.info/.financials/.get_shares*)
_JSON_BUCKET = {"t": 0.0, "lock": threading.Lock()}
_INFO_BUCKET = {"t": 0.0, "lock": threading.Lock()}

def _pace(bucket: dict, rps: float):
    """Ensure ≥ 1/rps seconds since last use of this bucket (with jitter)."""
    dt_min = 1.0 / max(0.1, rps)
    with bucket["lock"]:
        now = time.perf_counter()
        wait = bucket["t"] + dt_min - now
        if wait > 0:
            time.sleep(wait * (1.0 + random.uniform(-0.1, 0.1)))
        bucket["t"] = time.perf_counter()

# Shared Yahoo session
_YF_SESS = requests.Session()
_YF_SESS.headers.update({"User-Agent": "Mozilla/5.0", "Accept": "application/json"})
YF_HEADERS = {"User-Agent": "Mozilla/5.0"}

# -----------------------------------------------------------------------------
# Small utils
# -----------------------------------------------------------------------------

def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")

def is_us_ticker(t: str) -> bool:
    return t.upper().endswith(".US")

def to_iso_date(x) -> Optional[str]:
    try:
        return pd.to_datetime(x).date().isoformat()
    except Exception:
        return None

def pick(df: pd.DataFrame, row_names: List[str], col) -> Optional[float]:
    if df is None or df.empty:
        return None
    for rn in row_names:
        try:
            v = df.loc[rn, col]
            if pd.isna(v):
                continue
            return float(v)
        except Exception:
            continue
    return None

# -----------------------------------------------------------------------------
# Load shortlist with y_symbol
# -----------------------------------------------------------------------------

def _resolve_columns(df: pd.DataFrame):
    # ticker column
    ticker_col = None
    for cand in ["ticker", "Ticker", "symbol", "Symbol"]:
        if cand in df.columns:
            ticker_col = cand
            break
    if not ticker_col:
        raise SystemExit("shortlist must contain a 'ticker' (or 'symbol') column")

    # y_symbol column
    y_col = None
    for cand in ["y_symbol", "y_ticker", "yahoo", "yfin", "yf_symbol", "Y_symbol", "YahooSymbol"]:
        if cand in df.columns:
            y_col = cand
            break
    return ticker_col, y_col

def load_shortlist_rows(path: Path) -> List[Dict[str, str]]:
    df = pd.read_csv(path)
    ticker_col, y_col = _resolve_columns(df)
    rows: List[Dict[str, str]] = []
    for _, r in df.iterrows():
        t = str(r.get(ticker_col, "")).strip().upper()
        if not t:
            continue
        rows.append({
            "ticker": t,
            "y_symbol": str(r.get(y_col, "")).strip() if y_col else ""
        })
    return rows

def resolve_y_symbol(t: str, rows: List[Dict[str, str]]) -> Optional[str]:
    t_up = t.strip().upper()
    for r in rows:
        if r["ticker"].upper() == t_up:
            ys = (r.get("y_symbol") or "").strip()
            return ys or None
    return None

# -----------------------------------------------------------------------------
# Mapping helpers
# -----------------------------------------------------------------------------
BAL_MAP = {
    "assets_current": ["Current Assets", "Total Current Assets"],
    "liab_total": ["Total Liab", "Total Liabilities Net Minority Interest", "Total Liabilities"],
    "liab_current": ["Total Current Liabilities", "Current Liabilities"],
    "liab_noncurrent": ["Total Non Current Liabilities Net Minority Interest", "Non Current Liabilities", "Long Term Liab"],
    "equity": ["Total Stockholder Equity", "Total Equity Gross Minority Interest", "Stockholders Equity"],
    # adjusted NCAV extras
    "cash": ["Cash And Cash Equivalents", "Cash"],
    "short_invest": ["Other Short Term Investments", "Short Term Investments", "Marketable Securities"],
    "receivables": ["Net Receivables", "Accounts Receivable", "Receivables"],
    "inventory": ["Inventory", "Inventories"],
}
INC_MAP = {
    "revenue": ["Total Revenue", "Revenue"],
    "gross_profit": ["Gross Profit"],
    "oper_income": ["Operating Income", "Operating Income or Loss"],
    "net_income": ["Net Income", "Net Income Common Stockholders"],
}
CF_MAP = {
    "cfo": ["Total Cash From Operating Activities", "Net Cash Provided by Operating Activities"],
    "capex": ["Capital Expenditures"],
    "dividends_paid": ["Dividends Paid"],
}

# -----------------------------------------------------------------------------
# Core building from yfinance
# -----------------------------------------------------------------------------

def frame_to_periods(df_bal: pd.DataFrame,
                     df_inc: pd.DataFrame,
                     df_cf: pd.DataFrame,
                     ccy: str,
                     limit: int) -> List[Dict[str, Any]]:
    periods: List[Dict[str, Any]] = []
    cols: List[Any] = []
    if df_bal is not None and not df_bal.empty:
        cols = list(df_bal.columns)
    elif df_inc is not None and not df_inc.empty:
        cols = list(df_inc.columns)
    elif df_cf is not None and not df_cf.empty:
        cols = list(df_cf.columns)
    cols = cols[:limit]

    for col in cols:
        dt = to_iso_date(col)
        balance: Dict[str, Any] = {}
        for key, candidates in BAL_MAP.items():
            val = pick(df_bal, candidates, col)
            if val is not None:
                balance[key] = {"val": float(val), "unit": ccy}

        income: Dict[str, Any] = {}
        for key, candidates in INC_MAP.items():
            val = pick(df_inc, candidates, col)
            if val is not None:
                income[key] = {"val": float(val), "unit": ccy}

        cashflow: Dict[str, Any] = {}
        for key, candidates in CF_MAP.items():
            val = pick(df_cf, candidates, col)
            if val is not None:
                cashflow[key] = {"val": float(val), "unit": ccy}

        periods.append({
            "date": dt,
            "currency": ccy,
            "balance": balance,
            "income": income,
            "cashflow": cashflow,
        })

    periods.sort(key=lambda p: (p.get("date") or ""), reverse=True)
    return periods

# -----------------------------------------------------------------------------
# Shares time-series (Yahoo fundamentals-timeseries + fallbacks)
# -----------------------------------------------------------------------------

def _yahoo_timeseries_shares(y_symbol: str) -> Optional[pd.Series]:
    """Pulls a rich shares outstanding time series from Yahoo query2 fundamentals-timeseries."""
    _pace(_JSON_BUCKET, YF_RPS_JSON)
    url = f"https://query2.finance.yahoo.com/ws/fundamentals-timeseries/v1/finance/timeseries/{y_symbol}"
    now = int(time.time())
    params = {
        "type": ",".join([
            "trailingSharesOutstanding",
            "sharesOutstanding",
            "impliedSharesOutstanding",
            "annualBasicAverageShares",
            "annualDilutedAverageShares",
            "quarterlyBasicAverageShares",
            "quarterlyDilutedAverageShares",
        ]),
        "period1": "0",
        "period2": str(now),
        "lang": "en-US",
        "region": "US",
        "corsDomain": "finance.yahoo.com",
        "padTimeSeries": "true",
    }
    headers = {
        **_YF_SESS.headers,
        "Referer": f"https://finance.yahoo.com/quote/{y_symbol}/holders",
        "Accept": "application/json, text/plain, */*",
    }
    for attempt in range(4):
        try:
            resp = _YF_SESS.get(url, params=params, headers=headers, timeout=25)
            if resp.status_code == 200:
                js = resp.json()
                res = (js.get("timeseries") or {}).get("result")
                if res is None:
                    res = js.get("result")
                if not res:
                    return None
                bucket = res[0] if isinstance(res, list) else res
                pts: List[Tuple[pd.Timestamp, float]] = []
                for key, arr in (bucket or {}).items():
                    if not isinstance(arr, list):
                        continue
                    if "share" not in str(key).lower():
                        continue
                    for it in arr:
                        if not isinstance(it, dict):
                            continue
                        dt = None
                        raw = None
                        if it.get("asOfDate"):
                            try:
                                dt = pd.to_datetime(it["asOfDate"])  # YYYY-MM-DD
                            except Exception:
                                dt = None
                        if dt is None and it.get("timestamp") is not None:
                            try:
                                dt = pd.to_datetime(int(it["timestamp"]), unit="s")
                            except Exception:
                                dt = None
                        rv = it.get("reportedValue") or it.get("value") or {}
                        if isinstance(rv, dict) and rv.get("raw") is not None:
                            raw = rv.get("raw")
                        elif it.get("raw") is not None:
                            raw = it.get("raw")
                        if dt is not None and isinstance(raw, (int, float)):
                            pts.append((pd.to_datetime(dt.date()), float(raw)))
                if not pts:
                    return None
                # Deduplicate by date; keep last per-day value
                pts.sort(key=lambda x: x[0])
                idx = pd.DatetimeIndex([d for d, _ in pts])
                vals = [v for _, v in pts]
                s = pd.Series(vals, index=idx)
                s = s[~s.index.duplicated(keep="last")]
                return s
            if resp.status_code in (401, 404):
                return None
            if resp.status_code in (429, 500, 502, 503, 504):
                back = min(8.0, 0.8 * (2 ** attempt))
                time.sleep(back); _pace(_JSON_BUCKET, YF_RPS_JSON); continue
            return None
        except Exception:
            back = min(8.0, 0.8 * (2 ** attempt))
            time.sleep(back); _pace(_JSON_BUCKET, YF_RPS_JSON)
    return None


def _get_shares_series(T) -> Optional[pd.Series]:
    """Best-effort: Yahoo fundamentals-timeseries -> yfinance.get_shares_full() -> yfinance.get_shares()."""
    # 1) Yahoo fundamentals-timeseries
    try:
        s = _yahoo_timeseries_shares(T.ticker if hasattr(T, "ticker") else "")
        if isinstance(s, pd.Series) and not s.empty:
            return s
    except Exception:
        pass
    # 2) yfinance get_shares_full
    try:
        _pace(_INFO_BUCKET, YF_RPS_INFO)
        if hasattr(T, "get_shares_full"):
            s = T.get_shares_full()
            if isinstance(s, pd.Series) and not s.empty:
                return s
    except Exception:
        pass
    # 3) yfinance get_shares
    try:
        _pace(_INFO_BUCKET, YF_RPS_INFO)
        if hasattr(T, "get_shares"):
            s = T.get_shares()
            if isinstance(s, pd.Series) and not s.empty:
                return s
    except Exception:
        pass
    return None


def _map_shares_to_periods(periods: List[Dict[str, Any]], shares_series: Optional[pd.Series], info_shares: Optional[float], window_days: int = 90) -> Tuple[List[Dict[str, Any]], Optional[float], Dict[str, Any]]:
    """
    Attach 'shares_out' into EVERY period.balance using:
      1) nearest share count within ±window_days
      2) else last value prior to date
      3) else fallback to info_shares (flag approx)
    Returns (periods_with_shares, chosen_latest_shares, meta_shares_info)
    """
    latest_shares = None
    shares_meta: Dict[str, Any] = {}

    # Normalize series
    s = None
    if shares_series is not None:
        try:
            s = shares_series.copy()
            if not isinstance(s.index, pd.DatetimeIndex):
                s.index = pd.to_datetime(s.index)
            s = s.sort_index().astype(float)
            shares_meta["series_source"] = "fundamentals_timeseries" if "series_source" not in shares_meta else shares_meta["series_source"]
            if not s.empty:
                latest_shares = float(s.iloc[-1])
        except Exception:
            s = None

    def pick_nearest(dt: pd.Timestamp) -> Optional[float]:
        if s is None or s.empty:
            return None
        # candidates around dt
        try:
            # exact or nearest by absolute difference
            # build nearest on both sides
            before = s.loc[:dt]
            after  = s.loc[dt:]
            cand = []
            if not before.empty:
                cand.append((abs((dt - before.index[-1]).days), before.index[-1]))
            if not after.empty:
                cand.append((abs((after.index[0] - dt).days), after.index[0]))
            if not cand:
                return None
            cand.sort(key=lambda x: x[0])
            days, idx = cand[0]
            if days <= window_days:
                return float(s.loc[idx])
            # if too far, prefer last prior point
            if not before.empty:
                return float(before.iloc[-1])
            return float(s.iloc[0])
        except Exception:
            return None

    for i, p in enumerate(periods):
        pdate = p.get("date")
        if not pdate:
            continue
        dt = pd.to_datetime(pdate)
        val = pick_nearest(dt)
        approx = False
        src = None
        if val is None and isinstance(info_shares, (int, float)) and info_shares > 0:
            val = float(info_shares)
            approx = True
            src = "yfinance.info.sharesOutstanding"
        elif val is not None:
            src = "shares.nearest"
        if val is not None:
            if "balance" not in p or p["balance"] is None:
                p["balance"] = {}
            p["balance"]["shares_out"] = {
                "val": val,
                "unit": "shares",
                "src": src or "unknown",
                **({"approx": True} if approx else {})
            }

    if latest_shares is None and isinstance(info_shares, (int, float)) and info_shares > 0:
        latest_shares = float(info_shares)

    if latest_shares is not None:
        shares_meta["latest_val"] = latest_shares
        shares_meta["latest_source"] = ("fundamentals_timeseries" if s is not None and not s.empty else "yfinance.info")

    return periods, latest_shares, shares_meta


def compute_derived_latest(periods: List[Dict[str, Any]], shares_override: Optional[float] = None) -> Dict[str, Any]:
    if not periods:
        return {}
    latest = periods[0]
    b = latest.get("balance") or {}

    def gv(k: str) -> Optional[float]:
        node = b.get(k) or {}
        return node.get("val") if isinstance(node, dict) else None

    CA = gv("assets_current")
    TL = gv("liab_total")
    LC = gv("liab_current")
    LNC = gv("liab_noncurrent")
    if TL is None and (LC is not None and LNC is not None):
        TL = LC + LNC

    cash = gv("cash") or 0.0
    sti  = gv("short_invest") or 0.0
    ar   = gv("receivables") or 0.0
    inv  = gv("inventory") or 0.0

    SH = gv("shares_out")
    if SH is None and shares_override is not None:
        SH = float(shares_override)

    ncav = (CA - TL) if (CA is not None and TL is not None) else None
    adj  = (cash + 0.75 * ar + 0.5 * inv - (TL or 0.0)) if TL is not None else None

    ncav_ps     = (ncav / SH) if (ncav is not None and SH not in (None, 0)) else None
    adj_ncav_ps = (adj  / SH) if (adj  is not None and SH not in (None, 0)) else None

    out = {
        "date": latest.get("date"),
        "ncav": ncav,
        "ncav_ps": ncav_ps,
        "adj_ncav": adj,
        "adj_ncav_ps": adj_ncav_ps,
        "cash_plus_sti": (cash + sti) if (cash is not None and sti is not None) else None,
        "shares_is_weighted_avg": False,
    }
    if SH is not None:
        out["shares_out"] = SH
    return out


def yf_build_core(y_symbol: str, original_ticker: str) -> Dict[str, Any]:
    if yf is None:
        raise RuntimeError("yfinance is not installed")

    LOGGER.info("[core] %s -> %s — fetching yfinance info & statements", original_ticker, y_symbol)
    T = yf.Ticker(y_symbol)

    # --- info (includes reporting currency and shares outstanding) ---
    _pace(_INFO_BUCKET, YF_RPS_INFO); info = T.info or {}
    ccy = (info.get("financialCurrency") or info.get("currency") or "USD").upper()

    # shares outstanding from info
    info_shares = None
    info_shares_key = None
    for k in ("sharesOutstanding", "impliedSharesOutstanding"):
        try:
            v = info.get(k)
            if isinstance(v, (int, float)) and v > 0:
                info_shares = float(v)
                info_shares_key = k
                break
        except Exception:
            pass

    # time series of shares (for dilution detection)
    shares_series = _get_shares_series(T)

    # --- statements (paced) ---
    _pace(_INFO_BUCKET, YF_RPS_INFO); q_bal = getattr(T, "quarterly_balance_sheet", None)
    _pace(_INFO_BUCKET, YF_RPS_INFO); q_inc = getattr(T, "quarterly_financials", None)
    _pace(_INFO_BUCKET, YF_RPS_INFO); q_cf  = getattr(T, "quarterly_cashflow", None)
    _pace(_INFO_BUCKET, YF_RPS_INFO); a_bal = getattr(T, "balance_sheet", None)
    _pace(_INFO_BUCKET, YF_RPS_INFO); a_inc = getattr(T, "financials", None)
    _pace(_INFO_BUCKET, YF_RPS_INFO); a_cf  = getattr(T, "cashflow", None)

    q_bal = q_bal if isinstance(q_bal, pd.DataFrame) else pd.DataFrame()
    q_inc = q_inc if isinstance(q_inc, pd.DataFrame) else pd.DataFrame()
    q_cf  = q_cf  if isinstance(q_cf,  pd.DataFrame) else pd.DataFrame()
    a_bal = a_bal if isinstance(a_bal, pd.DataFrame) else pd.DataFrame()
    a_inc = a_inc if isinstance(a_inc, pd.DataFrame) else pd.DataFrame()
    a_cf  = a_cf  if isinstance(a_cf,  pd.DataFrame) else pd.DataFrame()

    q_periods = frame_to_periods(q_bal, q_inc, q_cf, ccy, limit=4)
    a_periods = frame_to_periods(a_bal, a_inc, a_cf, ccy, limit=6)

    both = (a_periods + q_periods)
    both.sort(key=lambda p: (p.get("date") or ""), reverse=True)

    # Attach shares_out to EVERY period (nearest mapping with window)
    both, latest_shares, shares_meta = _map_shares_to_periods(both, shares_series, info_shares, window_days=90)

    # Derived metrics
    derived_latest = compute_derived_latest(both, shares_override=latest_shares)

    # Outdated flag
    is_outdated = True
    if derived_latest and derived_latest.get("date"):
        dt = pd.to_datetime(derived_latest["date"]).date()
        cutoff = FRESH_QUARTERLY_DAYS if q_periods else FRESH_ANNUAL_DAYS
        is_outdated = (pd.Timestamp.utcnow().date() - dt).days > cutoff

    meta = {
        "schema_version": "core.v1",
        "ticker": original_ticker,
        "name": info.get("longName") or None,
        "exchange": info.get("exchange") or None,
        "country_iso": (info.get("country") or "").upper() or None,
        "sector": info.get("sector") or None,
        "industry": info.get("industry") or None,
        "fye_month": None,
        "website": info.get("website") or None,
        "ipo_date": None,
        "employees": info.get("fullTimeEmployees") or None,
        "ids": {"cik": None},
        "sic": None,
        "entity_type": None,
        "generated_at": _now_iso(),
        "source": "yfinance",
        "y_symbol": y_symbol,
    }

    derived_block = {}
    if derived_latest:
        derived_block = {
            "latest": {
                **derived_latest,
                "as_of": pd.Timestamp.utcnow().date().isoformat(),
                "source": "yfinance",
                "is_outdated": bool(is_outdated),
            }
        }
        if latest_shares is not None:
            derived_block["latest"]["shares_source"] = shares_meta.get("latest_source")
        if info_shares is not None and info_shares_key:
            derived_block["latest"]["shares_info_key"] = info_shares_key

    return {
        "meta": meta,
        "financials": {
            "annual": {"periods": a_periods},
            "quarterly": {"periods": q_periods},
            # all periods (annual+quarterly with shares mapped) for convenience
            "all": {"periods": both}
        },
        "derived": derived_block,
    }

# -----------------------------------------------------------------------------
# Insider % resolver (query2 + referer) with DF + HTML fallbacks
# -----------------------------------------------------------------------------

def _yahoo_quote_summary(y_symbol: str, modules: List[str]) -> dict:
    """
    Yahoo quoteSummary via query2 with region/lang/corsDomain and realistic referer.
    Still paced + backoff; 401/404 => {}.
    """
    _pace(_JSON_BUCKET, YF_RPS_JSON)
    url = f"https://query2.finance.yahoo.com/v10/finance/quoteSummary/{y_symbol}"
    params = {
        "modules": ",".join(modules),
        "lang": "en-US",
        "region": "US",
        "corsDomain": "finance.yahoo.com",
    }
    headers = {
        **_YF_SESS.headers,
        "Referer": f"https://finance.yahoo.com/quote/{y_symbol}/holders",
        "Accept": "application/json, text/plain, */*",
    }
    for attempt in range(4):
        try:
            resp = _YF_SESS.get(url, params=params, headers=headers, timeout=20)
            code = getattr(resp, "status_code", 0)
            if code == 200:
                js = resp.json()
                res = (js.get("quoteSummary", {}).get("result") or [{}])[0] or {}
                if res is None:
                    res = {}
                return res
            if code in (401, 404):
                LOGGER.info("QS %s %s modules=%s → empty", code, y_symbol, params["modules"])
                return {}
            if code in (429, 500, 502, 503, 504):
                back = min(8.0, 0.8 * (2 ** attempt))
                LOGGER.warning("QS %s for %s — retry %.1fs", code, y_symbol, back)
                time.sleep(back); _pace(_JSON_BUCKET, YF_RPS_JSON); continue
            LOGGER.warning("QS %s for %s body=%.120s", code, y_symbol, getattr(resp, "text", "")[:120])
            return {}
        except Exception as e:
            back = min(8.0, 0.8 * (2 ** attempt))
            LOGGER.warning("QS error for %s (%s) — retry %.1fs", y_symbol, e, back)
            time.sleep(back); _pace(_JSON_BUCKET, YF_RPS_JSON)
    return {}


def _extract_percent_from_df(df) -> Optional[float]:
    try:
        if df is None or df.empty:
            return None
        # scan a few rows for "insider" and a percentage
        for i in range(min(len(df), 6)):
            row = df.iloc[i]
            text = " ".join(str(x) for x in (row.values if hasattr(row, "values") else [row]))
            if "insider" in text.lower():
                m = re.search(r"([0-9]+(?:\\.[0-9]+)?)\\s*%", text)
                if m:
                    return float(m.group(1)) / 100.0
        # fallback by columns
        for col in df.columns:
            ser = df[col].astype(str).str.lower()
            hits = ser[ser.str.contains("insider")]
            if not hits.empty:
                idx = hits.index[0]
                row = df.loc[idx]
                s = " ".join(str(x) for x in getattr(row, "values", [row]))
                m = re.search(r"([0-9]+(?:\\.[0-9]+)?)\\s*%", s)
                if m:
                    return float(m.group(1)) / 100.0
    except Exception:
        pass
    return None


def _yahoo_holders_html(y_symbol: str) -> Optional[str]:
    """
    Fetch the holders page with a realistic referer and accept headers.
    We only accept pages that contain the root.App.main JSON bootstrap.
    """
    _pace(_JSON_BUCKET, YF_RPS_JSON)
    url = f"https://finance.yahoo.com/quote/{y_symbol}/holders"
    headers = {
        "User-Agent": "Mozilla/5.0",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Referer": f"https://finance.yahoo.com/quote/{y_symbol}",
    }
    try:
        resp = _YF_SESS.get(url, headers=headers, timeout=20)
        if resp.status_code == 200 and "root.App.main" in resp.text:
            return resp.text
        if resp.status_code in (401, 404):
            LOGGER.info("holders HTML %s for %s", resp.status_code, y_symbol)
            return None
        LOGGER.warning("holders HTML %s for %s", resp.status_code, y_symbol)
    except Exception as e:
        LOGGER.debug("holders HTML error for %s: %s", y_symbol, e)
    return None


def _extract_percent_from_html(html: str) -> Optional[float]:
    """
    Parse insidersPercentHeld from the root.App.main JSON bootstrap.
    Fallback to a %-literal near 'Insider' if the JSON path is missing.
    """
    try:
        m = re.search(r"root\\.App\\.main\\s*=\\s*(\{.*?\})\\s*;\\s*\\n", html, re.DOTALL)
        if m:
            import json as _json
            blob = _json.loads(m.group(1))
            stores = (((blob or {}).get("context") or {}).get("dispatcher") or {}).get("stores") or {}
            qss = (stores.get("QuoteSummaryStore") or {})
            mhb = qss.get("majorHoldersBreakdown") or {}
            node = mhb.get("insidersPercentHeld")
            if isinstance(node, dict) and node.get("raw") is not None:
                return float(node["raw"])
        m2 = re.search(r"Insider[s]?[^%]{0,80}?([0-9]+(?:\\.[0-9]+)?)\\s*%", html, re.IGNORECASE)
        if m2:
            return float(m2.group(1)) / 100.0
    except Exception:
        pass
    return None


def resolve_insiders_percent_held(y_symbol: str) -> Tuple[Optional[float], Optional[str]]:
    # 1) query2 JSON
    store = _yahoo_quote_summary(y_symbol, ["majorHoldersBreakdown", "insiderHolders"])
    try:
        mh = store.get("majorHoldersBreakdown") or {}
        node = mh.get("insidersPercentHeld")
        if isinstance(node, dict) and node.get("raw") is not None:
            return float(node["raw"]), "quoteSummary"
    except Exception:
        pass
    # 2) yfinance DF
    if yf is not None:
        try:
            _pace(_INFO_BUCKET, YF_RPS_INFO)
            df = yf.Ticker(y_symbol).major_holders
            pct = _extract_percent_from_df(df)
            if pct is not None:
                return pct, "major_holders_df"
        except Exception:
            pass
    # 3) holders HTML
    html = _yahoo_holders_html(y_symbol)
    if html:
        pct = _extract_percent_from_html(html)
        if pct is not None:
            return pct, "holders_html"
    return None, None


def insider_from_yahoo_api(y_symbol: str) -> Dict[str, Any]:
    store_tx = _yahoo_quote_summary(y_symbol, ["insiderTransactions"])
    buys = sells = 0
    bsh = ssh = 0.0
    try:
        tx = store_tx.get("insiderTransactions") or []
        cutoff = pd.Timestamp.utcnow() - pd.Timedelta(days=180)
        for it in tx:
            raw = ((it or {}).get("startDate") or {}).get("raw")
            if raw is None:
                continue
            if pd.to_datetime(raw, unit="s") < cutoff:
                continue
            sh = float(((it.get("shares") or {}).get("raw")) or 0.0)
            tt = str(it.get("transactionText") or "").upper()
            if "PURCHASE" in tt or tt.startswith("P ") or tt == "P":
                buys += 1; bsh += abs(sh)
            elif "SALE" in tt or tt.startswith("S ") or tt == "S":
                sells += 1; ssh += abs(sh)
    except Exception:
        pass

    pct, source = resolve_insiders_percent_held(y_symbol)
    status = "ok" if (buys or sells or bsh or ssh or pct is not None) else "no_data"
    if status == "no_data":
        LOGGER.info("[insider] %s no_data (pct=%s, tx_ct=0) — not provided or throttled", y_symbol, pct)

    payload = {
        "as_of": _now_iso(),
        "buys_count": int(buys),
        "sells_count": int(sells),
        "buys_shares": float(bsh),
        "sells_shares": float(ssh),
        "net_shares": float(bsh - ssh),
        "signal": ("InsiderBuy" if (bsh > ssh) else ("InsiderSell" if (ssh > bsh) else "Neutral")),
        "status": status,
        "source": "yahoo_api",
        "insiders_percent_held": pct,
    }
    if pct is None:
        payload["status_reason"] = "not_available_or_throttled"
    if source:
        payload["insiders_percent_held_source"] = source
    return payload

# -----------------------------------------------------------------------------
# Orchestrator
# -----------------------------------------------------------------------------

def write_json(path: Path, obj: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(obj, indent=2), encoding="utf-8")

def run_for_row(ticker: str, y_symbol: str, sleep: float = 0.35) -> Dict[str, Any]:
    """Fetch core & insiders using y_symbol, but write files under original ticker."""
    core_path = CORE_DIR / f"{ticker}_core.json"
    ins_path  = INS_DIR  / f"{ticker}.json"

    LOGGER.info("→ %s uses y_symbol=%s", ticker, y_symbol)

    core_obj = yf_build_core(y_symbol, ticker)
    write_json(core_path, core_obj)

    ins = insider_from_yahoo_api(y_symbol)
    ins_obj = {"ticker": ticker, "y_symbol": y_symbol, **ins}
    write_json(ins_path, ins_obj)

    LOGGER.info("✓ done %s", ticker)
    time.sleep(max(0.0, sleep))
    return {"ticker": ticker, "core": str(core_path), "insider": str(ins_path), "status": "ok"}

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--shortlist", default=str(ROOT / SHORTLIST_DEFAULT))
    ap.add_argument("--sleep", type=float, default=0.35)
    ap.add_argument("--one", help="run a single ORIGINAL ticker (non-US), e.g., 1420.JP")
    ap.add_argument("--verbose", action="store_true", help="log progress verbosely")
    args = ap.parse_args()

    logging.basicConfig(
        level=(logging.DEBUG if args.verbose else logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s"
    )
    LOGGER.info("non-US run starting")

    rows = load_shortlist_rows(Path(args.shortlist))
    logs: List[Dict[str, Any]] = []

    if args.one:
        t = args.one.strip().upper()
        if is_us_ticker(t):
            raise SystemExit("--one expects a non-US ticker (e.g., 0591.HK, 1420.JP)")
        ysym = resolve_y_symbol(t, rows) or t
        logs.append(run_for_row(t, ysym, sleep=args.sleep))
    else:
        for r in rows:
            t = r["ticker"].strip().upper()
            if not t:
                continue
            if is_us_ticker(t):
                LOGGER.info("[skip] %s is US — skipping in non-US run", t)
                continue
            ysym = (r.get("y_symbol") or "").strip() or t
            try:
                logs.append(run_for_row(t, ysym, sleep=args.sleep))
            except Exception as e:
                LOGGER.exception("[error] %s failed", t)
                logs.append({"ticker": t, "status": f"error:{e}"})

    report = {"generated_at": _now_iso(), "rows": logs}
    (ROOT / "reports").mkdir(parents=True, exist_ok=True)
    (ROOT / "reports" / "nonus_fetch_companyfacts_log.json").write_text(
        json.dumps(report, indent=2), encoding="utf-8"
    )

if __name__ == "__main__":
    main()
