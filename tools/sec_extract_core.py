# tools/sec_extract_core.py
#
# US Companyfacts (EDGAR) — **Shortlist-only** runner that keeps your
# working extraction logic *unchanged* and only swaps the universe loader.
#
# Why this version:
# • Your original tools/sec_extract_core.py works because of how it points
#   into `facts['facts']['us-gaap'][concept]['units']` and how dates are
#   collected. My previous shortlist wrapper diverged there and yielded
#   nulls. This file restores your exact facts logic and only changes
#   the universe selection to ncav_shortlist.csv + CIK mapping.
#
# Usage:
#   python tools/sec_extract_core_shortlist_fixed.py --force --verbose
#   python tools/sec_extract_core_shortlist_fixed.py --tickers AAPL.US MSFT.US
#
from __future__ import annotations
import os, csv, json, time
from pathlib import Path
from datetime import datetime, timedelta
from typing import Dict, List, Optional

import requests

# ------------------------ Config ------------------------
SEC_UA = os.environ.get("SEC_USER_AGENT", "net_net_screener_global/1.0 (yourname@email.com)")
SLEEP = float(os.environ.get("SEC_SLEEP", "0.35"))

ROOT = Path(__file__).resolve().parents[1]
# shortlist by default
UNIVERSE_CSV = ROOT / "data" / "tickers" / "ncav_shortlist.csv"
CORE_DIR = ROOT / "cache" / "sec_core"
LOG_DIR = ROOT / "reports"
REFDATA_DIR = ROOT / "cache" / "refdata"
CORE_DIR.mkdir(parents=True, exist_ok=True)
LOG_DIR.mkdir(parents=True, exist_ok=True)
REFDATA_DIR.mkdir(parents=True, exist_ok=True)

FACTS_URL       = "https://data.sec.gov/api/xbrl/companyfacts/CIK{cik:010d}.json"
SUBMISSIONS_URL = "https://data.sec.gov/submissions/CIK{cik:010d}.json"
COMPANY_TICKERS_JSON = "https://www.sec.gov/files/company_tickers.json"

# --------------------- HTTP / Universe ------------------
def session() -> requests.Session:
    s = requests.Session()
    s.headers.update({"User-Agent": SEC_UA, "Accept-Encoding": "gzip, deflate"})
    return s

def _load_sec_ticker_map(sess: requests.Session) -> Dict[str, int]:
    cachef = REFDATA_DIR / "company_tickers.json"
    try:
        data = json.loads(cachef.read_text("utf-8"))
    except Exception:
        r = sess.get(COMPANY_TICKERS_JSON, timeout=60)
        r.raise_for_status()
        data = r.json()
        cachef.write_text(json.dumps(data), encoding="utf-8")
    out: Dict[str,int] = {}
    for _, v in (data or {}).items():
        t = (v.get("ticker") or "").upper().strip()
        c = v.get("cik_str")
        if t and c:
            out[t] = int(c)
    return out

def load_universe_shortlist() -> List[Dict]:
    """Read ncav_shortlist.csv, keep .US only, map to CIK via SEC map."""
    rows = []
    with open(UNIVERSE_CSV, newline="", encoding="utf-8") as f:
        rd = csv.DictReader(f)
        for r in rd:
            t = (r.get("ticker") or r.get("symbol") or "").strip().upper()
            if t.endswith(".US"):
                rows.append({"ticker": t, "name": r.get("name","")})
    with session() as s:
        sec_map = _load_sec_ticker_map(s)
    out = []
    for r in rows:
        sym = r["ticker"].replace(".US", "")
        cik = sec_map.get(sym)
        if cik:
            out.append({"ticker": r["ticker"], "name": r.get("name",""), "cik": int(cik)})
    return out

# --------------------- Data fetch -----------------------

def fetch_companyfacts(sess: requests.Session, cik: int) -> dict:
    url = FACTS_URL.format(cik=cik)
    r = sess.get(url, timeout=60)
    if not r.ok:
        raise RuntimeError(f"http{r.status_code}")
    return r.json()

def fetch_submissions(sess: requests.Session, cik: int) -> dict:
    url = SUBMISSIONS_URL.format(cik=cik)
    r = sess.get(url, timeout=60)
    if not r.ok:
        # Submissions are helpful but not strictly required for core; return {}
        return {}
    return r.json()

# ---------------------- Fact helpers --------------------
# (IDENTICAL to your working tools/sec_extract_core.py)

def iter_points(facts: dict, concept: str) -> List[dict]:
    """Return all numeric points for a us-gaap concept across all units, newest first."""
    try:
        node = facts["facts"]["us-gaap"][concept]
    except Exception:
        return []
    out: List[dict] = []
    for unit, arr in (node.get("units") or {}).items():
        for pt in arr or []:
            v = pt.get("val")
            if isinstance(v, (int, float)):
                out.append({
                    "end": pt.get("end") or "",
                    "fp": (pt.get("fp") or "").upper(),
                    "fy": pt.get("fy"),
                    "form": (pt.get("form") or "").upper(),
                    "accn": pt.get("accn"),
                    "val": float(v),
                    "unit": unit,
                })
    out.sort(key=lambda x: x["end"], reverse=True)
    return out

def pick_at_date(facts: dict, concept: str, end_date: str) -> Optional[dict]:
    for pt in iter_points(facts, concept):
        if pt["end"] == end_date:
            return {
                "val": pt["val"],
                "src": f"us-gaap:{concept}",
                "form": pt.get("form"),
                "unit": pt.get("unit"),
                "fy": pt.get("fy"),
                "fp": pt.get("fp"),
                "accn": pt.get("accn"),
            }
    return None

# ---- Shares fallback priority (end-of-period preferred; WA ok if needed)
SHARE_CONCEPTS_PRIORITY = [
    ("CommonStockSharesOutstanding",                    "shares"),
    ("EntityCommonStockSharesOutstanding",              "shares"),
    ("CommonStockSharesOutstandingRestated",            "shares"),
    ("WeightedAverageNumberOfDilutedSharesOutstanding", "shares"),
    ("WeightedAverageNumberOfSharesOutstandingBasic",   "shares"),
    ("WeightedAverageNumberOfSharesOutstanding",        "shares"),
]

def pick_point_at_date_for_unit(facts: dict, concept: str, end_date: str, unit_hint: Optional[str]=None):
    try:
        node = facts["facts"]["us-gaap"][concept]
    except Exception:
        return None
    units = node.get("units") or {}
    keys = list(units.keys())
    ordered = []
    if unit_hint and unit_hint in units:
        ordered.append(unit_hint)
    ordered += [u for u in keys if (u != unit_hint and "shares" in u.lower())]
    ordered += [u for u in keys if (u not in ordered)]
    for uk in ordered:
        for pt in units[uk] or []:
            if pt.get("end") == end_date and isinstance(pt.get("val"), (int,float)):
                d = {
                    "val": float(pt["val"]),
                    "src": f"us-gaap:{concept}",
                    "form": pt.get("form"),
                    "unit": uk,
                    "fy": pt.get("fy"),
                    "fp": pt.get("fp"),
                    "accn": pt.get("accn"),
                }
                if "WeightedAverage" in concept:
                    d["approx_weighted_avg"] = True
                return d
    return None

def pick_shares_at_date(facts: dict, end_date: str):
    for concept, hint in SHARE_CONCEPTS_PRIORITY:
        got = pick_point_at_date_for_unit(facts, concept, end_date, hint)
        if got is not None:
            return got
    return None

# --------------------- Currency detection ----------------
CURRENCY_TOKENS = {
    "USD","EUR","GBP","JPY","CAD","AUD","CHF","CNY","HKD","KRW",
    "SEK","NOK","DKK","INR","BRL","MXN","ZAR","TWD","SGD","NZD","PLN","TRY","ILS"
}

def _parse_currency_from_unit(unit: Optional[str]) -> Optional[str]:
    if not unit:
        return None
    head = unit.split("/")[0].upper()
    return head if head in CURRENCY_TOKENS else None

def detect_currency(facts: dict) -> Optional[str]:
    probes = ["Assets","AssetsCurrent","Liabilities","StockholdersEquity","SalesRevenueNet","NetIncomeLoss"]
    for concept in probes:
        for pt in iter_points(facts, concept):
            cur = _parse_currency_from_unit(pt.get("unit"))
            if cur:
                return cur
    return None

# --------------------- Build periods --------------------
BALANCE_CONCEPTS = {
    "assets_total":    "Assets",
    "assets_current":  "AssetsCurrent",
    "cash":            "CashAndCashEquivalentsAtCarryingValue",
    "short_invest":    "MarketableSecuritiesCurrent",
    "receivables":     "AccountsReceivableNetCurrent",
    "inventory":       "InventoryNet",
    "liab_total":      "Liabilities",
    "liab_current":    "LiabilitiesCurrent",
    "liab_noncurrent": "LiabilitiesNoncurrent",
    "equity":          "StockholdersEquity",
}
INCOME_CONCEPTS = {
    "revenue":       "SalesRevenueNet",
    "gross_profit":  "GrossProfit",
    "oper_income":   "OperatingIncomeLoss",
    "net_income":    "NetIncomeLoss",
}
CF_CONCEPTS = {
    "cfo":            "NetCashProvidedByUsedInOperatingActivities",
    "capex":          "PaymentsToAcquirePropertyPlantAndEquipment",
    "dividends_paid": "PaymentsOfDividends",
}

def collect_dates(facts: dict, allowed_fp: set, anchors: tuple, limit: int) -> List[str]:
    dates = set()
    for concept in anchors:
        for pt in iter_points(facts, concept):
            if pt["fp"] in allowed_fp and pt["end"]:
                dates.add(pt["end"])
    if not dates:
        for pt in iter_points(facts, "Assets"):
            if pt["fp"] in allowed_fp and pt["end"]:
                dates.add(pt["end"])
    return sorted(dates, reverse=True)[:limit]

def build_period(facts: dict, end_date: str) -> dict:
    bal, inc, cf = {}, {}, {}
    for k, c in BALANCE_CONCEPTS.items():
        v = pick_at_date(facts, c, end_date)
        if v is not None: bal[k] = v
    # dedicated shares selector with fallbacks
    shares = pick_shares_at_date(facts, end_date)
    if shares is not None:
        bal["shares_out"] = shares
    for k, c in INCOME_CONCEPTS.items():
        v = pick_at_date(facts, c, end_date)
        if v is not None: inc[k] = v
    for k, c in CF_CONCEPTS.items():
        v = pick_at_date(facts, c, end_date)
        if v is not None: cf[k] = v
    return {
        "date": end_date,
        "currency": detect_currency(facts),
        "balance": bal,
        "income": inc,
        "cashflow": cf
    }

def build_period_sets(facts: dict) -> Dict[str, dict]:
    anchors = ("AssetsCurrent","Liabilities","LiabilitiesCurrent","LiabilitiesNoncurrent","CommonStockSharesOutstanding")
    annual_dates    = collect_dates(facts, {"FY"},        anchors, limit=5)
    quarterly_dates = collect_dates(facts, {"Q1","Q2","Q3","Q4"}, anchors, limit=4)
    return {
        "annual":   {"periods": [build_period(facts, d) for d in annual_dates]},
        "quarterly":{"periods": [build_period(facts, d) for d in quarterly_dates]},
    }

# ---------------------- Derived latest ------------------
def _gv(block, key):
    node = (block or {}).get(key) or {}
    return node.get("val")

def derive_latest(period: dict) -> dict:
    b = period.get("balance", {}) or {}
    CA  = _gv(b, "assets_current")
    TL  = _gv(b, "liab_total")
    LC  = _gv(b, "liab_current")
    LNC = _gv(b, "liab_noncurrent")
    SH  = _gv(b, "shares_out")

    if TL is None and (LC is not None and LNC is not None):
        TL = LC + LNC

    cash = _gv(b, "cash") or 0.0
    sti  = _gv(b, "short_invest") or 0.0
    ar   = _gv(b, "receivables") or 0.0
    inv  = _gv(b, "inventory") or 0.0

    ncav = (CA - TL) if (CA is not None and TL is not None) else None
    adj_ncav = (cash + 0.75*ar + 0.5*inv - TL) if TL is not None else None

    ncav_ps     = (ncav / SH) if (ncav is not None and SH not in (None,0)) else None
    adj_ncav_ps = (adj_ncav / SH) if (adj_ncav is not None and SH not in (None,0)) else None

    return {
        "date": period.get("date"),
        "ncav": ncav,
        "ncav_ps": ncav_ps,
        "adj_ncav": adj_ncav,
        "adj_ncav_ps": adj_ncav_ps,
        "cash_plus_sti": cash + sti if (cash is not None and sti is not None) else None,
        "shares_is_weighted_avg": bool((b.get("shares_out") or {}).get("approx_weighted_avg", False))
    }

# --------------------- SIC → industry group --------------
def _industry_group_from_sic(sic: Optional[int], sic_desc: Optional[str]) -> Optional[str]:
    if not sic:
        return None
    try:
        s = int(sic)
    except Exception:
        return None
    if 6000 <= s < 6800: return "Financials"
    if 4900 <= s < 5000: return "Utilities"
    if 6500 <= s < 6800: return "Real Estate / Funds"
    if 1000 <= s < 1500: return "Metals/Mining"
    if 1200 <= s < 1400: return "Coal"
    if 1300 <= s < 1400: return "Oil & Gas"
    if 2800 <= s < 2900: return "Chemicals"
    if 7300 <= s < 7400: return "Services/Tech"
    if 0 < s < 1000 or 1500 <= s < 9000: return "General Industry"
    return None

def _business_country_from_subs(subs: dict) -> Optional[str]:
    try:
        biz = (subs.get("addresses") or {}).get("business") or {}
        return (biz.get("country") or "").upper() or None
    except Exception:
        return None

# ------------------------- Build core -------------------
def build_core_object(ticker: str, name: str, cik: int, facts: dict, subs: dict) -> dict:
    financials = build_period_sets(facts)  # 5 annual + 4 quarterly
    both = (financials["annual"]["periods"] or []) + (financials["quarterly"]["periods"] or [])
    both.sort(key=lambda x: x.get("date",""), reverse=True)
    latest = derive_latest(both[0]) if both else {}

    sic  = None
    sicd = None
    fy_end = None
    entity_type = None
    country_iso = None
    try:
        sic = subs.get("sic")
        sicd = subs.get("sicDescription")
        fy_end = subs.get("fiscalYearEnd")  # e.g., "1231"
        entity_type = subs.get("entityType")
        country_iso = _business_country_from_subs(subs) or "US"
    except Exception:
        country_iso = "US"

    fye_month = None
    if fy_end and len(str(fy_end)) >= 2:
        try:
            fye_month = int(str(fy_end)[:2])
        except Exception:
            fye_month = None

    industry_group = _industry_group_from_sic(sic, sicd)

    return {
        "meta": {
            "schema_version": "core.v1",
            "ticker": ticker,
            "name": name or None,
            "exchange": None,
            "country_iso": country_iso,
            "sector": industry_group,
            "industry": sicd,
            "fye_month": fye_month,
            "website": None,
            "ipo_date": None,
            "employees": None,
            "ids": { "cik": f"{cik:010d}" },
            "sic": sic,
            "entity_type": entity_type,
            "generated_at": datetime.utcnow().isoformat(timespec="seconds") + "Z",
            "source": "SEC companyfacts + submissions",
        },
        "financials": financials,
        "derived": {"latest": latest} if latest else {},
    }

# ------------------------- Runner ----------------------
def main(max_names: int = 0, sleep: float = SLEEP, shard: int = 1, of: int = 1,
         skip_days: int = 7, force: bool = False, tickers: Optional[List[str]] = None,
         verbose: bool = False):
    print(f"[INFO] output -> {CORE_DIR.resolve()}")
    all_rows = load_universe_shortlist() if not tickers else load_universe_shortlist()

    # explicit tickers bypass shard
    if tickers:
        want = set(tickers)
        pairs = [(i, r["ticker"], r.get("name",""), r["cik"]) for i,r in enumerate(all_rows) if r["ticker"] in want]
    else:
        pairs = [(i, r["ticker"], r.get("name",""), r["cik"]) for i,r in enumerate(all_rows)]
        if of > 1:
            pairs = [p for p in pairs if (p[0] % of) == (shard - 1)]

    if max_names:
        pairs = pairs[:max_names]

    print(f"[INFO] selected {len(pairs)} of {len(all_rows)} (tickers_filter={bool(tickers)} shard {shard}/{of})")

    sess = session()
    fresh_cutoff = datetime.utcnow() - timedelta(days=skip_days)
    logs = []

    for j, (_, tkr, name, cik) in enumerate(pairs, 1):
        out = CORE_DIR / f"{tkr}_core.json"

        if not force and out.exists():
            mtime = datetime.utcfromtimestamp(out.stat().st_mtime)
            if mtime >= fresh_cutoff:
                if verbose: print(f"[SKIP fresh] {tkr} (mtime {mtime.isoformat()}Z)")
                continue

        try:
            if verbose: print(f"[{j}/{len(pairs)}] FETCH {tkr} (CIK {cik:010d})")
            facts = fetch_companyfacts(sess, cik)
            subs  = fetch_submissions(sess, cik)
            obj   = build_core_object(tkr, name, cik, facts, subs)
            out.write_text(json.dumps(obj, indent=2), encoding="utf-8")
            if verbose: print(f"  -> wrote {out.name}")
            logs.append({"ticker": tkr, "cik": cik, "status": "ok"})
        except Exception as e:
            print(f"[ERROR] {tkr}: {e}")
            logs.append({"ticker": tkr, "cik": cik, "status": f"error:{e}"})
        time.sleep(sleep)

    ts = datetime.utcnow().strftime("%Y%m%d_%H%M")
    (LOG_DIR / f"sec_core_extract_shortlist_{ts}.json").write_text(json.dumps(logs, indent=2), encoding="utf-8")
    print("Done.")

if __name__ == "__main__":
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument("--max", type=int, default=0)
    ap.add_argument("--sleep", type=float, default=SLEEP)
    ap.add_argument("--shard", type=int, default=1)
    ap.add_argument("--of", type=int, default=1)
    ap.add_argument("--skip-days", type=int, default=7)
    ap.add_argument("--force", action="store_true")
    ap.add_argument("--tickers", nargs="*", help="Specific tickers (e.g. AAL.US ACEL.US)")
    ap.add_argument("--verbose", action="store_true")
    args = ap.parse_args()

    main(
        max_names=args.max,
        sleep=args.sleep,
        shard=args.shard,
        of=args.of,
        skip_days=args.skip_days,
        force=args.force,
        tickers=args.tickers,
        verbose=args.verbose,
    )
