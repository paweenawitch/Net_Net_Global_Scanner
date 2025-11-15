#tools/sec_insider_scan.py
#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Insider (Form 4) scanner — shortlist-only, robust XML parsing, holders %

• Universe: data/ncav_shortlist.csv (ONLY .US)
• For each .US ticker, map to CIK via SEC company_tickers.json (cached)
• Pull recent Form 4 / 4/A filings from submissions JSON
• For each recent filing, fetch the filing directory index.json and then the
  XML (form4.xml/doc4.xml/etc), parse P/S transactions + share totals
• Aggregate last N days (default 180) into buys_count/sells_count/shares
• Add insiders_percent_held via Yahoo QuoteSummary (majorHoldersBreakdown)
• Output: cache/sec_insider/{TICKER}.json

Run:
  python tools/sec_insider_scan.py --days-back 180 --verbose
  python tools/sec_insider_scan.py --only AAPL.US,MSFT.US --verbose

Reqs: pip install requests pandas
"""
from __future__ import annotations
import argparse, csv, json, logging, os, re, sys, time, xml.etree.ElementTree as ET
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple

import requests
import pandas as pd

# ---------------------------
# Root-anchored defaults
# ---------------------------
ROOT = Path(__file__).resolve().parents[1]   # repo root = one up from tools/
SEC_USER_AGENT = os.environ.get("SEC_USER_AGENT", "net_net_screener_global/1.0 (yourname@email.com)")

DEFAULT_OUTDIR    = ROOT / "cache" / "sec_insider"
DEFAULT_UNIVERSE  = ROOT / "data" / "tickers" / "ncav_shortlist.csv"  # shortlist only
DEFAULT_DAYS_BACK = 180
DEFAULT_SLEEP     = 0.2

SUBMISSIONS_URL       = "https://data.sec.gov/submissions/CIK{cik:010d}.json"
FILING_DIR_INDEX_URL  = "https://www.sec.gov/Archives/edgar/data/{cik}/{acc_nodash}/index.json"
COMPANY_TICKERS_JSON  = "https://www.sec.gov/files/company_tickers.json"
YF_QS_URL             = "https://query1.finance.yahoo.com/v10/finance/quoteSummary/{sym}"

BASE_HEADERS = {
    "User-Agent": SEC_USER_AGENT,
    "Accept": "application/json, text/plain, */*",
    "Accept-Encoding": "gzip, deflate",
    "Connection": "keep-alive",
}
YF_HEADERS = {"User-Agent": "Mozilla/5.0"}

LOGGER = logging.getLogger("sec_insider_scan")

# ---------------------------
# Small utils
# ---------------------------

def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00","Z")

def ensure_outdir(p: Path):
    p.mkdir(parents=True, exist_ok=True)

def write_json(p: Path, d: dict):
    p.write_text(json.dumps(d, indent=2), encoding="utf-8")

# ---------------------------
# HTTP helpers
# ---------------------------

def sec_session() -> requests.Session:
    s = requests.Session()
    s.headers.update(BASE_HEADERS)
    return s

def fetch(sess: requests.Session, url: str, timeout=30, retries=3, backoff=0.8) -> Optional[requests.Response]:
    for attempt in range(1, retries + 1):
        try:
            r = sess.get(url, timeout=timeout)
            if r is not None and r.ok:
                return r
            if r is not None and r.status_code in (429, 403, 503) and attempt < retries:
                sleep = backoff * attempt
                LOGGER.warning("HTTP %s on %s (attempt %d/%d) — retrying in %.2fs", r.status_code, url, attempt, retries, sleep)
                time.sleep(sleep)
                continue
            if r is not None:
                LOGGER.warning("HTTP %s on %s — giving up", r.status_code, url)
            return None
        except Exception as e:
            if attempt < retries:
                sleep = backoff * attempt
                LOGGER.warning("Error %s on %s (attempt %d/%d) — retrying in %.2fs", e, url, attempt, retries, sleep)
                time.sleep(sleep)
                continue
            LOGGER.exception("Error on %s — giving up", url)
            return None
    return None

# ---------------------------
# Universe + CIK map
# ---------------------------

def load_sec_ticker_map(sess: requests.Session, cache_dir: Path) -> Dict[str, int]:
    cache_dir.mkdir(parents=True, exist_ok=True)
    cf = cache_dir / "company_tickers.json"
    try:
        data = json.loads(cf.read_text("utf-8"))
    except Exception:
        r = fetch(sess, COMPANY_TICKERS_JSON, timeout=60)
        if not r:
            return {}
        data = r.json()
        cf.write_text(json.dumps(data), encoding="utf-8")
    out: Dict[str, int] = {}
    for _, v in (data or {}).items():
        t = (v.get("ticker") or "").upper().strip()
        c = v.get("cik_str")
        if t and c:
            out[t] = int(c)
    return out


def load_universe_shortlist(path: Path) -> List[str]:
    rows: List[str] = []
    with path.open(encoding="utf-8", newline="") as f:
        rdr = csv.DictReader(f)
        for r in rdr:
            t = (r.get("ticker") or r.get("symbol") or list(r.values())[0]).strip().upper()
            if t.endswith(".US"):
                rows.append(t)
    return sorted(set(rows))

# ---------------------------
# Yahoo holders %
# ---------------------------

def insiders_percent_held_yahoo(us_ticker: str) -> Optional[float]:
    try:
        sym = us_ticker.replace(".US", "")
        url = YF_QS_URL.format(sym=sym)
        params = {"modules": "majorHoldersBreakdown"}
        r = requests.get(url, params=params, headers=YF_HEADERS, timeout=20)
        if r.status_code != 200:
            LOGGER.info("Yahoo %s for %s — skipping holders%%", r.status_code, us_ticker)
            return None
        js = r.json()
        store = (js.get("quoteSummary", {}).get("result") or [{}])[0] or {}
        mh = store.get("majorHoldersBreakdown") or {}
        node = mh.get("insidersPercentHeld")
        if isinstance(node, dict) and node.get("raw") is not None:
            return float(node["raw"])
    except Exception as e:
        LOGGER.debug("Holders%% error for %s: %s", us_ticker, e)
    return None

# ---------------------------
# Filing helpers
# ---------------------------

def _accession_nodash(acc: str) -> str:
    return re.sub(r"[^0-9]", "", acc or "")


def list_recent_form4(sess: requests.Session, cik: int, days_back: int) -> List[Tuple[str, str]]:
    """Return [(acc_nodash, primaryDocument)] for recent Form 4 within days_back."""
    r = fetch(sess, SUBMISSIONS_URL.format(cik=cik), timeout=60)
    if not r:
        return []
    sub = r.json()
    rec = sub.get("filings", {}).get("recent", {})
    forms = rec.get("form", [])
    accs  = rec.get("accessionNumber", [])
    prims = rec.get("primaryDocument", [])
    dates = rec.get("filingDate", [])

    out: List[Tuple[str, str]] = []
    for i, f in enumerate(forms):
        try:
            if f not in ("4", "4/A"):
                continue
            fdate = dates[i] if i < len(dates) else None
            if not fdate:
                continue
            dt = pd.to_datetime(fdate)
            if (datetime.utcnow() - dt.to_pydatetime()).days > days_back:
                continue
            acc = accs[i] if i < len(accs) else None
            prim = prims[i] if i < len(prims) else None
            if not acc:
                continue
            out.append((_accession_nodash(acc), prim or ""))
        except Exception:
            continue
    return out


def fetch_filing_dir(sess: requests.Session, cik: int, acc_nodash: str) -> List[str]:
    """List files in the filing directory via index.json."""
    url = FILING_DIR_INDEX_URL.format(cik=cik, acc_nodash=acc_nodash)
    r = fetch(sess, url, timeout=30)
    if not r:
        return []
    try:
        js = r.json()
        files = [f.get("name") for f in (js.get("directory", {}).get("item") or [])]
        return [x for x in files if isinstance(x, str)]
    except Exception:
        return []


def pick_form4_xml(files: List[str], primary_hint: str = "") -> Optional[str]:
    cand = []
    for fn in files:
        lower = fn.lower()
        if not lower.endswith(".xml"):
            continue
        # prefer obvious form4 files
        score = 0
        if "form4" in lower or "doc4" in lower or "f4" in lower:
            score += 5
        if "primary" in lower:
            score += 2
        if primary_hint and lower.endswith(primary_hint.lower()):
            score += 3
        cand.append((score, fn))
    if not cand:
        return None
    cand.sort(reverse=True)
    return cand[0][1]


def fetch_xml(sess: requests.Session, cik: int, acc_nodash: str, filename: str) -> Optional[str]:
    base = f"https://www.sec.gov/Archives/edgar/data/{cik}/{acc_nodash}/{filename}"
    r = fetch(sess, base, timeout=30)
    if not r:
        return None
    try:
        return r.text
    except Exception:
        return None

# ---------------------------
# XML parsing for P/S
# ---------------------------

def _local(tag: str) -> str:
    return tag.split("}", 1)[-1] if "}" in tag else tag


def _first_text_any(node: ET.Element, name: str) -> Optional[str]:
    n = name.lower()
    for el in node.iter():
        if _local(el.tag).lower() == n:
            txt = (el.text or "").strip()
            if txt:
                return txt
            # Check nested <value>
            for v in el.iter():
                if _local(v.tag).lower() == "value" and (v.text or "").strip():
                    return (v.text or "").strip()
    return None


def _first_number_any(node: ET.Element, name: str) -> Optional[float]:
    txt = _first_text_any(node, name)
    if txt is None:
        return None
    try:
        return float(str(txt).replace(",", ""))
    except Exception:
        return None


def summarize_form4(xml_text: str, allowed_codes: Optional[Set[str]] = None) -> Dict[str, float]:
    buys = sells = 0
    bsh = ssh = 0.0
    try:
        root = ET.fromstring(xml_text)
    except Exception:
        return {"buys_count": 0, "sells_count": 0, "buys_shares": 0.0, "sells_shares": 0.0, "net_shares": 0.0}

    # Both nonDerivativeTable and derivativeTable can contain transactions
    tx_nodes = [el for el in root.iter() if _local(el.tag).lower().endswith("transaction")]
    for n in tx_nodes:
        code = _first_text_any(n, "transactionCode")
        code = code.strip().upper() if code else None
        if allowed_codes and (not code or code not in allowed_codes):
            continue
        price = _first_number_any(n, "transactionPricePerShare")
        if price == 0 and code != "P":
            # filter out administrative 0-priced entries except P
            continue
        sh = _first_number_any(n, "transactionShares") or 0.0
        if code == "P":
            buys += 1; bsh += abs(sh)
        elif code == "S":
            sells += 1; ssh += abs(sh)

    return {
        "buys_count": int(buys),
        "sells_count": int(sells),
        "buys_shares": float(bsh),
        "sells_shares": float(ssh),
        "net_shares": float(bsh - ssh),
    }

# ---------------------------
# Orchestrator per ticker
# ---------------------------

def process_ticker(sess: requests.Session, ticker_us: str, cik: int, days_back: int, allowed_codes: Set[str]) -> Dict[str, any]:
    LOGGER.info("⇒ %s (CIK %d)", ticker_us, cik)
    filings = list_recent_form4(sess, cik, days_back)
    buys = sells = 0
    bsh = ssh = 0.0

    for acc_nodash, prim in filings:
        files = fetch_filing_dir(sess, cik, acc_nodash)
        if not files:
            continue
        pick = pick_form4_xml(files, prim)
        if not pick:
            # last-resort: any XML
            xmls = [f for f in files if f.lower().endswith('.xml')]
            pick = xmls[0] if xmls else None
        if not pick:
            continue
        xml_text = fetch_xml(sess, cik, acc_nodash, pick)
        if not xml_text:
            continue
        s = summarize_form4(xml_text, allowed_codes)
        buys += s["buys_count"]; sells += s["sells_count"]
        bsh  += s["buys_shares"]; ssh   += s["sells_shares"]

    status = "ok" if (buys or sells or bsh or ssh) else "no_data"
    signal = "InsiderBuy" if (buys>0 and sells==0) or (bsh>ssh) else ("InsiderSell" if (sells>0 and buys==0) or (ssh>bsh) else "Neutral")

    payload = {
        "ticker": ticker_us,
        "as_of": now_iso(),
        "buys_count": int(buys),
        "sells_count": int(sells),
        "buys_shares": float(bsh),
        "sells_shares": float(ssh),
        "net_shares": float(bsh - ssh),
        "signal": signal,
        "status": status,
        "source": "edgar_form4_xml",
    }

    pct = insiders_percent_held_yahoo(ticker_us)
    if pct is not None:
        payload["insiders_percent_held"] = pct

    return payload

# ---------------------------
# CLI
# ---------------------------

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--days-back", type=int, default=DEFAULT_DAYS_BACK)
    ap.add_argument("--outdir", default=str(DEFAULT_OUTDIR))
    ap.add_argument("--universe", default=str(DEFAULT_UNIVERSE))
    ap.add_argument("--only", help="comma-separated .US tickers (AAPL.US,MSFT.US)")
    ap.add_argument("--codes", help="comma-separated txn codes to include (default: P,S)")
    ap.add_argument("--verbose", action="store_true")
    args = ap.parse_args()

    logging.basicConfig(level=(logging.DEBUG if args.verbose else logging.INFO),
                        format="%(asctime)s %(levelname)s %(name)s: %(message)s")

    out_dir = Path(args.outdir)
    if not out_dir.is_absolute():
        out_dir = ROOT / out_dir
    ensure_outdir(out_dir)

    allowed_codes = set(c.strip().upper() for c in args.codes.split(",")) if args.codes else {"P", "S"}

    if args.only:
        tickers = [s.strip().upper() for s in args.only.split(",") if s.strip()]
    else:
        tickers = load_universe_shortlist(Path(args.universe))

    # Build CIK map once
    with sec_session() as sess:
        secmap = load_sec_ticker_map(sess, ROOT / "cache" / "refdata")
        logs = []
        for t in tickers:
            if not t.endswith(".US"):
                LOGGER.info("[skip] %s is not .US — skipping in this US insider run", t)
                continue
            cik = secmap.get(t.replace(".US", ""))
            if not cik:
                LOGGER.warning("[skip] %s has no CIK mapping", t)
                continue
            try:
                payload = process_ticker(sess, t, cik, args.days_back, allowed_codes)
                outpath = out_dir / f"{t}.json"
                write_json(outpath, payload)
                LOGGER.info("[ok] %s -> %s", t, outpath)
                time.sleep(DEFAULT_SLEEP)
            except Exception:
                LOGGER.exception("[error] %s failed", t)
                logs.append({"ticker": t, "status": "error"})

if __name__ == "__main__":
    sys.exit(main())
