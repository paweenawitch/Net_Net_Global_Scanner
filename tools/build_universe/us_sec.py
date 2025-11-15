from __future__ import annotations
import os, json, re
from pathlib import Path
from datetime import datetime, timezone
import requests
import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
DATA = ROOT / "data" / "tickers"
DATA.mkdir(parents=True, exist_ok=True)

OUT_CSV  = DATA / "us_full.csv"
OUT_META = DATA / "us_full.meta.json"
CACHE_JSON = ROOT / "cache" / "sec_company_tickers.json"
CACHE_JSON.parent.mkdir(parents=True, exist_ok=True)

SEC_UA = os.environ.get("SEC_USER_AGENT", "net_net_screener_global/1.0 (yourname@email.com)")

BAD_NAME_PAT = re.compile(r"(warrant|wts|rights?|unit|spac|acquisition|blank\s*check|trust|holding\s*co)", re.IGNORECASE)
BAD_CODE_PAT = re.compile(r"(-WT|-WTS|-WS|-U|-UN|-RT|-R|\s+WTS?|\s+UNIT|\s+RT)$", re.IGNORECASE)

def get_session() -> requests.Session:
    s = requests.Session()
    s.headers.update({"User-Agent": SEC_UA, "Accept-Encoding": "gzip, deflate"})
    return s

def fetch_company_tickers(sec: requests.Session) -> list[dict]:
    url = "https://www.sec.gov/files/company_tickers.json"
    r = sec.get(url, timeout=30)
    r.raise_for_status()
    data = r.json()
    CACHE_JSON.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
    rows: list[dict] = []
    it = data.items() if isinstance(data, dict) else enumerate(data)
    for _, row in it:
        t = (row.get("ticker") or "").strip().upper()
        if not t:
            continue
        rows.append({
            "ticker_base": t,
            "ticker": f"{t}.US",
            "name": (row.get("title") or "").strip(),
            "cik": int(row.get("cik_str", 0) or 0),
            "country": "US",
            "mic": "XNAS",
        })
    return rows

def looks_like_common(row) -> bool:
    name = row.get("name", "") or ""
    code = row.get("ticker_base", "") or ""
    if BAD_NAME_PAT.search(name): return False
    if BAD_CODE_PAT.search(code): return False
    return True

def sym_score(sym: str) -> int:
    s = sym or ""
    score = 0
    if any(ch.isdigit() for ch in s): score += 10
    if s.endswith("F"): score += 5
    if s.endswith("Y"): score += 5
    score += len(s)
    return score

def fetch_list() -> pd.DataFrame:
    s = get_session()
    rows = fetch_company_tickers(s)
    df = pd.DataFrame(rows)
    before = len(df)

    df = df.dropna(subset=["ticker_base", "ticker"]).drop_duplicates("ticker_base")
    mask_common = df.apply(looks_like_common, axis=1)
    df = df[mask_common].copy()
    df = df[df["cik"] > 0].copy()

    df["__score"] = df["ticker_base"].astype(str).map(sym_score)
    df = (df.sort_values(["cik", "__score", "ticker_base"]).groupby("cik", as_index=False).first())

    df = df[["ticker", "name", "cik", "ticker_base", "country", "mic"]].sort_values("ticker").reset_index(drop=True)
    df.to_csv(OUT_CSV, index=False)

    meta = {
        "source": "sec_company_tickers.json",
        "generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "rows": int(len(df)),
        "raw_rows": int(before),
        "path": str(OUT_CSV),
    }
    OUT_META.write_text(json.dumps(meta, indent=2), encoding="utf-8")

    print(f"✅ US: {len(df)} rows → {OUT_CSV}")
    return df

if __name__ == "__main__":
    fetch_list()
