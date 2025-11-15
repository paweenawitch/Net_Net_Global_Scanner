from __future__ import annotations
from pathlib import Path
from datetime import datetime, timezone
import os, re, io, json
import requests
import pandas as pd
from urllib.parse import urljoin

ROOT = Path(__file__).resolve().parents[2]
DATA = ROOT / "data" / "tickers"
DATA.mkdir(parents=True, exist_ok=True)

OUT = DATA / "jp_full.csv"
OUT_META = DATA / "jp_full.meta.json"
FALLBACK = DATA / "jp_source.csv"

ENV_XLS = os.environ.get("JPX_PRIMARY_XLS_URL", "").strip()
CANDIDATE_PAGES = [
    "https://www.jpx.co.jp/markets/equities/ss-reg/",
    "https://www.jpx.co.jp/english/markets/equities/ss-reg/",
    "https://www.jpx.co.jp/english/markets/statistics-equities/misc/01.html",
]
XLS_PAT = re.compile(r"Primary_Listing_Markets\.xls$", re.IGNORECASE)
CODE_PAT = re.compile(r"^\d{4}$")
DUMB_JPX_CSV = "https://dumbstockapi.com/stock?format=csv&exchanges=JPX"

def _download(s: requests.Session, url: str) -> bytes:
    r = s.get(url, timeout=60); r.raise_for_status(); return r.content

def _find_primary_xls(s: requests.Session) -> str | None:
    for page in CANDIDATE_PAGES:
        try:
            r = s.get(page, timeout=30)
            if not r.ok: continue
            for h in re.findall(r'href="([^"]+)"', r.text):
                if XLS_PAT.search(h): return urljoin(page, h)
        except Exception: pass
    return None

def _parse_primary_xls(content: bytes) -> pd.DataFrame:
    df = pd.read_excel(io.BytesIO(content))
    cols = {str(c).strip().lower(): c for c in df.columns}
    code_col = None; name_col = None
    for k, c in cols.items():
        if ("code" in k and "stock" in k) or k in ("code", "securities code", "証券コード"): code_col = c
        if ("name" in k and "stock" in k) or ("name" in k) or ("銘柄" in k): name_col = c
    if code_col is None or name_col is None: code_col, name_col = list(df.columns)[:2]
    out = df[[code_col, name_col]].copy(); out.columns = ["ticker_base","name"]
    out["ticker_base"] = out["ticker_base"].astype(str).str.extract(r"(\d{4})")[0]
    out = out.dropna(subset=["ticker_base","name"]); out = out[out["ticker_base"].str.match(CODE_PAT)]
    out["ticker"] = out["ticker_base"] + ".JP"; out["country"] = "JP"; out["mic"] = "XJPX"
    return out.drop_duplicates("ticker_base").reset_index(drop=True)

def _parse_dumb_csv(content: bytes) -> pd.DataFrame:
    df = pd.read_csv(io.BytesIO(content))
    if not {"ticker","name","exchange"}.issubset(df.columns): return pd.DataFrame()
    df = df[df["exchange"].astype(str).str.upper() == "JPX"].copy()
    df["ticker_base"] = df["ticker"].astype(str).str.extract(r"(\d{4})")[0]
    df = df.dropna(subset=["ticker_base"])
    df["ticker"] = df["ticker_base"] + ".JP"; df["country"] = "JP"; df["mic"] = "XJPX"
    return df[["ticker_base","ticker","name","country","mic"]].drop_duplicates("ticker_base").reset_index(drop=True)

def _write(df: pd.DataFrame, source: str):
    df.to_csv(OUT, index=False)
    meta = {"source": source, "generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
            "rows": int(len(df)), "path": str(OUT)}
    OUT_META.write_text(json.dumps(meta, indent=2), encoding="utf-8")
    print(f"✅ JP: {len(df)} rows ({source}) → {OUT}")

def fetch_list() -> pd.DataFrame:
    s = requests.Session()
    # A) ENV xls
    if ENV_XLS:
        try:
            c = _download(s, ENV_XLS); df = _parse_primary_xls(c); _write(df, "jpx_xls_env"); return df
        except Exception as e: print(f"[JP] env xls failed: {e}")
    # B) Crawl
    try:
        xls = _find_primary_xls(s)
        if xls:
            c = _download(s, xls); df = _parse_primary_xls(c); _write(df, "jpx_xls_crawl"); return df
    except Exception as e: print(f"[JP] crawl failed: {e}")
    # C) Consolidated roster
    try:
        c = _download(s, DUMB_JPX_CSV); df = _parse_dumb_csv(c)
        if not df.empty: _write(df, "dumbstockapi"); return df
    except Exception as e: print(f"[JP] dumb fallback failed: {e}")
    # D) Manual
    if FALLBACK.exists():
        try:
            df = pd.read_csv(FALLBACK)
            if {"ticker_base","name"}.issubset(df.columns):
                df = df[["ticker_base","name"]].dropna().copy()
                df["ticker"] = df["ticker_base"].astype(str) + ".JP"; df["country"]="JP"; df["mic"]="XJPX"
                _write(df, "manual"); return df
        except Exception as e: print(f"[JP] manual failed: {e}")
    # E) Empty
    df = pd.DataFrame(columns=["ticker_base","ticker","name","country","mic"]); _write(df, "empty"); return df

if __name__ == "__main__":
    fetch_list()
