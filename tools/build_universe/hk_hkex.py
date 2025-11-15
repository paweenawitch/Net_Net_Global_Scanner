from __future__ import annotations
from pathlib import Path
from urllib.parse import urljoin
from datetime import datetime, timezone
import io, os, re, sys, runpy, json
import requests
import pandas as pd

# ---------- Paths ----------
ROOT = Path(__file__).resolve().parents[2]
DATA_TICKERS = ROOT / "data" / "tickers"
DATA_TICKERS.mkdir(parents=True, exist_ok=True)
OUT = DATA_TICKERS / "hk_full.csv"
OUT_META = DATA_TICKERS / "hk_full.meta.json"

# Your robust SEHK builder (uploaded as data/sehk/universe_builder.py)
SEHK_BUILDER = ROOT / "data" / "sehk" / "universe_builder.py"
SEHK_OUT_CSV = ROOT / "data" / "sehk" / "universe" / "sehk_master.csv"

# Manual fallback (optional)
MANUAL_FALLBACK = DATA_TICKERS / "hk_source.csv"  # columns: ticker_base,name

# ---------- Sources ----------
ENV_URL = os.environ.get("HKEX_LIST_URL", "").strip()
SEHK_XLS = "https://www.hkex.com.hk/eng/services/trading/securities/securitieslists/ListOfSecurities.xlsx"
CANDIDATE_PAGES = [
    "https://www.hkex.com.hk/eng/services/trading/securities/securitieslists.htm",
    "https://www.hkex.com.hk/tc/services/trading/securities/securitieslists.htm",
]
DUMB_HKEX_CSV = "https://dumbstockapi.com/stock?format=csv&exchanges=HKEX"

NUM4 = re.compile(r"^\d{4}$")

# ---------- Meta helper ----------
def _write_meta(source: str, rows: int):
    meta = {
        "source": source,
        "generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "rows": int(rows),
        "path": str(OUT),
    }
    OUT_META.write_text(json.dumps(meta, indent=2), encoding="utf-8")

# ---------- Utilities ----------
def _write_out(df: pd.DataFrame, tag: str) -> pd.DataFrame:
    """Normalize and write hk_full.csv + meta"""
    if df is None or df.empty:
        empty = pd.DataFrame(columns=["ticker_base","ticker","name","country","mic"])
        empty.to_csv(OUT, index=False)
        _write_meta(tag, 0)
        print(f"[HKEX:{tag}] wrote 0 rows → {OUT}")
        return empty

    out = df.copy()
    if "ticker_base" not in out.columns and "stock_code" in out.columns:
        out["ticker_base"] = out["stock_code"]
    out["ticker_base"] = (
        out["ticker_base"].astype(str).str.extract(r"(\d{1,5})")[0]
        .fillna("0").astype(int).astype(str).str.zfill(4)
    )
    out = out[out["ticker_base"].str.match(NUM4)]
    if "name" not in out.columns:
        for c in ["Name of Securities", "securities name", "english short name"]:
            if c in out.columns:
                out["name"] = out[c].astype(str)
                break
    out["name"] = out["name"].astype(str).fillna("")
    out["ticker"] = out["ticker_base"] + ".HK"
    out["country"] = "HK"
    out["mic"] = "XHKG"
    out = out[["ticker_base","ticker","name","country","mic"]].drop_duplicates("ticker_base").reset_index(drop=True)
    out.to_csv(OUT, index=False)
    _write_meta(tag, len(out))
    print(f"[HKEX:{tag}] wrote {len(out)} rows → {OUT}")
    return out

def _download(session: requests.Session, url: str) -> bytes:
    r = session.get(url, timeout=60, headers={"User-Agent":"NetNet-Global/1.0"})
    r.raise_for_status()
    return r.content

# ---------- Path A: Use your universe_builder.py ----------
def _from_sehk_builder() -> pd.DataFrame:
    if not SEHK_OUT_CSV.exists():
        if not SEHK_BUILDER.exists():
            raise FileNotFoundError(f"Missing {SEHK_BUILDER}")
        print("[HKEX:builder] running universe_builder.py …")
        runpy.run_path(str(SEHK_BUILDER))
    if not SEHK_OUT_CSV.exists():
        raise FileNotFoundError(f"Builder did not create {SEHK_OUT_CSV}")
    df = pd.read_csv(SEHK_OUT_CSV)
    if "stock_code" not in df.columns:
        raise ValueError("sehk_master.csv missing 'stock_code'")
    df = df.rename(columns={"stock_code":"ticker_base"})
    if "name" not in df.columns:
        df["name"] = df.get("Name of Securities", "")
    return _write_out(df[["ticker_base","name"]], tag="builder")

# ---------- Path B: Directly parse the official Excel ----------
# (same parsing logic as your working version; meta handled by _write_out)
def _canonize_header(cells) -> dict[int,str]:
    SYN = {
        "stock code": "Stock Code","stockcode": "Stock Code","sehk code": "Stock Code","code": "Stock Code",
        "name of securities": "Name of Securities","securities name": "Name of Securities",
        "english short name": "Name of Securities","security name": "Name of Securities",
        "category": "Category","class": "Category","type": "Category",
        "sub-category": "Sub-Category","subcategory": "Sub-Category","sub category": "Sub-Category",
        "board lot": "Board Lot","boardlot": "Board Lot","lot size": "Board Lot",
        "isin": "ISIN","isin code": "ISIN",
        "股份代號": "Stock Code","證券名稱": "Name of Securities","股票名稱": "Name of Securities",
        "類別": "Category","次類別": "Sub-Category","買賣單位": "Board Lot","國際證券號碼": "ISIN",
    }
    out = {}
    for j, raw in enumerate(cells):
        if raw is None: continue
        s = str(raw).strip()
        if not s or s.lower().startswith("unnamed"): continue
        key = SYN.get(s.lower()) or SYN.get(re.sub(r"\s+"," ", s.lower()))
        if not key:
            s2 = re.sub(r"[\(\)（）].*$","", s).strip().lower()
            key = SYN.get(s2)
        if key: out[j] = key
    return out

def _find_table(df: pd.DataFrame):
    for i in range(min(50, len(df))):
        mapping = _canonize_header(df.iloc[i].tolist())
        if len(set(mapping.values())) >= 3 and "Stock Code" in mapping.values():
            return i, mapping
    return None

def _sheet_to_table(xl: pd.ExcelFile, sheet_name: str) -> pd.DataFrame | None:
    raw = xl.parse(sheet_name, header=None, dtype=object)
    found = _find_table(raw)
    if not found: return None
    hdr_i, idx_map = found
    data = raw.iloc[hdr_i+1:].reset_index(drop=True).copy()
    cols = [idx_map.get(j) for j in range(raw.shape[1])]
    data.columns = cols
    keep = [c for c in data.columns if c in {"Stock Code","Name of Securities","Category","Sub-Category","Board Lot","ISIN"}]
    data = data[keep].dropna(how="all")
    return data

def _is_equity(cat: str, subcat: str) -> bool:
    deny = {"cbbc","callable bull/bear contracts","warrants","derivative warrants","warrant",
            "bond","bonds","debt","notes","perpetual","etf","exchange traded funds","fund","trust","reit",
            "rights","preference","stapled","structured","equity linked"}
    allow = {"equity","ordinary shares","common shares","primary equity","secondary equity"}
    c = (str(cat) or "").strip().lower(); s = (str(subcat) or "").strip().lower()
    if any(t in c for t in deny) or any(t in s for t in deny): return False
    if any(t in c for t in allow) or any(t in s for t in allow): return True
    if "equity" in c or "equity" in s or "ordinary" in s: return True
    return False

def _from_official_xls() -> pd.DataFrame:
    s = requests.Session()
    content = _download(s, SEHK_XLS)
    xl = pd.ExcelFile(io.BytesIO(content), engine="openpyxl")
    tables = []
    for sheet in xl.sheet_names:
        t = _sheet_to_table(xl, sheet)
        if t is not None and not t.empty:
            tables.append(t)
    if not tables: raise RuntimeError("No suitable table found in HKEX Excel")
    df = pd.concat(tables, ignore_index=True)
    df["Stock Code"] = (df["Stock Code"].astype(str).str.extract(r"(\d+)")[0].fillna("0").astype(int).astype(str).str.zfill(4))
    df["Name of Securities"] = df["Name of Securities"].astype(str).str.strip()
    df["Category"] = df.get("Category", "").astype(str).str.strip()
    df["Sub-Category"] = df.get("Sub-Category","").astype(str).str.strip()
    mask = df.apply(lambda r: _is_equity(r.get("Category",""), r.get("Sub-Category","")), axis=1)
    base = df[mask].drop_duplicates(subset=["Stock Code"]).copy()
    base = base.rename(columns={"Stock Code":"ticker_base","Name of Securities":"name"})
    return _write_out(base[["ticker_base","name"]], tag="excel")

# ---------- Other paths (unchanged except meta handled by _write_out) ----------
def _from_hkex_html() -> pd.DataFrame:
    s = requests.Session()
    for page in CANDIDATE_PAGES:
        try:
            content = _download(s, page)
            html = content.decode("utf-8", errors="ignore")
            tables = pd.read_html(io.StringIO(html))
            frames = []
            for t in tables:
                cols = [str(c).lower() for c in t.columns]
                has_code = any(("stock code" in c) or ("股份代號" in c) or ("股票代碼" in c) or ("code" in c) for c in cols)
                has_name = any(("name" in c) or ("名稱" in c) or ("公司名稱" in c) for c in cols)
                if not (has_code and has_name): continue
                cmap = {}
                for c in t.columns:
                    cl = str(c).lower()
                    if ("stock code" in cl) or ("股份代號" in cl) or ("股票代碼" in cl) or ("code" in cl): cmap[c] = "ticker_base"
                    elif ("name" in cl) or ("名稱" in cl) or ("公司名稱" in cl): cmap[c] = "name"
                tt = t.rename(columns=cmap)
                if {"ticker_base","name"}.issubset(tt.columns):
                    frames.append(tt[["ticker_base","name"]])
            if frames:
                df = pd.concat(frames, ignore_index=True)
                return _write_out(df, tag="html")
        except Exception as e:
            print(f"[HKEX:html] failed on {page}: {e}")
    return _write_out(pd.DataFrame(columns=["ticker_base","name"]), tag="html_empty")

def _from_consolidated() -> pd.DataFrame:
    try:
        s = requests.Session()
        csv = _download(s, DUMB_HKEX_CSV)
        df = pd.read_csv(io.BytesIO(csv))
        if {"ticker","name","exchange"}.issubset(df.columns):
            df = df[df["exchange"].astype(str).str.upper() == "HKEX"].copy()
            df["ticker_base"] = df["ticker"].astype(str).str.extract(r"(\d{1,5})")[0]
            df = df.dropna(subset=["ticker_base"])
            df = df.rename(columns={"name":"name"})[["ticker_base","name"]]
            return _write_out(df, tag="fallback")
    except Exception as e:
        print(f"[HKEX:fallback] failed: {e}")
    return _write_out(pd.DataFrame(columns=["ticker_base","name"]), tag="fallback_empty")

def _from_manual() -> pd.DataFrame:
    if MANUAL_FALLBACK.exists():
        try:
            df = pd.read_csv(MANUAL_FALLBACK)
            if {"ticker_base","name"}.issubset(df.columns):
                return _write_out(df[["ticker_base","name"]], tag="manual")
        except Exception as e:
            print(f"[HKEX:manual] failed: {e}")
    return _write_out(pd.DataFrame(columns=["ticker_base","name"]), tag="manual_empty")

# ---------- Entry ----------
def fetch_list() -> pd.DataFrame:
    try:
        if SEHK_BUILDER.exists(): return _from_sehk_builder()
    except Exception as e: print(f"[HKEX:builder] failed: {e}")
    if ENV_URL:
        try:
            s = requests.Session()
            content = _download(s, ENV_URL)
            try:
                df = pd.read_csv(io.BytesIO(content))
                if "Stock Code" in df.columns:
                    df = df.rename(columns={"Stock Code":"ticker_base","Name of Securities":"name"})
                return _write_out(df, tag="env")
            except Exception: pass
            try:
                df = pd.read_excel(io.BytesIO(content))
                if "Stock Code" in df.columns:
                    df = df.rename(columns={"Stock Code":"ticker_base","Name of Securities":"name"})
                return _write_out(df, tag="env_xls")
            except Exception: pass
            html = content.decode("utf-8", errors="ignore")
            tables = pd.read_html(io.StringIO(html))
            if tables:
                frames = []
                for t in tables:
                    cols = [str(c).lower() for c in t.columns]
                    if any(("code" in c) or ("股份代號" in c) for c in cols) and any(("name" in c) or ("名稱" in c) for c in cols):
                        cmap = {}
                        for c in t.columns:
                            cl = str(c).lower()
                            if ("code" in cl) or ("股份代號" in cl): cmap[c] = "ticker_base"
                            elif ("name" in cl) or ("名稱" in cl):   cmap[c] = "name"
                        tt = t.rename(columns=cmap)
                        if {"ticker_base","name"}.issubset(tt.columns):
                            frames.append(tt[["ticker_base","name"]])
                if frames:
                    df = pd.concat(frames, ignore_index=True)
                    return _write_out(df, tag="env_html")
        except Exception as e:
            print(f"[HKEX:env] failed: {e}")
    try:
        return _from_official_xls()
    except Exception as e: print(f"[HKEX:excel] failed: {e}")
    df = _from_hkex_html()
    if not df.empty: return df
    df = _from_consolidated()
    if not df.empty: return df
    return _from_manual()

if __name__ == "__main__":
    fetch_list()
