# application/build_shortlist_service.py
from __future__ import annotations
from dataclasses import asdict
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple
from concurrent.futures import ThreadPoolExecutor, as_completed
import logging
import time

import pandas as pd

from application.ports import (
    ShortlistConfig,
    UniverseRepository,  # or ShortlistUniverseRepository depending on your ports.py
    FundamentalsRepository,
    PriceClient,
    FxProvider,
    ShortlistRepository,
)


def _target_currency(house_ticker: str) -> str:
    if house_ticker.endswith(".US"):
        return "USD"
    if house_ticker.endswith(".HK"):
        return "HKD"
    if house_ticker.endswith(".JP"):
        return "JPY"
    return "USD"


def _convert_ncavps(fin_ccy: str, target_ccy: str, ncav_ps: Optional[float], usd_per: Dict[str, float]) -> Optional[float]:
    if ncav_ps is None:
        return None
    fin = (fin_ccy or "USD").upper()
    tgt = (target_ccy or "USD").upper()
    if fin == tgt:
        return float(ncav_ps)
    uf = usd_per.get(fin)
    ut = usd_per.get(tgt)
    if uf is None or ut is None or uf == 0:
        return None
    return float(ncav_ps) * (uf / ut)


class BuildShortlistService:
    def __init__(
        self,
        universe_repo: UniverseRepository,          # or ShortlistUniverseRepository
        fundamentals_repo: FundamentalsRepository,
        price_client: PriceClient,
        fx_provider: FxProvider,
        out_repo: ShortlistRepository,
        logger: Optional[logging.Logger] = None,
        log_every: int = 50,
    ) -> None:
        self.universe_repo = universe_repo
        self.fundamentals_repo = fundamentals_repo
        self.price_client = price_client
        self.fx_provider = fx_provider
        self.out_repo = out_repo
        self.log = logger or logging.getLogger("shortlist")
        self.log_every = max(1, int(log_every))

    def run(self, cfg: ShortlistConfig) -> dict:
        t0 = time.time()
        rows: List[dict] = []

        # 1) Load universe
        urows = self.universe_repo.load_tickers()
        tickers = [str(r["ticker"]).strip() for r in urows if str(r.get("ticker", ""))]
        if cfg.limit:
            tickers = tickers[: cfg.limit]
        self.log.info("Universe loaded: %d tickers (limit=%s)", len(tickers), cfg.limit or "None")

        # 2) Fundamentals (concurrent with bounded threads)
        self.log.info(
            "Fetching fundamentals (prices_only=%s, timeout=%ss, workers=%d)...",
            cfg.prices_only,
            cfg.fetch_timeout,
            max(1, int(cfg.max_workers or 1)),
        )

        def _one(h: str) -> dict:
            if cfg.prices_only:
                rec = self.fundamentals_repo.get_cached(h)
                if rec is None:
                    return {
                        "ticker": h,
                        "y_symbol": None,
                        "fs_date": None,
                        "currency": "",
                        "assets_current": None,
                        "liab_total": None,
                        "ncav": None,
                        "shares_out": None,
                        "ncav_ps": None,
                        "data_age_days": None,
                        "fs_source": None,
                        "fs_selected_col": None,
                        "note": "no cache",
                    }
                return rec
            else:
                return self.fundamentals_repo.get_or_update(h, cfg.fetch_timeout)

        done = 0
        total = len(tickers)
        workers = max(1, int(cfg.max_workers or 1))
        # Collect results in original order for deterministic output
        order_map: Dict[str, int] = {t: i for i, t in enumerate(tickers)}
        results: List[Tuple[int, dict]] = []

        if total > 0:
            with ThreadPoolExecutor(max_workers=workers) as ex:
                futs = {ex.submit(_one, h): h for h in tickers}
                for f in as_completed(futs):
                    h = futs[f]
                    try:
                        rec = f.result()
                    except Exception as e:
                        rec = {
                            "ticker": h,
                            "y_symbol": None,
                            "fs_date": None,
                            "currency": "",
                            "assets_current": None,
                            "liab_total": None,
                            "ncav": None,
                            "shares_out": None,
                            "ncav_ps": None,
                            "data_age_days": None,
                            "fs_source": None,
                            "fs_selected_col": None,
                            "note": f"error: {e}",
                        }
                    results.append((order_map[h], rec))
                    done += 1
                    if (done % self.log_every) == 0 or done == total:
                        self.log.info("Fundamentals progress: %d / %d (%.1f%%)", done, total, 100.0 * done / max(1, total))

        results.sort(key=lambda x: x[0])
        rows = [rec for _, rec in results]
        base = pd.DataFrame(rows)
        self.log.info("Fundamentals collected: %d rows", len(base))

        # 3) FX normalization (FS -> trading ccy)
        base["target_ccy"] = base["ticker"].map(_target_currency)
        base["currency"] = base.get("currency", pd.Series([None] * len(base)))
        base["currency"] = base["currency"].astype(object)
        na_mask = base["currency"].isna()
        base.loc[na_mask, "currency"] = base.loc[na_mask, "target_ccy"]
        empty_mask = base["currency"].astype(str).str.strip() == ""
        base.loc[empty_mask, "currency"] = base.loc[empty_mask, "target_ccy"]

        need_ccy = sorted(
            set(
                base["currency"].dropna().astype(str).str.upper().tolist()
                + base["target_ccy"].dropna().astype(str).str.upper().tolist()
            )
        )
        self.log.info("Fetching FX rates for %d currencies...", len(need_ccy))
        usd_per = self.fx_provider.usd_per_ccy(need_ccy)
        self.log.debug("FX map (USD per CCY): %s", usd_per)

        base["ncav_ps_target"] = base.apply(
            lambda r: _convert_ncavps(r["currency"], r["target_ccy"], r["ncav_ps"], usd_per), axis=1
        )

        # 4) Gate: FS recency + NCAV positive
        base["within_2y"] = base["fs_date"].apply(
            lambda d: (pd.notna(d) and (pd.Timestamp.now() - pd.to_datetime(d)).days <= cfg.max_fs_age_days)
        )
        base["ncav_positive"] = base["ncav"].apply(lambda x: (x is not None) and (pd.notna(x)) and (float(x) > 0))
        base["ncavps_pos_target"] = base["ncav_ps_target"].apply(
            lambda x: (x is not None) and (pd.notna(x)) and (float(x) > 0)
        )

        self.log.info(
            "Gate counts → within_2y: %d, ncav_positive: %d, ncavps_pos_target: %d",
            int(base["within_2y"].sum()),
            int(base["ncav_positive"].sum()),
            int(base["ncavps_pos_target"].sum()),
        )

        # 5) Prices only for viable
        need_price_mask = base["within_2y"] & base["ncav_positive"] & base["ncavps_pos_target"]
        price_symbols = sorted(set(base.loc[need_price_mask, "y_symbol"].dropna().astype(str)))
        self.log.info("Fetching prices for %d Yahoo symbols (batch=%d)...", len(price_symbols), cfg.prices_batch)

        # chunked logging around price client
        def _chunks(seq, n):
            for i in range(0, len(seq), n):
                yield i // n + 1, seq[i : i + n]

        px_map: Dict[str, Tuple[Optional[float], Optional[str]]] = {}
        for b_idx, chunk in _chunks(price_symbols, cfg.prices_batch):
            self.log.info("Price batch %d: %d symbols", b_idx, len(chunk))
            res = self.price_client.latest_closes(chunk, batch_size=cfg.prices_batch)
            px_map.update(res)

        base["price"] = base["y_symbol"].map(lambda y: px_map.get(y, (None, None))[0])
        base["price_date"] = base["y_symbol"].map(lambda y: px_map.get(y, (None, None))[1])

        # 6) Ratio + pass flag
        def _ratio(price, ncavps_tgt):
            try:
                price = float(price)
                ncavps_tgt = float(ncavps_tgt)
                if ncavps_tgt == 0:
                    return None
                return price / ncavps_tgt
            except Exception:
                return None

        base["price_vs_ncavps"] = base.apply(lambda r: _ratio(r["price"], r["ncav_ps_target"]), axis=1)
        base["is_ncav_netnet"] = base.apply(
            lambda r: (
                r["within_2y"]
                and r["ncav_positive"]
                and r["ncavps_pos_target"]
                and (r["price_vs_ncavps"] is not None)
                and (r["price_vs_ncavps"] < 1.0)
            ),
            axis=1,
        )

        shortlist_count = int((base["is_ncav_netnet"] == True).sum())
        self.log.info("Shortlist count: %d", shortlist_count)

        # 7) Save & meta
        out_all = self.out_repo.save_all(base)
        shortlist_df = base[base["is_ncav_netnet"] == True].copy().reset_index(drop=True)
        out_short = self.out_repo.save_shortlist(shortlist_df)

        meta = {
            "generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
            "params": asdict(cfg),
            "counts": {
                "universe": int(len(tickers)),
                "eligible_for_price": int(len(price_symbols)),
                "with_ncavps_target": int(base["ncav_ps_target"].notna().sum()),
                "within_2y": int(base["within_2y"].sum()),
                "ncav_positive": int(base["ncav_positive"].sum()),
                "shortlist": shortlist_count,
            },
            "fx": {"usd_per": usd_per},
            "outputs": {
                "ncav_all_csv": str(out_all),
                "ncav_shortlist_csv": str(out_short),
            },
            "elapsed_sec": round(time.time() - t0, 2),
        }
        self.out_repo.save_meta(meta)
        self.log.info("Saved: all=%s, shortlist=%s", out_all, out_short)
        self.log.info("Done in %.2fs", meta["elapsed_sec"])
        return meta
