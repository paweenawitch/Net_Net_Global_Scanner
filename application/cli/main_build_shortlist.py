## application/cli/main_build_shortlist.py
from __future__ import annotations
from pathlib import Path
import argparse
import logging

from application.ports import ShortlistConfig
from application.build_shortlist_service import BuildShortlistService
from infrastructure.repositories.csv_universe_loader_repository import CsvUniverseLoaderRepository
from infrastructure.repositories.ncav_cache_repository import NcavCacheRepository
from infrastructure.sources.yahoo_price_client import YahooPriceClient
from infrastructure.sources.yahoo_fx_provider import YahooFxProvider
from infrastructure.repositories.local_shortlist_repository import LocalShortlistRepository

# Default to your tools cache root for outputs
try:
    from tools.ncav_cache import ROOT as CACHE_ROOT  # type: ignore
except Exception:
    CACHE_ROOT = Path.cwd()

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--tickers_csv", type=str, default=str(CACHE_ROOT / "data" / "tickers" / "global_full.csv"))
    ap.add_argument("--max-workers", type=int, default=3)
    ap.add_argument("--fetch-timeout", type=int, default=12)
    ap.add_argument("--prices-batch", type=int, default=40)
    ap.add_argument("--limit", type=int)
    ap.add_argument("--prices-only", action="store_true")
    ap.add_argument("--max-fs-age-days", type=int, default=730)
    ap.add_argument("--verbose", "-v", action="count", default=1,
                    help="-v INFO, -vv DEBUG, -vvv TRACE-like (DEBUG+extra)")
    ap.add_argument("--log-every", type=int, default=10, help="log fundamentals progress every N tickers")
    args = ap.parse_args()

    # Logging setup
    level = logging.WARNING
    if args.verbose == 1: level = logging.INFO
    elif args.verbose >= 2: level = logging.DEBUG
    logging.basicConfig(level=level, format="%(asctime)s | %(levelname)s | %(name)s | %(message)s")

    logger = logging.getLogger("shortlist")
    logger.info("Starting shortlist builder")

    universe = CsvUniverseLoaderRepository(Path(args.tickers_csv))
    fundamentals = NcavCacheRepository()
    prices = YahooPriceClient()
    fx = YahooFxProvider()
    out = LocalShortlistRepository(CACHE_ROOT)

    svc = BuildShortlistService(universe, fundamentals, prices, fx, out, logger=logger, log_every=args.log_every)
    cfg = ShortlistConfig(
        max_workers=args.max_workers,
        fetch_timeout=args.fetch_timeout,
        prices_batch=args.prices_batch,
        max_fs_age_days=args.max_fs_age_days,
        prices_only=args.prices_only,
        limit=args.limit,
    )
    meta = svc.run(cfg)
    logger.info("Shortlist done → %s", meta["outputs"]["ncav_shortlist_csv"])
    print("Shortlist done →", meta["outputs"]["ncav_shortlist_csv"])

if __name__ == "__main__":
    main()
