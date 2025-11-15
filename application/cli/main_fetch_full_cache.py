# application/cli/main_fetch_full_cache.py

import argparse
from pathlib import Path

from application.fetch_cache_orchestrator import FetchCacheOrchestrator, FetchConfig


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--shortlist", help="Override shortlist CSV path (relative to repo root)")
    # backwards-compatible convenience flags
    ap.add_argument("--force-us-core", action="store_true", help="Pass --force to US_CORE job (if supported)")
    ap.add_argument("--us-only", action="store_true", help="Run only US jobs (US_CORE [+ US_INSIDERS])")
    ap.add_argument("--nonus-only", action="store_true", help="Run only NON_US job")
    ap.add_argument("--skip-insiders", action="store_true", help="Skip US_INSIDERS job")

    # generic filters (for power users / future markets)
    ap.add_argument("--only", nargs="*", help="Explicit list of job names to run (e.g. US_CORE NON_US)")
    ap.add_argument("--skip", nargs="*", help="Explicit list of job names to skip (e.g. US_INSIDERS)")

    ap.add_argument("--verbose", action="store_true")
    args = ap.parse_args()

    cfg = FetchConfig()
    if args.shortlist:
        # allow both absolute and relative paths; relative is from repo_root
        custom = Path(args.shortlist)
        cfg.shortlist_csv = custom if custom.is_absolute() else (cfg.repo_root / custom)

    orch = FetchCacheOrchestrator(cfg)

    # --- build ONLY / SKIP sets based on flags ---

    only: set[str] = set(args.only or [])
    skip: set[str] = set(args.skip or [])

    # convenience flags mapped to registry job names
    # NOTE: these assume your registry uses "US_CORE", "NON_US", "US_INSIDERS"
    if args.us_only and args.nonus_only:
        raise SystemExit("--us-only and --nonus-only are mutually exclusive")

    if args.us_only:
        # run both US core & insiders unless user explicitly skips insiders
        only.update({"US_CORE", "US_INSIDERS"})
    elif args.nonus_only:
        only.add("NON_US")

    if args.skip_insiders:
        skip.add("US_INSIDERS")

    # if "only" stays empty, orchestrator will just run all jobs
    only_arg = only or None
    skip_arg = skip or None

    # --- extra args per job (force-refresh, etc.) ---

    extra_args: dict[str, list[str]] = {}

    if args.force_us_core:
        extra_args.setdefault("US_CORE", []).append("--force")

    extra_args_arg = extra_args or None

    # --- run orchestration ---

    orch.run_all(
        verbose=args.verbose,
        only=only_arg,
        skip=skip_arg,
        extra_args=extra_args_arg,
    )


if __name__ == "__main__":
    main()
