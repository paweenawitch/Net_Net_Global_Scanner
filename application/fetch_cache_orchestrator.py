# application/fetch_cache_orchestrator.py

from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Optional, Dict, List

from infrastructure.runners.python_script_runner import PythonScriptRunner
from application.market_registry import default_registry


@dataclass
class FetchConfig:
    """
    Configuration for the cache-fetch step.

    - repo_root: project root
    - shortlist_csv: where the shortlist is saved
    - tools_dir: folder that contains the fetch scripts (SEC, non-US, etc.)
    """
    repo_root: Path = Path(__file__).resolve().parents[1]
    shortlist_csv: Path = Path(__file__).resolve().parents[1] / "data" / "tickers" / "ncav_shortlist.csv"
    tools_dir: Path = Path("tools")


class FetchCacheOrchestrator:
    """
    Orchestrates running all registered market jobs against the shortlist.

    New markets are added by registering a MarketJob in application/market_registry.py.
    This file does NOT need to change when someone adds JP, UK, etc.
    """

    def __init__(self, cfg: Optional[FetchConfig] = None) -> None:
        self.cfg = cfg or FetchConfig()
        # runs scripts relative to repo_root
        self.runner = PythonScriptRunner(self.cfg.repo_root)
        # registry builds the list of MarketJob objects (US_CORE, NON_US, US_INSIDERS, ...)
        self.jobs = default_registry(self.cfg.tools_dir)

    def run_all(
        self,
        verbose: bool = False,
        only: Iterable[str] | None = None,
        skip: Iterable[str] | None = None,
        extra_args: Optional[Dict[str, List[str]]] = None,
    ) -> None:
        """
        Run all (or a filtered subset of) market jobs.

        Args:
            verbose: if True, append --verbose to each job's args (if not already present).
            only: optional iterable of job names to run (e.g. ["US_CORE","NON_US"]).
            skip: optional iterable of job names to skip.
            extra_args: optional mapping of JOB_NAME -> list of extra CLI args
                        (e.g. {"US_CORE": ["--force"]}).
        """
        only_set = {n.upper() for n in only} if only else None
        skip_set = {n.upper() for n in skip} if skip else set()
        extra_args = extra_args or {}

        for job in self.jobs:
            name = job.name
            name_upper = name.upper()

            # filter by "only" / "skip" if provided
            if only_set is not None and name_upper not in only_set:
                continue
            if name_upper in skip_set:
                continue

            # build script arguments from the registry entry
            args = list(job.args_builder(self.cfg.shortlist_csv))

            # attach any extra args for this job (e.g. --force)
            extra = extra_args.get(name_upper)
            if extra:
                args.extend(extra)

            # propagate verbosity flag if requested and not already present
            if verbose and "--verbose" not in args:
                args.append("--verbose")

            rc = self.runner.run(job.script_rel, args)
            if rc != 0:
                raise SystemExit(f"{name} failed (rc={rc})")
