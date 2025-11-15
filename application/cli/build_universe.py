# application/cli/build_universe.py
from __future__ import annotations
from pathlib import Path
import argparse
import sys

from application.build_universe_service import BuildUniverseService
from infrastructure.repositories.csv_universe_writer_repository import CsvUniverseWriterRepository
from infrastructure.sources.us_sec_source import USSecSource
from infrastructure.sources.jp_jpx_source import JPJpxSource
from infrastructure.sources.hk_hkex_source import HKHKEXSource

def _find_repo_root(start: Path) -> Path:
    """
    Heuristics:
      1) If current or any parent contains "tools/build_universe", that's the repo root.
      2) Else if current or any parent contains both "application" and "infrastructure", use it.
      3) Fallback to two levels up from this file: <root>/application/cli/build_universe.py -> parents[2] == <root>.
    """
    cur = start.resolve()
    for p in [cur, *cur.parents]:
        if (p / "tools" / "build_universe").exists():
            return p
        if (p / "application").exists() and (p / "infrastructure").exists():
            return p
    return Path(__file__).resolve().parents[2]

def main() -> None:
    parser = argparse.ArgumentParser(description="Build Net Net global universe (Clean Architecture).")
    parser.add_argument("--root", type=str, default=None, help="Project root path (directory that contains /tools and /application).")
    args = parser.parse_args()

    if args.root:
        project_root = Path(args.root).resolve()
    else:
        # Try CWD first (common for IDE runs on Windows)
        project_root = _find_repo_root(Path.cwd())

    tools_dir = project_root / "tools" / "build_universe"
    if not tools_dir.exists():
        print(f"❌ Could not locate tools/build_universe under root: {project_root}")
        print("   Tip: run with --root <path-to-your-repo-root>")
        sys.exit(2)

    repo = CsvUniverseWriterRepository(project_root)
    sources = [
        USSecSource(project_root),
        JPJpxSource(project_root),
        HKHKEXSource(project_root),
    ]
    svc = BuildUniverseService(sources=sources, repo=repo)
    result = svc.run()
    print(f"✅ Global universe built: {result['rows']} rows at {project_root / 'data' / 'tickers'}")

if __name__ == "__main__":
    main()
