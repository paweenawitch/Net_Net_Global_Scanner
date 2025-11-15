#tools/screening_engine.py
from __future__ import annotations
from pathlib import Path
import argparse

from application.screening_service import ScreeningService
from infrastructure.repositories.csv_shortlist_reader_repository import CsvShortlistReaderRepository
from infrastructure.repositories.sec_core_fs_repository import SecCoreFsRepository
from infrastructure.repositories.sec_insider_fs_repository import SecInsiderFsRepository
from infrastructure.fx.exchangerate_host_provider import ExchangerateHostFxProvider
from infrastructure.reporting.valuation_report_writer import CsvJsonValuationWriter



def main() -> None:
    here = Path(__file__).resolve().parent

    default_shortlist = here.parent / "data" / "tickers" / "ncav_shortlist.csv"

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--shortlist",
        type=str,
        default=str(default_shortlist),
        help="Path to ncav_shortlist.csv",
    )
    args = parser.parse_args()

    shortlist_path = Path(args.shortlist)
    if not shortlist_path.exists():
        raise FileNotFoundError(f"❌ ncav_shortlist.csv not found at: {shortlist_path}")

    # Directories (same layout as old script, but now wired via infra)
    core_dir = (here.parent / "cache" / "sec_core")
    insider_dir = (here.parent / "cache" / "sec_insider")
    fx_cache_file = (here.parent / "cache" / "fx" / "latest.json")
    public_dir = (here.parent / "public" / "reports")
    internal_dir = (here.parent / "reports" / "_internal")

    # Instantiate infrastructure
    shortlist_repo = CsvShortlistReaderRepository()
    core_repo = SecCoreFsRepository(core_dir=core_dir)
    insider_repo = SecInsiderFsRepository(insider_dir=insider_dir)
    fx_provider = ExchangerateHostFxProvider(cache_file=fx_cache_file)
    writer = CsvJsonValuationWriter(public_dir=public_dir, internal_dir=internal_dir)

    # Application service
    service = ScreeningService(
        shortlist_repo=shortlist_repo,
        core_repo=core_repo,
        insider_repo=insider_repo,
        fx_provider=fx_provider,
        writer=writer,
    )

    summary = service.screen_shortlist(shortlist_path)
    paths = summary.output_paths

    print(f"✅ Flags saved: {paths.get('csv')}, {paths.get('json')}")
    print(f"🔒 Debug: {paths.get('debug')} and {paths.get('latest_debug')}")
    print(f"📊 Tickers screened: {summary.count}")


if __name__ == "__main__":
    main()
