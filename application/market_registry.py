#application/market_registry.py
from pathlib import Path
from application.ports_markets import MarketJob

def only_us(t: str) -> bool: return t.upper().endswith(".US")
def non_us(t: str) -> bool:  return not t.upper().endswith(".US")

def default_registry(tools_root: Path) -> list[MarketJob]:
    return [
        MarketJob(
            name="US_CORE",
            script_rel=tools_root / "sec_extract_core.py",
            args_builder=lambda shortlist: ["--skip-days","7","--sleep","0.35"],
            include_filter=only_us
        ),
        MarketJob(
            name="NON_US",
            script_rel=tools_root / "non_us_fetch_companyfact.py",
            args_builder=lambda shortlist: ["--shortlist", str(shortlist), "--sleep","0.35"],
            include_filter=non_us  # script already skips .US internally, this is just symmetry
        ),
        MarketJob(
            name="US_INSIDERS",
            script_rel=tools_root / "sec_insider_scan.py",
            args_builder=lambda shortlist: ["--days-back","180","--universe", str(shortlist)],
            include_filter=only_us
        ),
    ]
