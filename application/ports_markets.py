#application/ports_markets.py
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Sequence

@dataclass
class MarketJob:
    name: str                     # e.g., "US_CORE", "NON_US", "US_INSIDERS", "JP_EDINET"
    script_rel: Path              # relative path under tools/
    args_builder: Callable[[Path], Sequence[str]]  # gets shortlist path -> argv list
    include_filter: Callable[[str], bool] | None = None  # optional: restrict by ticker suffix
