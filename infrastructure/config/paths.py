#infrastructure/config/paths.py
from dataclasses import dataclass
from pathlib import Path

@dataclass
class RepoPaths:
    root: Path
    tools: Path
    cache_core: Path
    cache_insider: Path
    shortlist: Path

    @classmethod
    def from_root(cls, root: Path) -> "RepoPaths":
        return cls(
            root=root,
            tools=root / "tools",
            cache_core=root / "cache" / "sec_core",
            cache_insider=root / "cache" / "sec_insider",
            shortlist=root / "data" / "tickers" / "ncav_shortlist.csv",
        )
