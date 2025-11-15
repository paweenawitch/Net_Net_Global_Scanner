# infrastructure/sources/us_sec_source.py
from __future__ import annotations
from pathlib import Path
import importlib.util
import pandas as pd

from application.ports import TickerSource
from infrastructure.config.paths import RepoPaths

class USSecSource(TickerSource):
    market_code = "US"
    source_label = "sec_company_tickers.json"

    def __init__(self, project_root: Path) -> None:
        self.paths = RepoPaths.from_root(Path(project_root))

    def _import_tool(self):
        tool_path = self.paths.tools / "build_universe" / "us_sec.py"
        spec = importlib.util.spec_from_file_location("us_sec_tool", tool_path)
        if spec is None or spec.loader is None:
            raise FileNotFoundError(f"Missing {tool_path}")
        mod = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(mod)  # type: ignore[attr-defined]
        return mod

    def fetch(self) -> pd.DataFrame:
        mod = self._import_tool()
        return mod.fetch_list()
