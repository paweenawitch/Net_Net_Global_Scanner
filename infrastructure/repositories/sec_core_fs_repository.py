# infrastructure/repositories/sec_core_fs_repository.py
from __future__ import annotations
import json
from pathlib import Path
from typing import Dict, Any, Optional
from application.screening_service import CoreRepository

class SecCoreFsRepository(CoreRepository):
    def __init__(self, core_dir: Path) -> None:
        self._core_dir = core_dir

    def _read_json(self, path: Path) -> Optional[Dict[str, Any]]:
        try:
            return json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            return None

    def load_core(self, ticker: str) -> Optional[Dict[str, Any]]:
        for name in (f"{ticker}_core.json", f"{ticker}.json"):
            p = self._core_dir / name
            if p.exists():
                data = self._read_json(p)
                if data:
                    return data
        return None
