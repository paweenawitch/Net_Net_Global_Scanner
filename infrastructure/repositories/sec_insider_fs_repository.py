#infrastructure/repositories/sec_insider_fs_repository.py
from __future__ import annotations
import json
from pathlib import Path
from typing import Dict, Any, Optional

from application.screening_service import InsiderRepository


class SecInsiderFsRepository(InsiderRepository):
    def __init__(self, insider_dir: Path) -> None:
        self._insider_dir = insider_dir

    def _read_json(self, path: Path) -> Optional[Dict[str, Any]]:
        try:
            return json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            return None

    def load_insiders(self, ticker: str) -> Optional[Dict[str, Any]]:
        p = self._insider_dir / f"{ticker}.json"
        if not p.exists():
            return None
        return self._read_json(p)
