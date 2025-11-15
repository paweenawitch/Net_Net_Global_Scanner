# application/screening_service.py

from __future__ import annotations
from dataclasses import dataclass
from pathlib import Path
from typing import Protocol, List, Dict, Any, Optional

from domain.models.valuation_result import ValuationResult
from domain.services.netnet_analysis import analyze_one_ticker


@dataclass
class ShortlistItem:
    ticker: str
    last_price: float


class ShortlistRepository(Protocol):
    def load_shortlist(self, path: Path) -> List[ShortlistItem]:
        ...


class CoreRepository(Protocol):
    def load_core(self, ticker: str) -> Optional[Dict[str, Any]]:
        ...


class InsiderRepository(Protocol):
    def load_insiders(self, ticker: str) -> Optional[Dict[str, Any]]:
        ...


class FxProvider(Protocol):
    def get_rates_ccy_to_usd(self) -> Dict[str, float]:
        ...


class ValuationWriter(Protocol):
    def write(
        self,
        valuations: List[ValuationResult],
        fx_rates_ccy_to_usd: Dict[str, float],
    ) -> Dict[str, str]:
        ...


@dataclass
class ScreeningSummary:
    count: int
    output_paths: Dict[str, str]


class ScreeningService:
    def __init__(
        self,
        shortlist_repo: ShortlistRepository,
        core_repo: CoreRepository,
        insider_repo: InsiderRepository,
        fx_provider: FxProvider,
        writer: ValuationWriter,
    ) -> None:
        self._shortlist_repo = shortlist_repo
        self._core_repo = core_repo
        self._insider_repo = insider_repo
        self._fx_provider = fx_provider
        self._writer = writer

    def screen_shortlist(self, shortlist_path: Path) -> ScreeningSummary:
        items = self._shortlist_repo.load_shortlist(shortlist_path)
        fx_rates = self._fx_provider.get_rates_ccy_to_usd()

        results: List[ValuationResult] = []
        for item in items:
            core = self._core_repo.load_core(item.ticker)
            if not core:
                continue

            insider_blob = self._insider_repo.load_insiders(item.ticker) or {}
            valuation = analyze_one_ticker(
                core=core,
                insider_blob=insider_blob,
                last_price=item.last_price,
                fx_rates=fx_rates,
            )
            results.append(valuation)

        paths = self._writer.write(results, fx_rates_ccy_to_usd=fx_rates)
        return ScreeningSummary(count=len(results), output_paths=paths)
