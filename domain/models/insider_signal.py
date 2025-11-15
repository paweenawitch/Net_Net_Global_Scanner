# domain/models/insider_signal.py
from dataclasses import dataclass
from typing import Literal, Optional


# These string values are aligned with what the current screening logic emits.
SignalType = Literal[
    "Buy",        # clear insider buying
    "Net Buy",    # more buys than sells in aggregate
    "Sell",       # clear selling
    "Net Sell",   # more sells than buys
    "None",       # no insider activity detected / no data
    "Unknown"     # data present but couldn't classify
]


@dataclass
class InsiderSignal:
    ticker: str
    signal: SignalType

    # raw counts/sums that led to the judgment
    total_buy_trades: Optional[int]
    total_sell_trades: Optional[int]
    net_shares_change: Optional[float]   # + means insiders accumulated, - means dumped

    # optional metadata if you have it in core data:
    last_activity_date: Optional[str]    # "2025-09-30", etc.
    source: Optional[str]                # e.g. "EDGAR", "HKEX filing", "TSE", etc.
