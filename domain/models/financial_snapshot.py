# domain/models/financial_snapshot.py
from dataclasses import dataclass
from typing import Optional
from datetime import date


@dataclass(frozen=True)
class FinancialSnapshot:
    ticker: str

    # point in time
    period_date: date         # e.g. 2024-12-31
    currency: str             # e.g. "USD", "JPY", "HKD", "CNY"

    # key balance sheet items (raw)
    assets_current: Optional[float]    # us-gaap:AssetsCurrent
    liab_total: Optional[float]        # total liabilities; if we only have current, we can approximate later
    liab_current: Optional[float]      # us-gaap:LiabilitiesCurrent
    assets_total: Optional[float]      # us-gaap:Assets
    cash: Optional[float]              # CashAndCashEquivalentsAtCarryingValue
    receivables: Optional[float]       # AccountsReceivableNetCurrent
    inventory: Optional[float]         # InventoryNet
    equity: Optional[float]            # StockholdersEquity

    shares_out: Optional[float]        # CommonStockSharesOutstanding (point-in-time, not weighted avg)

    # income statement (can be useful for “melting ice cube / zombie shipping co?”)
    oper_income: Optional[float]       # OperatingIncomeLoss
    net_income: Optional[float]        # NetIncomeLoss

    # cash flow statement
    cfo: Optional[float]               # NetCashProvidedByUsedInOperatingActivities
    capex: Optional[float]             # PaymentsToAcquirePropertyPlantAndEquipment
