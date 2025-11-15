# domain/models/company.py
from dataclasses import dataclass
from typing import Optional


@dataclass(frozen=True)
class CompanyId:
    ticker: str            # "OP.US"
    cik: Optional[str]     # "0001869467" for US, None for JP/HK
    country_iso: str       # "US", "JP", "HK", ...


@dataclass(frozen=True)
class CompanyProfile:
    name: str                          # "OceanPal Inc."
    exchange: Optional[str]            # e.g. "NASDAQ", "TSE", "HKEX" (None in sample)
    sector: Optional[str]              # "General Industry"
    industry: Optional[str]            # "Deep Sea Foreign Transportation of Freight"
    sic: Optional[str]                 # "4412" (US only usually)
    entity_type: Optional[str]         # "other", "operating", "adr", etc.
    website: Optional[str]
    ipo_date: Optional[str]            # ISO "YYYY-MM-DD" if known
    fye_month: Optional[int]           # 12 -> December fiscal year end
    employees: Optional[int]           # headcount if we have it


@dataclass(frozen=True)
class Company:
    id: CompanyId
    profile: CompanyProfile
