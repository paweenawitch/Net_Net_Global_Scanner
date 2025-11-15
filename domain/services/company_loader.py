# domain/services/company_loader.py
import pandas as pd
from domain.models.company import CompanyId, CompanyProfile, Company

def load_companies(path: str) -> list[Company]:
    df = pd.read_csv(path)
    companies = []
    for _, r in df.iterrows():
        cid = CompanyId(
            ticker=r["ticker"],
            cik=None if r["country"] != "US" else str(r.get("cik", "")),
            country_iso=r["country"],
        )
        profile = CompanyProfile(name=r["name"], exchange=None,
                                 sector=None, industry=None,
                                 sic=None, entity_type=None,
                                 website=None, ipo_date=None,
                                 fye_month=None, employees=None)
        companies.append(Company(id=cid, profile=profile))
    return companies
