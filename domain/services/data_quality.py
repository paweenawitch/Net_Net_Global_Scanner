#domain/services/data_quality.py
from typing import Optional, Dict, Any, Tuple
from datetime import datetime
from domain.services.periods import _extract_period_date

def assess_staleness(
    latest_period: Optional[Dict[str, Any]],
    now_dt: Optional[datetime] = None,
    stale_after_days: int = 540,
) -> Tuple[bool, Optional[int]]:
    """
    Decide if latest financial data is 'outdated'.

    stale_after_days default ~18 months (540 days).
    Returns (is_outdated, age_days).
    """
    if now_dt is None:
        now_dt = datetime.utcnow()

    if latest_period is None:
        return True, None

    d = _extract_period_date(latest_period)
    if d is None:
        return True, None

    age_days = (now_dt - d).days
    is_stale = age_days > stale_after_days
    return is_stale, age_days
