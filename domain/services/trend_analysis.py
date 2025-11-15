#domain/services/trend_analysis.py --> dilution check and pairing for ncav trend
from dataclasses import dataclass
from typing import Optional, Dict, Any, Tuple, List
from domain.services.periods import _extract_period_date
from domain.services.balance_sheet_metrics import safe_float

def pct_change(old: Optional[float], new: Optional[float]) -> Optional[float]:
    """
    Return (new - old) / abs(old) as a fraction.
    Positive => grew, Negative => shrank/burned.
    """
    if old is None or new is None:
        return None
    if old == 0:
        return None
    return (float(new) - float(old)) / abs(float(old))

def _pick_pair_by_gap(
    periods: List[Dict[str, Any]],
    approx_days: int,
    tolerance_days: int,
) -> Optional[Tuple[Dict[str, Any], Dict[str, Any]]]:
    """
    Given sorted periods [newest,...], try to find two periods whose dates are
    about `approx_days` apart (± tolerance_days).
    Return (newer, older).
    """
    if len(periods) < 2:
        return None

    # Precompute dates
    dated = [(p, _extract_period_date(p)) for p in periods]
    dated = [(p, d) for (p, d) in dated if d is not None]
    if len(dated) < 2:
        return None

    newer, newer_dt = dated[0]
    # search older that matches gap
    for older, older_dt in dated[1:]:
        delta_days = (newer_dt - older_dt).days
        if abs(delta_days - approx_days) <= tolerance_days:
            return (newer, older)

    # fallback: just take first two if we didn't find a "nice" gap
    if len(dated) >= 2:
        return (dated[0][0], dated[1][0])

    return None

def pair_for_qoq(periods: List[Dict[str, Any]]) -> Optional[Tuple[Dict[str, Any], Dict[str, Any]]]:
    # ~90 days gap +/- 45
    return _pick_pair_by_gap(periods, approx_days=90, tolerance_days=45)

def pair_for_hoh(periods: List[Dict[str, Any]]) -> Optional[Tuple[Dict[str, Any], Dict[str, Any]]]:
    # ~180 days gap +/- 60
    return _pick_pair_by_gap(periods, approx_days=180, tolerance_days=60)

def pair_for_yoy(periods: List[Dict[str, Any]]) -> Optional[Tuple[Dict[str, Any], Dict[str, Any]]]:
    # ~365 days gap +/- 90
    return _pick_pair_by_gap(periods, approx_days=365, tolerance_days=90)

def _shares_out(period: Dict[str, Any]) -> Optional[float]:
    """
    Get shares_out from period snapshot.
    We assume something like period["shares_out"] or inside balance/meta.
    """
    if period is None:
        return None
    # common places
    for k in ["shares_out", "shares_outstanding", "basic_shares_out"]:
        v = period.get(k)
        if v is not None:
            return safe_float(v)

    # sometimes inside "meta" of the period
    meta = period.get("meta") or {}
    for k in ["shares_out", "shares_outstanding", "basic_shares_out"]:
        v = meta.get(k)
        if v is not None:
            return safe_float(v)

    return None

def _pct_change_shares(newer: Dict[str, Any], older: Dict[str, Any]) -> Optional[float]:
    """
    (shares_new - shares_old) / shares_old
    Positive => issued stock (dilution).
    Negative => bought back.
    """
    sh_new = _shares_out(newer)
    sh_old = _shares_out(older)
    return pct_change(sh_old, sh_new)

@dataclass
class DilutionWindowStats:
    max_issue: Optional[float]      # most positive % change (worst dilution)
    max_buyback: Optional[float]    # most negative % change (best buyback)

def _max_change_within_days(
    periods: List[Dict[str, Any]],
    window_days: int,
) -> DilutionWindowStats:
    """
    Look at every pair of periods within `window_days` between them
    and record the worst issuance (max positive pct) and best buyback
    (most negative pct).
    """
    dated = [(p, _extract_period_date(p)) for p in periods]
    dated = [(p, d) for (p, d) in dated if d is not None]

    max_issue: Optional[float] = None
    max_buyback: Optional[float] = None

    for i in range(len(dated)):
        p_new, d_new = dated[i]
        for j in range(i + 1, len(dated)):
            p_old, d_old = dated[j]
            if d_new is None or d_old is None:
                continue
            gap = (d_new - d_old).days
            if gap < 0:
                continue
            if gap > window_days:
                continue
            chg = _pct_change_shares(p_new, p_old)
            if chg is None:
                continue
            if max_issue is None or chg > max_issue:
                max_issue = chg
            if max_buyback is None or chg < max_buyback:
                max_buyback = chg

    return DilutionWindowStats(max_issue=max_issue, max_buyback=max_buyback)


def max_dilution_within_1y(periods: List[Dict[str, Any]]) -> Optional[float]:
    """
    Return worst (max positive) dilution within ~365 days.
    """
    stats = _max_change_within_days(periods, window_days=365)
    return stats.max_issue


def max_change_within_3y(periods: List[Dict[str, Any]]) -> DilutionWindowStats:
    """
    Return both worst issuance and best buyback over ~3 years (~1095 days).
    """
    return _max_change_within_days(periods, window_days=1095)

