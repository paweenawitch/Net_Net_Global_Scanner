# domain/services/periods.py
from __future__ import annotations
from typing import Any, Optional, Dict, List, Tuple
from datetime import datetime


def parse_date(d: Any) -> Optional[datetime]:
    """
    Best-effort turn "2024-12-31", 2024-12-31T00:00:00Z, etc. into datetime.
    Returns None if we can't parse.
    """
    if d is None:
        return None
    if isinstance(d, datetime):
        return d

    candidates = [
        "%Y-%m-%d",
        "%Y/%m/%d",
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%dT%H:%M:%SZ",
        "%Y-%m-%d %H:%M:%S",
    ]
    ds = str(d).strip()
    for fmt in candidates:
        try:
            return datetime.strptime(ds, fmt)
        except ValueError:
            continue
    return None


def _extract_period_date(period: Dict[str, Any]) -> Optional[datetime]:
    """
    Extracts the accounting period end date. We assume keys like:
    'statement_date', 'period_end', etc.
    """
    for k in ["statement_date", "period_end", "date", "as_of_date", "fs_date"]:
        d = parse_date(period.get(k))
        if d:
            return d
    return None


def _sort_periods_desc(periods: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Sort snapshots newest-first by their statement date.
    """
    with_dates: List[Tuple[datetime, Dict[str, Any]]] = []
    for p in periods:
        dt = _extract_period_date(p)
        if dt is not None:
            with_dates.append((dt, p))

    # sort newest-first
    with_dates.sort(key=lambda tup: tup[0], reverse=True)
    return [p for _, p in with_dates]


def _get_period_list(core: Dict[str, Any], bucket: str) -> List[Dict[str, Any]]:
    """
    Support both:
        core["financials"][bucket]["periods"]
    and legacy:
        core[bucket] = [period, ...]
    """
    fin = core.get("financials") or {}
    node = fin.get(bucket)

    periods: Any = []
    if isinstance(node, dict):
        periods = node.get("periods") or []
    elif isinstance(node, list):
        periods = node
    else:
        # fallback to old shape
        periods = core.get(bucket) or []

    if not isinstance(periods, list):
        return []
    return periods


def quarters_sorted(core: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Return list of quarterly snapshots newest-first.
    Works with core["financials"]["quarterly"]["periods"].
    """
    q = _get_period_list(core, "quarterly")
    return _sort_periods_desc(q)


def annuals_sorted(core: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Return list of annual snapshots newest-first.
    Works with core["financials"]["annual"]["periods"].
    """
    a = _get_period_list(core, "annual")
    return _sort_periods_desc(a)


def detect_period_currency(period: Dict[str, Any]) -> Optional[str]:
    """
    Try to get the reporting currency from a period snapshot.
    Common keys: 'currency', 'ccy', 'report_ccy', etc.
    """
    if not period:
        return None
    for k in ["currency", "ccy", "report_ccy", "reporting_currency"]:
        val = period.get(k)
        if val:
            return str(val).upper()
    # maybe balance has it
    bal = period.get("balance") or {}
    for k in ["currency", "ccy", "report_ccy", "reporting_currency"]:
        val = bal.get(k)
        if val:
            return str(val).upper()
    return None


def all_periods_sorted(core: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Combine quarterly + annual, dedupe by date signature, newest-first.
    """
    q = quarters_sorted(core)
    a = annuals_sorted(core)
    buckets: Dict[str, Dict[str, Any]] = {}

    def sig(p: Dict[str, Any]) -> Optional[str]:
        dt = _extract_period_date(p)
        return dt.strftime("%Y-%m-%d") if dt else None

    for src in [q, a]:
        for p in src:
            s = sig(p)
            if s and s not in buckets:
                buckets[s] = p

    # sort again newest-first
    combos = list(buckets.values())
    combos = _sort_periods_desc(combos)
    return combos
