# domain/services/balance_sheet_metrics.py --> Show how we calculate NCAV, solvency, and liquidity
from typing import Any, Optional, Dict

from domain.services.fx_utils import convert_between
from domain.services.periods import all_periods_sorted, detect_period_currency


def safe_float(x: Any) -> Optional[float]:
    """
    Convert x to float if possible, else return None.
    """
    try:
        if x is None:
            return None
        if isinstance(x, (int, float)):
            return float(x)
        s = str(x).strip()
        if s == "" or s.lower() == "nan" or s.lower() == "none":
            return None
        return float(s)
    except Exception:
        return None


def _extract_val(raw: Any) -> Optional[float]:
    """
    Handle structures like:
        {"val": 123.0, "unit": "CNY", ...}
    or just a bare number.
    """
    if isinstance(raw, dict) and "val" in raw:
        raw = raw.get("val")
    return safe_float(raw)


def get_balance_value(period: Dict[str, Any], key: str) -> Optional[float]:
    """
    Safe pull from balance sheet dict in a snapshot period.

    Supports cached formats like:
        period["balance"][key] = {"val": 123.0, "unit": "CNY"}
    and also flat:
        period[key] = 123.0
    """
    if not period:
        return None

    # Check balance-like containers first
    for container_key in ["balance", "balance_sheet", "bs"]:
        bal = period.get(container_key)
        if isinstance(bal, Dict) and key in bal:
            return _extract_val(bal.get(key))

    # Fallback to flat on period
    if key in period:
        return _extract_val(period.get(key))

    return None


def listing_ccy_for_ticker(core: Dict[str, Any]) -> Optional[str]:
    """
    Figure out what currency the listing / quote is in.
    Try meta then fallback per-period currency.
    """
    meta = core.get("meta", {})
    ccy = meta.get("currency") or meta.get("listing_currency")
    if ccy:
        return str(ccy).upper()

    # fallback: sniff from most recent period
    periods = all_periods_sorted(core)
    if periods:
        p0 = periods[0]
        det = detect_period_currency(p0)
        if det:
            return det.upper()

    return None


def current_ratio(period: Dict[str, Any]) -> Optional[float]:
    """
    current assets / current liabilities
    """
    ca = get_balance_value(period, "assets_current")
    cl = get_balance_value(period, "liab_current")
    if ca is None or cl is None or cl == 0:
        return None
    return float(ca) / float(cl)


def de_ratio(period: Dict[str, Any]) -> Optional[float]:
    """
    total liabilities / (total assets - total liabilities)
    This is 'Debt-to-Equity' in Graham-ish loose sense.
    """
    ta = get_balance_value(period, "assets_total")
    tl = get_balance_value(period, "liab_total")
    if ta is None or tl is None:
        return None
    equity = float(ta) - float(tl)
    if equity == 0:
        return None
    return float(tl) / equity


def ncav_total_native(period: Dict[str, Any]) -> Optional[float]:
    """
    Net Current Asset Value (in the company's reporting ccy):
    NCAV = current assets - total liabilities

    If we don't have current assets (e.g. some SEC facts), we fall back
    to total assets as a rough approximation.
    """
    ca = get_balance_value(period, "assets_current")
    if ca is None:
        ca = get_balance_value(period, "assets_total")
    tl = get_balance_value(period, "liab_total")
    if ca is None or tl is None:
        return None
    return float(ca) - float(tl)


def ncav_total_usd(
    period: Dict[str, Any],
    fx_rates: Dict[str, float],
) -> Optional[float]:
    """
    Same NCAV but converted to USD using fx_rates.
    """
    native_ncav = ncav_total_native(period)
    if native_ncav is None:
        return None
    src_ccy = detect_period_currency(period)
    return convert_between(native_ncav, src_ccy, "USD", fx_rates)


def compute_ncav_ps_from_period(
    period: Dict[str, Any],
    shares_out: Optional[float],
) -> Optional[float]:
    """
    NCAV per share in native currency.
    """
    total_ncav = ncav_total_native(period)
    if total_ncav is None:
        return None
    if shares_out is None or shares_out == 0:
        return None
    return float(total_ncav) / float(shares_out)
