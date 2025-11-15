#domain/services/fx_utils.py --> currency helper
from __future__ import annotations
from typing import Dict, Optional
def _ccy_alias(ccy: str) -> str:
    """
    Normalize weird Yahoo codes etc. to standard ISO-ish tickers.
    Example: 'HKD' stays 'HKD', 'JPY' stays 'JPY', 'CNY' vs 'RMB' etc.
    Extend as needed.
    """
    if not ccy:
        return ccy
    ccy_up = ccy.upper()
    aliases = {
        "RMB": "CNY",
        "CNH": "CNY",  # treat offshore/onshore RMB the same for NCAV work
    }
    return aliases.get(ccy_up, ccy_up)


def _normalize_rates(raw_rates: Dict[str, float]) -> Dict[str, float]:
    """
    Take raw map { 'JPY': 0.0067, 'HKD': 0.128, ... } and normalize keys.
    """
    out: Dict[str, float] = {}
    for k, v in raw_rates.items():
        if v is None:
            continue
        ak = _ccy_alias(k)
        # last write wins if duplicates collide
        out[ak] = float(v)
    return out


def convert_between(
    amount: Optional[float],
    from_ccy: Optional[str],
    to_ccy: Optional[str],
    fx_rates: Dict[str, float],
) -> Optional[float]:
    """
    Convert 'amount' in from_ccy to to_ccy using fx_rates.
    Assumptions:
        - fx_rates maps 1 unit of {ccy} -> USD
        - So: value_in_usd = amount * fx_rates[from_ccy]
        - If to_ccy == 'USD', just return that.
        - If to_ccy != 'USD', we do cross via USD.
    If we don't have what we need, return None.
    """
    if amount is None:
        return None
    if from_ccy is None or to_ccy is None:
        return None

    from_ccy_norm = _ccy_alias(from_ccy)
    to_ccy_norm = _ccy_alias(to_ccy)

    # same currency? no conversion
    if from_ccy_norm == to_ccy_norm:
        return float(amount)

    # fx_rates give us ccy -> USD
    if from_ccy_norm not in fx_rates:
        return None
    usd_val = float(amount) * float(fx_rates[from_ccy_norm])

    if to_ccy_norm == "USD":
        return usd_val

    # need USD -> to_ccy
    if to_ccy_norm not in fx_rates:
        return None
    # if 1 JPY = 0.0067 USD, then 1 USD = 1/0.0067 JPY
    usd_to_target = 1.0 / float(fx_rates[to_ccy_norm])
    return usd_val * usd_to_target
