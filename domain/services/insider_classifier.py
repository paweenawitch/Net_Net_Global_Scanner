# domain/services/insider_classifier.py
from typing import Any, Dict, Optional, Tuple

from domain.services.balance_sheet_metrics import safe_float


def insider_signal(
    insider_blob: Optional[Dict[str, Any]]
) -> Tuple[str, Dict[str, Optional[float]]]:
    """
    Collapse raw insider activity into headline string like:
    "Buy", "Sell", "Net Buy", "Net Sell", "None", "Unknown"

    Supports both:
      - {"total_buy_trades": ..., "total_sell_trades": ..., "net_shares_change": ...}
      - {"buys_count": ..., "sells_count": ..., "net_shares": ..., "as_of": ...}

    Returns:
        (headline, stats_dict)

    stats_dict keys:
        total_buy_trades
        total_sell_trades
        net_shares_change  (positive => insiders accumulated)
        last_activity_date
        source
    """
    if not insider_blob:
        return (
            "None",
            {
                "total_buy_trades": None,
                "total_sell_trades": None,
                "net_shares_change": None,
                "last_activity_date": None,
                "source": None,
            },
        )

    # Try multiple key names for robustness
    total_buy = safe_float(
        insider_blob.get("total_buy_trades")
        if "total_buy_trades" in insider_blob
        else insider_blob.get("buys_count")
    )
    total_sell = safe_float(
        insider_blob.get("total_sell_trades")
        if "total_sell_trades" in insider_blob
        else insider_blob.get("sells_count")
    )
    net_chg = safe_float(
        insider_blob.get("net_shares_change")
        if "net_shares_change" in insider_blob
        else insider_blob.get("net_shares")
    )
    last_dt = insider_blob.get("last_activity_date") or insider_blob.get("as_of")
    src = insider_blob.get("source")

    # Heuristic classification
    headline = "Unknown"
    if total_buy is None and total_sell is None and net_chg is None:
        headline = "None"
    else:
        if net_chg is not None:
            if net_chg > 0:
                # Bought overall
                headline = "Net Buy"
            elif net_chg < 0:
                headline = "Net Sell"

        # If we have explicit buy_trades/sell_trades dominance, override
        if total_buy is not None and total_sell is not None:
            if total_buy > 0 and (total_sell == 0 or total_sell is None):
                headline = "Buy"
            if total_sell > 0 and (total_buy == 0 or total_buy is None):
                headline = "Sell"

    return (
        headline,
        {
            "total_buy_trades": total_buy,
            "total_sell_trades": total_sell,
            "net_shares_change": net_chg,
            "last_activity_date": last_dt,
            "source": src,
        },
    )
