#domain/services/flag_classifier.py
from typing import Optional, Tuple, List
def classify_flags(
    price_to_ncavps: Optional[float],
    cr: Optional[float],
    de: Optional[float],
    ncav_qoq: Optional[float],
    ncav_hoh: Optional[float],
    ncav_yoy: Optional[float],
    dil_qoq: Optional[float],
    dil_hoh: Optional[float],
    dil_yoy: Optional[float],
    max_dil_1y: Optional[float],
    max_issue_3y: Optional[float],
    max_buyback_3y: Optional[float],
    is_outdated: bool,
) -> Tuple[List[str], List[str]]:
    """
    Produce green_flags[], red_flags[] according to Graham-ish heuristics.
    Rule of thumb:
        Green when:
          - Price <= ~2/3 of NCAV/share
          - Current ratio healthy (>2 maybe)
          - NCAV not collapsing fast
          - Evidence of buyback
        Red when:
          - Data stale
          - Significant dilution
          - NCAV melting fast
          - Leverage scary
    """
    green: List[str] = []
    red: List[str] = []

    # value
    if price_to_ncavps is not None and price_to_ncavps <= 2.0 / 3.0:
        green.append("Trading ≤ 2/3 NCAV")

    # liquidity
    if cr is not None and cr >= 2.0:
        green.append("Current ratio ≥ 2")

    # capital discipline
    if max_buyback_3y is not None and max_buyback_3y < -0.05:
        green.append("Meaningful buyback in last 3y")

    # NCAV stability/improvement
    # if NCAV change YoY >= 0 -> not burning
    if ncav_yoy is not None and ncav_yoy >= 0:
        green.append("NCAV stable YoY or improving")

    # stale data
    if is_outdated:
        red.append("Financials are stale")

    # leverage
    if de is not None and de > 1.5:
        red.append("High leverage")

    # NCAV burn
    # if NCAV down more than ~20% recently -> danger
    for label, chg in [("QoQ", ncav_qoq), ("HoH", ncav_hoh), ("YoY", ncav_yoy)]:
        if chg is not None and chg < -0.2:
            red.append(f"NCAV down {label} >20%")

    # dilution recent
    # If shares_out jumped >5% in recent windows, flag
    for label, dil in [("QoQ", dil_qoq), ("HoH", dil_hoh), ("YoY", dil_yoy)]:
        if dil is not None and dil > 0.05:
            red.append(f"Dilution {label} >5%")

    # worst 12m
    if max_dil_1y is not None and max_dil_1y > 0.08:
        red.append("Issued >8% in last 12m")

    # worst 3y
    if max_issue_3y is not None and max_issue_3y > 0.20:
        red.append("Issued >20% in last 3y")

    return green, red