# domain/services/netnet_analysis.py
from domain.services.periods import all_periods_sorted
from domain.services.balance_sheet_metrics import (
    current_ratio,
    de_ratio,
    ncav_total_native,
    compute_ncav_ps_from_period,
    listing_ccy_for_ticker,
    safe_float,
)
from domain.services.fx_utils import convert_between
from domain.services.trend_analysis import (
    pct_change,
    max_dilution_within_1y,
    max_change_within_3y,
    pair_for_qoq,
    pair_for_hoh,
    pair_for_yoy,
)
from domain.services.data_quality import assess_staleness
from domain.services.insider_classifier import insider_signal
from domain.services.flag_classifier import classify_flags
from domain.models.valuation_result import ValuationResult


def _extract_shares_out(period):
    """
    Try several locations / shapes for shares_out.
    Works with:
        period["shares_out"]
        period["balance"]["shares_out"]["val"]
        period["shares_outstanding"], etc.
    """
    if not period:
        return None

    containers = [period]
    bal = period.get("balance")
    if isinstance(bal, dict):
        containers.append(bal)
    meta = period.get("meta")
    if isinstance(meta, dict):
        containers.append(meta)

    for container in containers:
        for k in ["shares_out", "shares_outstanding", "basic_shares_out"]:
            raw = container.get(k)
            if raw is None:
                continue
            if isinstance(raw, dict) and "val" in raw:
                raw = raw.get("val")
            val = safe_float(raw)
            if val is not None:
                return val

    return None


def analyze_one_ticker(core, insider_blob, last_price, fx_rates) -> ValuationResult:
    periods = all_periods_sorted(core)
    latest = periods[0] if periods else None

    # --- core balance sheet / NCAV ---
    cr = current_ratio(latest)          # safely returns None if latest is None
    de = de_ratio(latest)
    ncav_native = ncav_total_native(latest)

    shares = _extract_shares_out(latest)
    ncav_ps = compute_ncav_ps_from_period(latest, shares)

    # cross-currency NCAV
    listing_ccy = listing_ccy_for_ticker(core)
    ncav_usd = convert_between(ncav_native, listing_ccy, "USD", fx_rates)

    # valuation ratios
    price_to_ncavps = None
    margin_of_safety = None
    if last_price is not None and ncav_ps not in (None, 0):
        price_to_ncavps = float(last_price) / float(ncav_ps)
        margin_of_safety = 1.0 - price_to_ncavps

    # --- trend & dilution ---
    q_pair = pair_for_qoq(periods)
    h_pair = pair_for_hoh(periods)
    y_pair = pair_for_yoy(periods)

    def ncav_from(p):
        return ncav_total_native(p) if p else None

    ncav_qoq = pct_change(
        ncav_from(q_pair[1]) if q_pair else None,
        ncav_from(q_pair[0]) if q_pair else None,
    )
    ncav_hoh = pct_change(
        ncav_from(h_pair[1]) if h_pair else None,
        ncav_from(h_pair[0]) if h_pair else None,
    )
    ncav_yoy = pct_change(
        ncav_from(y_pair[1]) if y_pair else None,
        ncav_from(y_pair[0]) if y_pair else None,
    )

    def so(p):
        return _extract_shares_out(p)

    def dil_from(pair):
        if not pair:
            return None
        old = so(pair[1])
        new = so(pair[0])
        return pct_change(old, new)

    dilution_qoq = dil_from(q_pair)
    dilution_hoh = dil_from(h_pair)
    dilution_yoy = dil_from(y_pair)

    max_dil_1y = max_dilution_within_1y(periods)
    win3 = max_change_within_3y(periods)
    max_issue_3y = win3.max_issue
    max_buyback_3y = win3.max_buyback

    # --- data quality ---
    is_outdated, age_days = assess_staleness(latest)

    # --- insider ---
    insider_headline, insider_stats = insider_signal(insider_blob)

    # --- flags (our policy knobs) ---
    green_flags, red_flags = classify_flags(
        price_to_ncavps,
        cr,
        de,
        ncav_qoq,
        ncav_hoh,
        ncav_yoy,
        dilution_qoq,
        dilution_hoh,
        dilution_yoy,
        max_dil_1y,
        max_issue_3y,
        max_buyback_3y,
        is_outdated,
    )

    # --- period label / fs date ---
    latest_fs_date = None
    latest_label = None
    if latest:
        latest_fs_date = latest.get("statement_date") or latest.get("date")
        latest_label = str(latest_fs_date)

    # --- package result ---
    return ValuationResult(
        ticker=core["meta"]["ticker"],
        exchange=core["meta"].get("exchange"),
        country_iso=core["meta"].get("country_iso"),
        sector=core["meta"].get("sector"),
        industry=core["meta"].get("industry"),
        reporting_currency=listing_ccy,
        latest_fs_date=latest_fs_date,
        current_ratio=cr,
        debt_to_equity=de,
        ncav_total_native=ncav_native,
        ncav_total_usd=ncav_usd,
        ncav_per_share=ncav_ps,
        ncav_ps_shortlist=core["meta"].get("ncav_ps_shortlist"),
        shares_out=shares,
        last_price=last_price,
        price_to_ncavps=price_to_ncavps,
        margin_of_safety=margin_of_safety,
        fx_rate_used=None,
        fx_source="cache",
        ncavps_fx_note=None,
        ncav_change_qoq=ncav_qoq,
        ncav_change_hoh=ncav_hoh,
        ncav_change_yoy=ncav_yoy,
        dilution_qoq=dilution_qoq,
        dilution_hoh=dilution_hoh,
        dilution_yoy=dilution_yoy,
        max_dilution_1y=max_dil_1y,
        max_issue_3y=max_issue_3y,
        max_buyback_3y=max_buyback_3y,
        is_outdated=is_outdated,
        data_age_days=age_days,
        fs_source=core.get("fs_source"),
        note=core.get("note"),
        insider_signal=insider_headline,
        green_flags=green_flags,
        red_flags=red_flags,
        core_period_count=len(periods),
        insider_records=insider_stats.get("total_buy_trades"),
        latest_period_label=latest_label,
        listing_note=core["meta"].get("listing_note"),
        passes_price_to_ncav_rule=(
            price_to_ncavps is not None and price_to_ncavps <= (2.0 / 3.0)
        ),
        has_recent_buyback=(
            win3.max_buyback is not None and win3.max_buyback < -0.05
        ),
        has_recent_dilution=(
            win3.max_issue is not None and win3.max_issue > 0.08
        ),
    )
