"""Rule-based yield curve regime classification.

Classifies each (date, currency, curve_type) observation into one of seven
regimes based on the 2s10s slope, 10yr yield level, and their recent changes.

All functions are pure: DataFrames in, DataFrames out, no I/O.
"""

from __future__ import annotations

import polars as pl

# Priority order for classification:
# 1. inverted  — 2s10s < -25bp
# 2. flat      — |2s10s| < 25bp
# 3. dynamic   — bear_steep / bull_flat / bear_flat / bull_steep
# 4. normal_steep — 2s10s > 100bp AND level < historical median
# 5. normal    — fallback

REGIME_SCHEMA = {
    "date": pl.Date,
    "currency": pl.Utf8,
    "curve_type": pl.Utf8,
    "regime": pl.Utf8,
    "slope_2s10s_bp": pl.Float64,
    "level_10y": pl.Float64,
}

# Thresholds for standard curves (with 2yr tenor)
_INVERTED_THRESHOLD = -25.0  # bp
_FLAT_THRESHOLD = 25.0  # bp (absolute)
_NORMAL_STEEP_THRESHOLD = 100.0  # bp
_CHANGE_THRESHOLD = 10.0  # bp — minimum change to classify as rising/falling

# Adjusted thresholds for GBP (5s10s since no 2yr available)
_GBP_INVERTED_THRESHOLD = -15.0
_GBP_FLAT_THRESHOLD = 15.0
_GBP_NORMAL_STEEP_THRESHOLD = 60.0


def classify_regimes(
    curves_df: pl.DataFrame,
    lookback_days: int = 20,
) -> pl.DataFrame:
    """Classify yield curve regimes for each observation date.

    For each (date, currency, curve_type) group, assigns one of:
    ``inverted``, ``flat``, ``bear_steep``, ``bull_flat``, ``bear_flat``,
    ``bull_steep``, ``normal_steep``, or ``normal``.

    Uses 2s10s slope where available; falls back to 5s10s with adjusted
    thresholds for currencies lacking a 2yr tenor (e.g. GBP).

    Args:
        curves_df: DataFrame with columns ``date``, ``currency``,
            ``curve_type``, ``tenor_years``, ``yield_pct``.
        lookback_days: Number of business days for computing level/slope
            direction changes. Defaults to 20.

    Returns:
        DataFrame with columns: ``date``, ``currency``, ``curve_type``,
        ``regime``, ``slope_2s10s_bp``, ``level_10y``.
    """
    if curves_df.is_empty():
        return pl.DataFrame(schema=REGIME_SCHEMA)

    results: list[dict] = []

    for (ccy, ctype), group_df in curves_df.group_by(["currency", "curve_type"]):
        _classify_currency(
            group_df, str(ccy), str(ctype), lookback_days, results
        )

    if not results:
        return pl.DataFrame(schema=REGIME_SCHEMA)

    return pl.DataFrame(results).cast(REGIME_SCHEMA).sort("date", "currency")


def _classify_currency(
    group_df: pl.DataFrame,
    currency: str,
    curve_type: str,
    lookback_days: int,
    results: list[dict],
) -> None:
    """Classify regimes for a single (currency, curve_type) group."""
    # Build per-date lookup of tenor -> yield
    dates_sorted = group_df.sort("date")["date"].unique().sort().to_list()

    # Determine which slope to use
    all_tenors = set(group_df["tenor_years"].unique().to_list())
    has_2yr = 2.0 in all_tenors
    has_5yr = 5.0 in all_tenors
    has_10yr = 10.0 in all_tenors

    if has_10yr and (has_2yr or has_5yr):
        short_tenor = 2.0 if has_2yr else 5.0
    else:
        return  # Cannot classify without 10yr + a short tenor

    use_gbp_thresholds = not has_2yr

    # Extract time series of slope and level
    slope_series: dict[object, float] = {}
    level_series: dict[object, float] = {}

    for row in group_df.iter_rows(named=True):
        d = row["date"]
        tenor = row["tenor_years"]
        yld = row["yield_pct"]
        if tenor == 10.0:
            level_series[d] = yld
        if tenor == short_tenor:
            # Store negative key to compute slope later
            slope_series.setdefault(d, {})  # type: ignore[arg-type]
        if tenor in (short_tenor, 10.0):
            slope_series.setdefault(d, {})  # type: ignore[arg-type]
            slope_series[d][tenor] = yld  # type: ignore[index]

    # Compute actual slope values
    slope_bp: dict[object, float] = {}
    for d, tenors in slope_series.items():  # type: ignore[assignment]
        if short_tenor in tenors and 10.0 in tenors:  # type: ignore[operator]
            slope_bp[d] = (tenors[10.0] - tenors[short_tenor]) * 100  # type: ignore[index]

    # Compute historical median of 10yr level
    level_values = list(level_series.values())
    if level_values:
        sorted_levels = sorted(level_values)
        mid = len(sorted_levels) // 2
        if len(sorted_levels) % 2 == 0:
            historical_median = (sorted_levels[mid - 1] + sorted_levels[mid]) / 2
        else:
            historical_median = sorted_levels[mid]
    else:
        historical_median = None

    # Select thresholds
    if use_gbp_thresholds:
        inv_thresh = _GBP_INVERTED_THRESHOLD
        flat_thresh = _GBP_FLAT_THRESHOLD
        steep_thresh = _GBP_NORMAL_STEEP_THRESHOLD
    else:
        inv_thresh = _INVERTED_THRESHOLD
        flat_thresh = _FLAT_THRESHOLD
        steep_thresh = _NORMAL_STEEP_THRESHOLD

    # Classify each date
    for d in dates_sorted:
        if d not in slope_bp or d not in level_series:
            continue

        current_slope = slope_bp[d]
        current_level = level_series[d]

        regime = _classify_single(
            current_slope,
            current_level,
            d,
            slope_bp,
            level_series,
            dates_sorted,
            lookback_days,
            historical_median,
            inv_thresh,
            flat_thresh,
            steep_thresh,
        )

        results.append({
            "date": d,
            "currency": currency,
            "curve_type": curve_type,
            "regime": regime,
            "slope_2s10s_bp": current_slope,
            "level_10y": current_level,
        })


def _classify_single(
    slope: float,
    level: float,
    current_date: object,
    slope_bp: dict[object, float],
    level_series: dict[object, float],
    dates_sorted: list,
    lookback_days: int,
    historical_median: float | None,
    inv_thresh: float,
    flat_thresh: float,
    steep_thresh: float,
) -> str:
    """Classify a single observation into a regime label."""
    # Static rules (highest priority)
    if slope < inv_thresh:
        return "inverted"
    if abs(slope) < flat_thresh:
        return "flat"

    # Dynamic rules — require sufficient history
    idx = dates_sorted.index(current_date)
    if idx >= lookback_days:
        lookback_date = dates_sorted[idx - lookback_days]
        if lookback_date in slope_bp and lookback_date in level_series:
            level_change = (level - level_series[lookback_date]) * 100  # bp
            slope_change = slope - slope_bp[lookback_date]  # already in bp

            level_rising = level_change > _CHANGE_THRESHOLD
            level_falling = level_change < -_CHANGE_THRESHOLD
            slope_rising = slope_change > _CHANGE_THRESHOLD
            slope_falling = slope_change < -_CHANGE_THRESHOLD

            if level_rising and slope_rising:
                return "bear_steep"
            if level_falling and slope_falling:
                return "bull_flat"
            if level_rising and slope_falling:
                return "bear_flat"
            if level_falling and slope_rising:
                return "bull_steep"

    # Normal steep
    if slope > steep_thresh and historical_median is not None and level < historical_median:
        return "normal_steep"

    return "normal"
