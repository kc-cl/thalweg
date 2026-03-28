"""Yield curve slope, curvature, and cross-market spread analytics.

All functions are pure: DataFrames in, DataFrames out, no I/O.
"""

from __future__ import annotations

import polars as pl

# Default slope pairs: (short_tenor, long_tenor) in years
DEFAULT_SLOPE_PAIRS: list[tuple[float, float]] = [(2, 10), (2, 30), (5, 30)]

# Default cross-market currency pairs: (currency_a, currency_b)
DEFAULT_CROSS_MARKET_PAIRS: list[tuple[str, str]] = [
    ("USD", "CAD"),
    ("USD", "EUR"),
    ("EUR", "GBP"),
    ("CAD", "GBP"),
]

# Output schemas for empty-frame returns
SLOPES_SCHEMA = {
    "date": pl.Date,
    "currency": pl.Utf8,
    "curve_type": pl.Utf8,
    "slope_name": pl.Utf8,
    "value_bp": pl.Float64,
}

CURVATURE_SCHEMA = {
    "date": pl.Date,
    "currency": pl.Utf8,
    "curve_type": pl.Utf8,
    "butterfly_name": pl.Utf8,
    "value_bp": pl.Float64,
}

CROSS_MARKET_SCHEMA = {
    "date": pl.Date,
    "pair": pl.Utf8,
    "tenor_years": pl.Float64,
    "spread_bp": pl.Float64,
}


def _slope_name(short: float, long: float) -> str:
    """Format a slope pair as e.g. '2s10s'.

    Args:
        short: Short-end tenor in years.
        long: Long-end tenor in years.

    Returns:
        Human-readable slope name like '2s10s'.
    """
    s = int(short) if short == int(short) else short
    l_ = int(long) if long == int(long) else long
    return f"{s}s{l_}s"


def compute_slopes(
    curves_df: pl.DataFrame,
    pairs: list[tuple[float, float]] | None = None,
) -> pl.DataFrame:
    """Compute yield curve slopes (long minus short tenor) in basis points.

    For each (date, currency, curve_type) group and each tenor pair, the slope
    is ``(yield_long - yield_short) * 100``.  Pairs whose tenors are not both
    present in the group are silently skipped.

    Args:
        curves_df: DataFrame with columns ``date``, ``currency``,
            ``curve_type``, ``tenor_years``, ``yield_pct``.
        pairs: List of ``(short_tenor, long_tenor)`` tuples in years.
            Defaults to ``[(2, 10), (2, 30), (5, 30)]``.

    Returns:
        DataFrame with columns ``date``, ``currency``, ``curve_type``,
        ``slope_name`` (e.g. ``'2s10s'``), ``value_bp``.
    """
    if pairs is None:
        pairs = DEFAULT_SLOPE_PAIRS

    if curves_df.is_empty():
        return pl.DataFrame(schema=SLOPES_SCHEMA)

    group_keys = ["date", "currency", "curve_type"]
    results: list[dict] = []

    for group_key, group_df in curves_df.group_by(group_keys):
        date_val, ccy, ctype = group_key
        # Build a tenor -> yield lookup for this group
        tenor_yield = dict(
            zip(
                group_df["tenor_years"].to_list(),
                group_df["yield_pct"].to_list(),
            )
        )

        for short, long in pairs:
            short_f = float(short)
            long_f = float(long)
            if short_f in tenor_yield and long_f in tenor_yield:
                slope_bp = (tenor_yield[long_f] - tenor_yield[short_f]) * 100
                results.append(
                    {
                        "date": date_val,
                        "currency": ccy,
                        "curve_type": ctype,
                        "slope_name": _slope_name(short_f, long_f),
                        "value_bp": slope_bp,
                    }
                )

    if not results:
        return pl.DataFrame(schema=SLOPES_SCHEMA)

    return pl.DataFrame(results).cast(SLOPES_SCHEMA)


def compute_curvature(curves_df: pl.DataFrame) -> pl.DataFrame:
    """Compute butterfly spreads (curvature) in basis points.

    Two butterflies are computed when tenors are available:

    * **2s5s10s**: ``(2 * Y(5) - Y(2) - Y(10)) * 100``
    * **5s10s30s**: ``(2 * Y(10) - Y(5) - Y(30)) * 100``

    Args:
        curves_df: DataFrame with columns ``date``, ``currency``,
            ``curve_type``, ``tenor_years``, ``yield_pct``.

    Returns:
        DataFrame with columns ``date``, ``currency``, ``curve_type``,
        ``butterfly_name``, ``value_bp``.
    """
    if curves_df.is_empty():
        return pl.DataFrame(schema=CURVATURE_SCHEMA)

    # (belly_tenor, wing_short, wing_long, name)
    butterflies = [
        (5.0, 2.0, 10.0, "2s5s10s"),
        (10.0, 5.0, 30.0, "5s10s30s"),
    ]

    group_keys = ["date", "currency", "curve_type"]
    results: list[dict] = []

    for group_key, group_df in curves_df.group_by(group_keys):
        date_val, ccy, ctype = group_key
        tenor_yield = dict(
            zip(
                group_df["tenor_years"].to_list(),
                group_df["yield_pct"].to_list(),
            )
        )

        for belly, wing_short, wing_long, name in butterflies:
            if belly in tenor_yield and wing_short in tenor_yield and wing_long in tenor_yield:
                value_bp = (
                    2 * tenor_yield[belly] - tenor_yield[wing_short] - tenor_yield[wing_long]
                ) * 100
                results.append(
                    {
                        "date": date_val,
                        "currency": ccy,
                        "curve_type": ctype,
                        "butterfly_name": name,
                        "value_bp": value_bp,
                    }
                )

    if not results:
        return pl.DataFrame(schema=CURVATURE_SCHEMA)

    return pl.DataFrame(results).cast(CURVATURE_SCHEMA)


def compute_cross_market_spreads(
    curves_df: pl.DataFrame,
    pairs: list[tuple[str, str]] | None = None,
) -> pl.DataFrame:
    """Compute yield spreads between two markets at matched tenors.

    For each date, currency pair, and tenor present in **both** curves,
    the spread is ``(yield_a - yield_b) * 100`` in basis points.

    Only dates where both currencies have data contribute to the output.

    Args:
        curves_df: DataFrame with columns ``date``, ``currency``,
            ``curve_type``, ``tenor_years``, ``yield_pct``.
        pairs: List of ``(currency_a, currency_b)`` tuples.
            Defaults to ``[('USD','CAD'), ('USD','EUR'), ('EUR','GBP'),
            ('CAD','GBP')]``.

    Returns:
        DataFrame with columns ``date``, ``pair`` (e.g. ``'USD-CAD'``),
        ``tenor_years``, ``spread_bp``.
    """
    if pairs is None:
        pairs = DEFAULT_CROSS_MARKET_PAIRS

    if curves_df.is_empty():
        return pl.DataFrame(schema=CROSS_MARKET_SCHEMA)

    results: list[dict] = []

    for ccy_a, ccy_b in pairs:
        df_a = curves_df.filter(pl.col("currency") == ccy_a)
        df_b = curves_df.filter(pl.col("currency") == ccy_b)

        if df_a.is_empty() or df_b.is_empty():
            continue

        # Inner join on (date, tenor_years) to get matched observations
        joined = df_a.select(
            pl.col("date"),
            pl.col("tenor_years"),
            pl.col("yield_pct").alias("yield_a"),
        ).join(
            df_b.select(
                pl.col("date"),
                pl.col("tenor_years"),
                pl.col("yield_pct").alias("yield_b"),
            ),
            on=["date", "tenor_years"],
            how="inner",
        )

        if joined.is_empty():
            continue

        pair_label = f"{ccy_a}-{ccy_b}"
        spread_df = joined.with_columns(
            pl.lit(pair_label).alias("pair"),
            ((pl.col("yield_a") - pl.col("yield_b")) * 100).alias("spread_bp"),
        ).select("date", "pair", "tenor_years", "spread_bp")

        results.append(spread_df)

    if not results:
        return pl.DataFrame(schema=CROSS_MARKET_SCHEMA)

    return pl.concat(results).cast(CROSS_MARKET_SCHEMA)
