"""Analog forecasting via nearest neighbors in PCA score space.

Finds historical dates where the yield curve looked similar to a target
date (nearest neighbors in PC space), then tracks what happened over
the next *h* days to build a spaghetti forecast.

All functions are pure: PCAResult + DataFrames in, DataFrames out, no I/O.
"""

from __future__ import annotations

import logging
from datetime import date, timedelta

import numpy as np
import polars as pl

from thalweg.analytics.pca import PCAResult

logger = logging.getLogger(__name__)

ANALOG_SCHEMA = {
    "analog_date": pl.Date,
    "distance": pl.Float64,
    "currency": pl.Utf8,
    "curve_type": pl.Utf8,
}

FORECAST_SCHEMA = {
    "currency": pl.Utf8,
    "curve_type": pl.Utf8,
    "analog_date": pl.Date,
    "future_date": pl.Date,
    "tenor_years": pl.Float64,
    "yield_pct": pl.Float64,
    "is_median": pl.Boolean,
}

# Default buffer: exclude analogs within this many days of the target
# to prevent look-ahead overlap with the forecast horizon.
_DEFAULT_BUFFER_DAYS: int = 21


def find_analogs(
    pca_result: PCAResult,
    target_date: date | None = None,
    k: int = 20,
    buffer_days: int = _DEFAULT_BUFFER_DAYS,
) -> pl.DataFrame:
    """Find K historical dates most similar to target in PC space.

    Computes Euclidean distance in PC space between the target date and
    every other date in the PCA scores. Excludes the target date itself
    and dates within *buffer_days* of the target to avoid look-ahead
    overlap with any subsequent forecast horizon.

    Args:
        pca_result: Fitted PCA result containing scores_df with date
            and pc1..pcK columns.
        target_date: Reference date. Defaults to the latest date in
            scores_df.
        k: Number of nearest neighbors to return.
        buffer_days: Minimum number of days between an analog and the
            target date. Defaults to 21.

    Returns:
        DataFrame with columns ``analog_date``, ``distance``,
        ``currency``, ``curve_type`` sorted by distance ascending.
        Returns an empty DataFrame with ``ANALOG_SCHEMA`` if there is
        insufficient data.
    """
    scores_df = pca_result.scores_df.sort("date")

    if scores_df.is_empty():
        return pl.DataFrame(schema=ANALOG_SCHEMA)

    # Determine number of actual PCA components
    n_components = pca_result.components.shape[0]
    pc_cols = [f"pc{i + 1}" for i in range(n_components)]

    # Validate that required columns are present
    available_cols = [c for c in pc_cols if c in scores_df.columns]
    if len(available_cols) < n_components:
        logger.warning(
            "PCA result has %d components but scores_df only has columns %s",
            n_components, scores_df.columns,
        )
        return pl.DataFrame(schema=ANALOG_SCHEMA)

    # Default target to latest date
    if target_date is None:
        target_date = scores_df["date"].max()

    # Find target row
    target_row = scores_df.filter(pl.col("date") == target_date)
    if target_row.is_empty():
        logger.info("Target date %s not found in scores_df", target_date)
        return pl.DataFrame(schema=ANALOG_SCHEMA)

    target_scores = target_row.select(available_cols).to_numpy().flatten()  # (K,)

    # Exclude target itself and dates within the buffer
    buffer_start = target_date - timedelta(days=buffer_days)
    buffer_end = target_date + timedelta(days=buffer_days)
    candidates = scores_df.filter(
        (pl.col("date") < buffer_start) | (pl.col("date") > buffer_end)
    )

    if candidates.is_empty():
        return pl.DataFrame(schema=ANALOG_SCHEMA)

    # Compute Euclidean distance from target to each candidate
    candidate_scores = candidates.select(available_cols).to_numpy()  # (M, K)
    distances = np.sqrt(
        np.sum((candidate_scores - target_scores) ** 2, axis=1)
    )

    # Sort by distance and take top K
    result_df = candidates.select("date").with_columns(
        pl.Series("distance", distances),
    ).sort("distance").head(k)

    # Build output with schema
    return result_df.select(
        pl.col("date").alias("analog_date"),
        pl.col("distance"),
        pl.lit(pca_result.currency).alias("currency"),
        pl.lit(pca_result.curve_type).alias("curve_type"),
    ).cast(ANALOG_SCHEMA)


def forecast_from_analogs(
    curves_df: pl.DataFrame,
    pca_result: PCAResult,
    target_date: date | None = None,
    k: int = 20,
    horizon_days: int = 63,
) -> pl.DataFrame:
    """For each analog, find what the curve looked like horizon_days later.

    Finds the *k* nearest analogs in PCA space, then for each analog
    date looks up the observed yield curve *horizon_days* later. If the
    exact future date is missing (weekends, holidays), the closest
    available date within a +/-5 day window is used instead.

    A median forecast path (``is_median=True``) is appended, computed
    as the per-tenor median across all analog outcomes.

    Args:
        curves_df: DataFrame with columns ``date``, ``currency``,
            ``curve_type``, ``tenor_years``, ``yield_pct``.
        pca_result: Fitted PCA result for a single currency/curve_type.
        target_date: Reference date. Defaults to the latest date in
            scores_df.
        k: Number of nearest analogs.
        horizon_days: Number of calendar days to look ahead.

    Returns:
        DataFrame with columns ``currency``, ``curve_type``,
        ``analog_date``, ``future_date``, ``tenor_years``,
        ``yield_pct``, ``is_median``.
        Returns an empty DataFrame with ``FORECAST_SCHEMA`` if there is
        insufficient data.
    """
    analogs = find_analogs(pca_result, target_date, k, buffer_days=_DEFAULT_BUFFER_DAYS)

    if analogs.is_empty():
        return pl.DataFrame(schema=FORECAST_SCHEMA)

    currency = pca_result.currency
    curve_type = pca_result.curve_type

    # Resolve target_date (same logic as find_analogs)
    if target_date is None:
        target_date = pca_result.scores_df["date"].max()

    # Filter curves to this currency / curve_type
    ccy_curves = curves_df.filter(
        (pl.col("currency") == currency)
        & (pl.col("curve_type") == curve_type)
    )

    if ccy_curves.is_empty():
        return pl.DataFrame(schema=FORECAST_SCHEMA)

    # Get the set of available dates for fast lookup
    date_set = set(ccy_curves["date"].unique().to_list())

    # Tolerance window for finding the nearest available date
    date_tolerance = 5

    rows: list[dict] = []
    analog_dates = analogs["analog_date"].to_list()

    for analog_date in analog_dates:
        ideal_future = analog_date + timedelta(days=horizon_days)
        actual_future = _find_nearest_date(ideal_future, date_set, date_tolerance)

        if actual_future is None:
            continue

        # Look up the curve at the future date
        future_curve = ccy_curves.filter(pl.col("date") == actual_future)

        for row in future_curve.iter_rows(named=True):
            rows.append({
                "currency": currency,
                "curve_type": curve_type,
                "analog_date": analog_date,
                "future_date": actual_future,
                "tenor_years": row["tenor_years"],
                "yield_pct": row["yield_pct"],
                "is_median": False,
            })

    if not rows:
        return pl.DataFrame(schema=FORECAST_SCHEMA)

    individual_df = pl.DataFrame(rows).cast(FORECAST_SCHEMA)

    # Compute median path across all analogs, per tenor
    median_future = target_date + timedelta(days=horizon_days)
    median_df = (
        individual_df
        .group_by("tenor_years")
        .agg(pl.col("yield_pct").median())
        .with_columns(
            pl.lit(currency).alias("currency"),
            pl.lit(curve_type).alias("curve_type"),
            pl.lit(target_date).alias("analog_date"),
            pl.lit(median_future).alias("future_date"),
            pl.lit(True).alias("is_median"),
        )
        .select(list(FORECAST_SCHEMA.keys()))
        .cast(FORECAST_SCHEMA)
    )

    return pl.concat([individual_df, median_df])


def _find_nearest_date(
    target: date,
    available: set[date],
    tolerance: int,
) -> date | None:
    """Find the closest available date within a tolerance window.

    Checks the target date first, then searches outward by +/-1, +/-2,
    etc. up to *tolerance* days.

    Args:
        target: The ideal date to look up.
        available: Set of dates that exist in the data.
        tolerance: Maximum number of days to search in each direction.

    Returns:
        The nearest available date, or None if nothing is within range.
    """
    if target in available:
        return target

    for offset in range(1, tolerance + 1):
        before = target - timedelta(days=offset)
        after = target + timedelta(days=offset)
        if before in available:
            return before
        if after in available:
            return after

    return None
