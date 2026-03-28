"""Empirical shock distributions and fan chart generation.

Computes what range of yield curve moves has historically occurred over
various horizons, using PCA score deltas. Returns quantile bands in
reconstructed yield space suitable for fan chart visualization.

All functions are pure: PCAResult in, DataFrames out, no I/O.
"""

from __future__ import annotations

import logging

import numpy as np
import polars as pl

from thalweg.analytics.pca import PCAResult

logger = logging.getLogger(__name__)

# Default horizons: 1 month, 3 months, 6 months, 1 year (business days)
DEFAULT_HORIZONS: list[int] = [21, 63, 126, 252]

# Default quantile levels for distribution bands
DEFAULT_QUANTILES: list[float] = [
    0.005, 0.025, 0.05, 0.10, 0.25,
    0.50,
    0.75, 0.90, 0.95, 0.975, 0.995,
]

# Output schema for fan chart data
FAN_SCHEMA = {
    "currency": pl.Utf8,
    "curve_type": pl.Utf8,
    "horizon_days": pl.Int32,
    "quantile": pl.Float64,
    "tenor_years": pl.Float64,
    "yield_pct": pl.Float64,
}


def compute_shock_distribution(
    pca_result: PCAResult,
    horizon_days: int = 21,
    quantiles: list[float] | None = None,
) -> pl.DataFrame:
    """Compute empirical shock distribution for a single horizon.

    Derives the historical distribution of PCA score changes over the
    given horizon, then projects those quantile shocks from today's
    position back into yield space.

    Args:
        pca_result: Fitted PCA result containing scores, loadings,
            mean, and std.
        horizon_days: Number of observation days for the shock horizon.
        quantiles: Quantile levels to compute. Defaults to
            ``DEFAULT_QUANTILES``.

    Returns:
        DataFrame with columns ``currency``, ``curve_type``,
        ``horizon_days``, ``quantile``, ``tenor_years``, ``yield_pct``.
        Returns an empty DataFrame with ``FAN_SCHEMA`` if there is
        insufficient data.
    """
    if quantiles is None:
        quantiles = DEFAULT_QUANTILES

    # Determine number of actual PCA components
    k = pca_result.components.shape[0]
    n_tenors = len(pca_result.tenors)

    # Extract scores as numpy array, using only the fitted components
    pc_cols = [f"pc{i + 1}" for i in range(k)]
    scores_df = pca_result.scores_df.sort("date")

    # Check that all needed columns exist
    available_cols = [c for c in pc_cols if c in scores_df.columns]
    if len(available_cols) < k:
        logger.warning(
            "PCA result has %d components but scores_df only has columns %s",
            k, scores_df.columns,
        )
        return pl.DataFrame(schema=FAN_SCHEMA)

    scores = scores_df.select(available_cols).to_numpy()  # (N, K)
    n_obs = scores.shape[0]

    # Need at least horizon + 1 observations to compute one delta
    if n_obs < horizon_days + 1:
        logger.info(
            "Insufficient data for horizon %d: only %d observations",
            horizon_days, n_obs,
        )
        return pl.DataFrame(schema=FAN_SCHEMA)

    # Compute score deltas: delta[i] = scores[i + h] - scores[i]
    delta_scores = scores[horizon_days:] - scores[:-horizon_days]  # (N-h, K)

    if delta_scores.shape[0] == 0:
        return pl.DataFrame(schema=FAN_SCHEMA)

    # Compute empirical quantiles of deltas along axis 0
    quantile_deltas = np.quantile(
        delta_scores, quantiles, axis=0,
    )  # (n_quantiles, K)

    # Today's scores: last row
    today_scores = scores[-1]  # (K,)

    # Build rows: for each quantile, project future scores back to yield space
    rows: list[dict] = []
    for q_idx, q in enumerate(quantiles):
        future_scores = today_scores + quantile_deltas[q_idx]  # (K,)
        # Reconstruct to yield space: standardized = scores @ components
        standardized = future_scores @ pca_result.components  # (T,)
        yields = standardized * pca_result.std + pca_result.mean  # (T,)

        for t_idx in range(n_tenors):
            rows.append({
                "currency": pca_result.currency,
                "curve_type": pca_result.curve_type,
                "horizon_days": horizon_days,
                "quantile": q,
                "tenor_years": pca_result.tenors[t_idx],
                "yield_pct": float(yields[t_idx]),
            })

    return pl.DataFrame(rows).cast(FAN_SCHEMA)


def compute_fan_chart(
    curves_df: pl.DataFrame,
    pca_result: PCAResult,
    horizons: list[int] | None = None,
    quantiles: list[float] | None = None,
) -> pl.DataFrame:
    """Compute fan chart bands across multiple horizons.

    Calls ``compute_shock_distribution`` for each horizon and
    concatenates the results into a single DataFrame.

    Args:
        curves_df: Curve data (available for context but not directly
            used in the computation).
        pca_result: Fitted PCA result containing scores, loadings,
            mean, and std.
        horizons: List of horizon lengths in observation days.
            Defaults to ``DEFAULT_HORIZONS``.
        quantiles: Quantile levels to compute. Defaults to
            ``DEFAULT_QUANTILES``.

    Returns:
        DataFrame with columns ``currency``, ``curve_type``,
        ``horizon_days``, ``quantile``, ``tenor_years``, ``yield_pct``.
        Returns an empty DataFrame with ``FAN_SCHEMA`` if no horizons
        produce results.
    """
    if horizons is None:
        horizons = DEFAULT_HORIZONS
    if quantiles is None:
        quantiles = DEFAULT_QUANTILES

    parts: list[pl.DataFrame] = []
    for h in horizons:
        result = compute_shock_distribution(pca_result, horizon_days=h, quantiles=quantiles)
        if not result.is_empty():
            parts.append(result)

    if not parts:
        return pl.DataFrame(schema=FAN_SCHEMA)

    return pl.concat(parts).cast(FAN_SCHEMA)
