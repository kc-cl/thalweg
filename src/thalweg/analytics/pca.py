"""PCA decomposition of yield curve history.

Decomposes a panel of yield curves into principal components (level, slope,
curvature) for each currency. All functions are pure: DataFrames in,
DataFrames out, no I/O.
"""

from __future__ import annotations

from dataclasses import dataclass

import numpy as np
import polars as pl
from sklearn.decomposition import PCA

# Output schemas for parquet storage
PCA_SCORES_SCHEMA = {
    "date": pl.Date,
    "currency": pl.Utf8,
    "curve_type": pl.Utf8,
    "pc1": pl.Float64,
    "pc2": pl.Float64,
    "pc3": pl.Float64,
}

PCA_LOADINGS_SCHEMA = {
    "currency": pl.Utf8,
    "curve_type": pl.Utf8,
    "component": pl.Utf8,
    "tenor_years": pl.Float64,
    "loading": pl.Float64,
}


@dataclass
class PCAResult:
    """Result of PCA decomposition on yield curve history.

    Attributes:
        currency: Currency code (e.g. 'USD').
        curve_type: Curve type (e.g. 'govt_par').
        tenors: Ordered list of tenors used in the decomposition.
        mean: Per-tenor mean yield used for standardization, shape (T,).
        std: Per-tenor std yield used for standardization, shape (T,).
        components: PCA loading matrix, shape (K, T).
        explained_variance: Fraction of variance explained per component, shape (K,).
        scores_df: DataFrame with date, currency, curve_type, pc1, pc2, pc3.
    """

    currency: str
    curve_type: str
    tenors: list[float]
    mean: np.ndarray
    std: np.ndarray
    components: np.ndarray
    explained_variance: np.ndarray
    scores_df: pl.DataFrame


def fit_pca(
    curves_df: pl.DataFrame,
    currency: str,
    n_components: int = 3,
) -> PCAResult | None:
    """Fit PCA on yield curve history for a single currency.

    Pivots curve data to wide format (rows = dates, columns = tenors),
    drops incomplete dates, standardizes per tenor, and fits sklearn PCA.

    Args:
        curves_df: DataFrame with columns ``date``, ``currency``,
            ``curve_type``, ``tenor_years``, ``yield_pct``.
        currency: Currency to filter and decompose.
        n_components: Maximum number of principal components. Capped at the
            number of available tenors.

    Returns:
        PCAResult with loadings, scores, and explained variance, or None if
        there is insufficient data.
    """
    ccy_df = curves_df.filter(pl.col("currency") == currency)
    if ccy_df.is_empty():
        return None

    # Determine curve_type (use the first one present)
    curve_type = ccy_df["curve_type"][0]

    # Pivot to wide: rows = dates, columns = tenors
    wide = ccy_df.pivot(
        on="tenor_years",
        index="date",
        values="yield_pct",
    ).sort("date")

    # Tenor columns are everything except "date"
    tenor_cols = sorted(
        [c for c in wide.columns if c != "date"],
        key=lambda c: float(c),
    )
    tenors = [float(c) for c in tenor_cols]
    n_tenors = len(tenors)

    if n_tenors == 0:
        return None

    # Cap components at the number of tenors
    k = min(n_components, n_tenors)

    # Drop rows with any missing tenors (require complete curves)
    wide_complete = wide.drop_nulls(subset=tenor_cols)

    # Need at least k+1 dates for a meaningful decomposition
    if wide_complete.height < k + 1:
        return None

    dates = wide_complete["date"].to_list()
    matrix = wide_complete.select(tenor_cols).to_numpy()  # (N, T)

    # Standardize: zero mean, unit variance per tenor
    col_mean = matrix.mean(axis=0)  # (T,)
    col_std = matrix.std(axis=0, ddof=0)  # (T,)

    # Replace zero std with 1.0 to avoid division by zero (constant tenor)
    col_std[col_std == 0.0] = 1.0

    standardized = (matrix - col_mean) / col_std  # (N, T)

    # Fit PCA
    pca = PCA(n_components=k)
    scores = pca.fit_transform(standardized)  # (N, K)

    # Build scores DataFrame — always include pc1, pc2, pc3 for schema compat
    scores_data: dict[str, list] = {
        "date": dates,
        "currency": [currency] * len(dates),
        "curve_type": [curve_type] * len(dates),
    }
    for i in range(3):
        col_name = f"pc{i + 1}"
        if i < k:
            scores_data[col_name] = scores[:, i].tolist()
        else:
            scores_data[col_name] = [0.0] * len(dates)

    scores_df = pl.DataFrame(scores_data).cast(PCA_SCORES_SCHEMA)

    return PCAResult(
        currency=currency,
        curve_type=curve_type,
        tenors=tenors,
        mean=col_mean,
        std=col_std,
        components=pca.components_,
        explained_variance=pca.explained_variance_ratio_,
        scores_df=scores_df,
    )


def fit_all_pca(
    curves_df: pl.DataFrame,
    n_components: int = 3,
) -> dict[str, PCAResult]:
    """Fit PCA for each currency present in the data.

    Calls ``fit_pca`` for every distinct currency. Currencies where PCA
    cannot be fitted (insufficient data) are silently skipped.

    Args:
        curves_df: DataFrame with columns ``date``, ``currency``,
            ``curve_type``, ``tenor_years``, ``yield_pct``.
        n_components: Maximum number of principal components per currency.

    Returns:
        Dict mapping currency code to its PCAResult.
    """
    if curves_df.is_empty():
        return {}

    currencies = curves_df["currency"].unique().sort().to_list()
    results: dict[str, PCAResult] = {}

    for ccy in currencies:
        result = fit_pca(curves_df, ccy, n_components=n_components)
        if result is not None:
            results[ccy] = result

    return results
