"""Analytics module for yield curve derived metrics."""

from thalweg.analytics.pca import fit_all_pca, fit_pca
from thalweg.analytics.regimes import classify_regimes
from thalweg.analytics.spreads import (
    compute_cross_market_spreads,
    compute_curvature,
    compute_slopes,
)

__all__ = [
    "classify_regimes",
    "compute_cross_market_spreads",
    "compute_curvature",
    "compute_slopes",
    "fit_all_pca",
    "fit_pca",
]
