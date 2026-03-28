"""Analytics module for yield curve derived metrics."""

from thalweg.analytics.spreads import (
    compute_cross_market_spreads,
    compute_curvature,
    compute_slopes,
)

__all__ = ["compute_slopes", "compute_curvature", "compute_cross_market_spreads"]
