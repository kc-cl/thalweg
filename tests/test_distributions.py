"""Tests for the shock distribution and fan chart module."""

from __future__ import annotations

from datetime import date, timedelta

import numpy as np
import polars as pl
import pytest

from thalweg.analytics.distributions import (
    DEFAULT_HORIZONS,
    DEFAULT_QUANTILES,
    FAN_SCHEMA,
    compute_fan_chart,
    compute_shock_distribution,
)
from thalweg.analytics.pca import PCAResult, fit_pca


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

CURVE_INPUT_SCHEMA = {
    "date": pl.Date,
    "currency": pl.Utf8,
    "curve_type": pl.Utf8,
    "tenor_years": pl.Float64,
    "yield_pct": pl.Float64,
}


def _make_synthetic_curves(
    n_days: int = 300,
    tenors: list[float] | None = None,
    currency: str = "USD",
    curve_type: str = "govt_par",
    seed: int = 42,
) -> pl.DataFrame:
    """Create synthetic curve data with a strong level factor.

    Generates a random walk in the level factor plus a log-shaped
    term structure and small tenor-specific noise, producing data
    where PCA will find a dominant first component.

    Args:
        n_days: Number of observation dates.
        tenors: Ordered list of tenors. Defaults to [2, 5, 10, 30].
        currency: Currency code.
        curve_type: Curve type label.
        seed: Random seed for reproducibility.

    Returns:
        DataFrame matching the standard curve schema.
    """
    if tenors is None:
        tenors = [2.0, 5.0, 10.0, 30.0]

    rng = np.random.default_rng(seed)
    dates = [date(2024, 1, 1) + timedelta(days=i) for i in range(n_days)]
    level = np.cumsum(rng.normal(0, 0.01, n_days)) + 3.0

    rows: list[dict] = []
    for i, d in enumerate(dates):
        for t in tenors:
            yld = level[i] + 0.1 * np.log(t) + rng.normal(0, 0.02)
            rows.append({
                "date": d,
                "currency": currency,
                "curve_type": curve_type,
                "tenor_years": t,
                "yield_pct": yld,
            })

    return pl.DataFrame(rows).cast(CURVE_INPUT_SCHEMA)


def _fit_pca_for_tests(
    n_days: int = 300,
    tenors: list[float] | None = None,
    currency: str = "USD",
    seed: int = 42,
) -> PCAResult:
    """Build synthetic curves and fit PCA, returning the result.

    Raises AssertionError if PCA fitting fails (insufficient data).
    """
    curves = _make_synthetic_curves(
        n_days=n_days, tenors=tenors, currency=currency, seed=seed,
    )
    result = fit_pca(curves, currency, n_components=3)
    assert result is not None, "PCA fitting should succeed with synthetic data"
    return result


# ---------------------------------------------------------------------------
# Tests: compute_shock_distribution
# ---------------------------------------------------------------------------


class TestShockDistributionShape:
    """Verify output shape and schema of compute_shock_distribution."""

    def test_shock_distribution_shape(self) -> None:
        """Returns correct number of quantile x tenor rows."""
        pca_result = _fit_pca_for_tests(n_days=300)
        tenors = pca_result.tenors
        quantiles = DEFAULT_QUANTILES

        result = compute_shock_distribution(pca_result, horizon_days=21)

        expected_rows = len(quantiles) * len(tenors)
        assert result.height == expected_rows
        assert result.schema == FAN_SCHEMA

        # All tenors present for each quantile
        unique_tenors = sorted(result["tenor_years"].unique().to_list())
        assert unique_tenors == sorted(tenors)

        # All default quantiles present
        unique_quantiles = sorted(result["quantile"].unique().to_list())
        assert unique_quantiles == pytest.approx(sorted(quantiles))

    def test_shock_distribution_custom_quantiles(self) -> None:
        """Custom quantile list produces matching output rows."""
        pca_result = _fit_pca_for_tests(n_days=200)
        custom_q = [0.10, 0.50, 0.90]

        result = compute_shock_distribution(
            pca_result, horizon_days=21, quantiles=custom_q,
        )

        n_tenors = len(pca_result.tenors)
        assert result.height == len(custom_q) * n_tenors

    def test_shock_distribution_schema(self) -> None:
        """Output matches the FAN_SCHEMA exactly."""
        pca_result = _fit_pca_for_tests(n_days=200)
        result = compute_shock_distribution(pca_result, horizon_days=21)

        assert result.schema == FAN_SCHEMA
        assert result["currency"][0] == "USD"
        assert result["curve_type"][0] == "govt_par"
        assert result["horizon_days"][0] == 21


class TestShockDistributionOrdering:
    """Lower quantile yields should generally be below higher quantile yields."""

    def test_shock_distribution_ordering(self) -> None:
        """Lower quantile yields < higher quantile yields for most tenors."""
        pca_result = _fit_pca_for_tests(n_days=300)

        result = compute_shock_distribution(
            pca_result, horizon_days=21, quantiles=[0.05, 0.50, 0.95],
        )

        for tenor in pca_result.tenors:
            tenor_data = result.filter(pl.col("tenor_years") == tenor).sort("quantile")
            yields = tenor_data["yield_pct"].to_list()
            # q5 <= q50 <= q95
            assert yields[0] <= yields[1], (
                f"Tenor {tenor}: 5th pct ({yields[0]:.4f}) > "
                f"50th pct ({yields[1]:.4f})"
            )
            assert yields[1] <= yields[2], (
                f"Tenor {tenor}: 50th pct ({yields[1]:.4f}) > "
                f"95th pct ({yields[2]:.4f})"
            )


class TestShockDistributionMedian:
    """50th percentile should be close to the current curve at short horizons."""

    def test_shock_distribution_median_near_current(self) -> None:
        """50th percentile at 21-day horizon is approximately current yields.

        The median shock should be near zero (i.e., no change), so the
        projected 50th percentile curve should be close to today's curve
        reconstructed from PCA.
        """
        pca_result = _fit_pca_for_tests(n_days=300)

        result = compute_shock_distribution(
            pca_result, horizon_days=21, quantiles=[0.50],
        )

        # Reconstruct today's curve from PCA
        k = pca_result.components.shape[0]
        pc_cols = [f"pc{i + 1}" for i in range(k)]
        today_scores = (
            pca_result.scores_df.sort("date").select(pc_cols).to_numpy()[-1]
        )
        today_standardized = today_scores @ pca_result.components
        today_yields = today_standardized * pca_result.std + pca_result.mean

        # Compare median projected yields to today's yields
        for t_idx, tenor in enumerate(pca_result.tenors):
            row = result.filter(pl.col("tenor_years") == tenor)
            projected_yield = row["yield_pct"][0]
            current_yield = today_yields[t_idx]
            # Allow reasonable tolerance: median shock at 21 days should be
            # within ~50bp of current (generous for random walk data)
            assert abs(projected_yield - current_yield) < 0.50, (
                f"Tenor {tenor}: median projected {projected_yield:.4f} vs "
                f"current {current_yield:.4f} differ by > 50bp"
            )


# ---------------------------------------------------------------------------
# Tests: compute_fan_chart
# ---------------------------------------------------------------------------


class TestFanChartMultipleHorizons:
    """Fan chart should return data for each requested horizon."""

    def test_fan_chart_multiple_horizons(self) -> None:
        """Returns data for each horizon where sufficient data exists."""
        pca_result = _fit_pca_for_tests(n_days=300)
        curves_df = _make_synthetic_curves(n_days=300)

        horizons = [21, 63]
        result = compute_fan_chart(
            curves_df, pca_result, horizons=horizons,
        )

        unique_horizons = sorted(result["horizon_days"].unique().to_list())
        assert unique_horizons == horizons

        # Each horizon should have quantiles * tenors rows
        n_q = len(DEFAULT_QUANTILES)
        n_t = len(pca_result.tenors)
        for h in horizons:
            h_data = result.filter(pl.col("horizon_days") == h)
            assert h_data.height == n_q * n_t

    def test_fan_chart_default_horizons(self) -> None:
        """Default horizons are used when none specified.

        With 300 days of data, all default horizons (21, 63, 126, 252)
        should have enough observations.
        """
        pca_result = _fit_pca_for_tests(n_days=300)
        curves_df = _make_synthetic_curves(n_days=300)

        result = compute_fan_chart(curves_df, pca_result)

        unique_horizons = sorted(result["horizon_days"].unique().to_list())
        # 300 days supports horizons up to 252 (need h+1 = 253 <= 300)
        assert unique_horizons == sorted(DEFAULT_HORIZONS)


class TestFanChartWiderAtLongerHorizons:
    """Longer horizons should produce wider confidence bands."""

    def test_fan_chart_wider_at_longer_horizons(self) -> None:
        """6-month bands are wider than 1-month bands.

        The spread between high and low quantiles should increase with
        horizon length because cumulative shocks grow over time.
        Uses 1000 days so both horizons have ample delta samples.
        """
        pca_result = _fit_pca_for_tests(n_days=1000)
        curves_df = _make_synthetic_curves(n_days=1000)

        # Compare 21d vs 126d (both have plenty of delta samples)
        result = compute_fan_chart(
            curves_df, pca_result,
            horizons=[21, 126],
            quantiles=[0.05, 0.50, 0.95],
        )

        # For each tenor, check that 95-5 spread is wider at 126d than 21d
        wider_count = 0
        for tenor in pca_result.tenors:
            for h in [21, 126]:
                h_data = result.filter(
                    (pl.col("horizon_days") == h)
                    & (pl.col("tenor_years") == tenor)
                ).sort("quantile")
                if h == 21:
                    spread_short = (
                        h_data["yield_pct"][-1] - h_data["yield_pct"][0]
                    )
                else:
                    spread_long = (
                        h_data["yield_pct"][-1] - h_data["yield_pct"][0]
                    )

            if spread_long > spread_short:
                wider_count += 1

        # Most tenors should show wider bands at longer horizons
        n_tenors = len(pca_result.tenors)
        assert wider_count >= n_tenors // 2, (
            f"Only {wider_count}/{n_tenors} tenors have wider bands "
            f"at 126d vs 21d"
        )


# ---------------------------------------------------------------------------
# Tests: edge cases
# ---------------------------------------------------------------------------


class TestEmptyPcaResult:
    """Handles insufficient data gracefully."""

    def test_empty_pca_result(self) -> None:
        """PCA result with too few dates for horizon returns empty DataFrame."""
        # Create PCA result with only 30 dates, then request 63-day horizon
        pca_result = _fit_pca_for_tests(n_days=30)

        result = compute_shock_distribution(pca_result, horizon_days=63)

        assert result.is_empty()
        assert result.schema == FAN_SCHEMA

    def test_fan_chart_partial_horizons(self) -> None:
        """Fan chart skips horizons with insufficient data."""
        # 50 days of data: 21-day horizon works, 63-day does not
        pca_result = _fit_pca_for_tests(n_days=50)
        curves_df = _make_synthetic_curves(n_days=50)

        result = compute_fan_chart(
            curves_df, pca_result, horizons=[21, 63],
        )

        unique_horizons = result["horizon_days"].unique().to_list()
        assert 21 in unique_horizons
        assert 63 not in unique_horizons

    def test_fan_chart_all_horizons_insufficient(self) -> None:
        """Fan chart returns empty when no horizon has enough data."""
        pca_result = _fit_pca_for_tests(n_days=10)
        curves_df = _make_synthetic_curves(n_days=10)

        result = compute_fan_chart(
            curves_df, pca_result, horizons=[21, 63],
        )

        assert result.is_empty()
        assert result.schema == FAN_SCHEMA
