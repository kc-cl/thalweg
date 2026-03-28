"""Tests for the PCA decomposition module."""

from __future__ import annotations

from datetime import date, timedelta

import numpy as np
import polars as pl
import pytest

from thalweg.analytics.pca import (
    PCA_SCORES_SCHEMA,
    PCAResult,
    fit_all_pca,
    fit_pca,
)


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
    currency: str,
    tenors: list[float],
    num_days: int,
    start: date = date(2024, 1, 1),
    curve_type: str = "govt_par",
    base_yields: list[float] | None = None,
    seed: int = 42,
) -> pl.DataFrame:
    """Generate synthetic curve history with random perturbations.

    Each tenor gets a base yield plus a small random walk, so PCA will
    find meaningful structure.

    Args:
        currency: Currency code.
        tenors: List of tenor values.
        num_days: Number of observation dates.
        start: First observation date.
        curve_type: Curve type label.
        base_yields: Base yield per tenor (defaults to ascending from 2.0).
        seed: Random seed for reproducibility.

    Returns:
        DataFrame matching the standard curve schema.
    """
    rng = np.random.default_rng(seed)

    if base_yields is None:
        base_yields = [2.0 + 0.5 * i for i in range(len(tenors))]

    rows: list[dict] = []
    # Create a shared level factor that drives all tenors (for strong PC1)
    level_shocks = np.cumsum(rng.normal(0, 0.02, num_days))

    for day_idx in range(num_days):
        d = start + timedelta(days=day_idx)
        for tenor_idx, tenor in enumerate(tenors):
            yld = (
                base_yields[tenor_idx]
                + level_shocks[day_idx]
                + rng.normal(0, 0.01)  # tenor-specific noise
            )
            rows.append({
                "date": d,
                "currency": currency,
                "curve_type": curve_type,
                "tenor_years": tenor,
                "yield_pct": yld,
            })

    return pl.DataFrame(rows).cast(CURVE_INPUT_SCHEMA)


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestFitPcaBasic:
    """Basic PCA fitting tests."""

    def test_fit_pca_basic(self) -> None:
        """Synthetic 50-day curve with 4 tenors produces 3 PCs, variance sums < 1."""
        df = _make_synthetic_curves("USD", [2.0, 5.0, 10.0, 30.0], num_days=50)
        result = fit_pca(df, "USD", n_components=3)

        assert result is not None
        assert isinstance(result, PCAResult)
        assert result.currency == "USD"
        assert result.tenors == [2.0, 5.0, 10.0, 30.0]
        assert result.components.shape == (3, 4)
        assert result.explained_variance.shape == (3,)
        assert result.explained_variance.sum() < 1.0
        assert all(v > 0 for v in result.explained_variance)

    def test_pca_scores_shape(self) -> None:
        """Scores DataFrame has one row per date."""
        num_days = 50
        df = _make_synthetic_curves("USD", [2.0, 5.0, 10.0, 30.0], num_days=num_days)
        result = fit_pca(df, "USD")

        assert result is not None
        assert result.scores_df.height == num_days
        assert set(result.scores_df.columns) == {"date", "currency", "curve_type", "pc1", "pc2", "pc3"}
        assert result.scores_df.schema == PCA_SCORES_SCHEMA

    def test_pca_loadings_shape(self) -> None:
        """Components shape is (K, T) where K=n_components, T=n_tenors."""
        df = _make_synthetic_curves("CAD", [2.0, 5.0, 10.0, 30.0], num_days=50)
        result = fit_pca(df, "CAD", n_components=3)

        assert result is not None
        assert result.components.shape == (3, 4)

        # With n_components=2, should get (2, 4)
        result2 = fit_pca(df, "CAD", n_components=2)
        assert result2 is not None
        assert result2.components.shape == (2, 4)


class TestPcaReconstruction:
    """Verify that scores + loadings approximately reconstruct the input."""

    def test_pca_reconstruction(self) -> None:
        """Reconstruct standardized yields from scores and loadings within tolerance."""
        df = _make_synthetic_curves("USD", [2.0, 5.0, 10.0, 30.0], num_days=50)
        result = fit_pca(df, "USD", n_components=3)

        assert result is not None

        # Extract original wide matrix
        wide = df.pivot(on="tenor_years", index="date", values="yield_pct").sort("date")
        tenor_cols = sorted(
            [c for c in wide.columns if c != "date"],
            key=lambda c: float(c),
        )
        original = wide.select(tenor_cols).to_numpy()

        # Standardize using the stored mean/std
        standardized = (original - result.mean) / result.std

        # Reconstruct from scores and loadings
        scores = result.scores_df.select("pc1", "pc2", "pc3").to_numpy()
        # Only use the actual number of components fitted
        k = result.components.shape[0]
        scores_k = scores[:, :k]
        reconstructed = scores_k @ result.components

        # Reconstruction error should be small (PCA captures most variance)
        error = np.abs(standardized - reconstructed).max()
        # With 3 PCs and 4 tenors, the residual is at most 1 PC of variance
        assert error < 2.0, f"Max reconstruction error {error:.4f} too large"


class TestPcaExplainedVariance:
    """Variance ordering tests."""

    def test_pca_explained_variance_order(self) -> None:
        """PC1 explains more variance than PC2, PC2 more than PC3."""
        df = _make_synthetic_curves("USD", [2.0, 5.0, 10.0, 30.0], num_days=100)
        result = fit_pca(df, "USD", n_components=3)

        assert result is not None
        ev = result.explained_variance
        assert ev[0] >= ev[1], "PC1 should explain >= PC2 variance"
        assert ev[1] >= ev[2], "PC2 should explain >= PC3 variance"


class TestFitAllPca:
    """Tests for multi-currency PCA fitting."""

    def test_fit_all_pca(self) -> None:
        """Fits PCA independently for multiple currencies."""
        df_usd = _make_synthetic_curves("USD", [2.0, 5.0, 10.0, 30.0], num_days=50, seed=1)
        df_cad = _make_synthetic_curves("CAD", [2.0, 5.0, 10.0, 30.0], num_days=50, seed=2)
        combined = pl.concat([df_usd, df_cad])

        results = fit_all_pca(combined, n_components=3)

        assert "USD" in results
        assert "CAD" in results
        assert results["USD"].currency == "USD"
        assert results["CAD"].currency == "CAD"
        # Each should have its own scores
        assert results["USD"].scores_df.height == 50
        assert results["CAD"].scores_df.height == 50


class TestPcaEdgeCases:
    """Edge case and boundary tests."""

    def test_pca_empty_input(self) -> None:
        """Empty DataFrame returns None."""
        empty = pl.DataFrame(schema=CURVE_INPUT_SCHEMA)
        result = fit_pca(empty, "USD")
        assert result is None

    def test_fit_all_pca_empty(self) -> None:
        """Empty DataFrame returns empty dict."""
        empty = pl.DataFrame(schema=CURVE_INPUT_SCHEMA)
        results = fit_all_pca(empty)
        assert results == {}

    def test_pca_nonexistent_currency(self) -> None:
        """Currency not in data returns None."""
        df = _make_synthetic_curves("USD", [2.0, 5.0, 10.0], num_days=50)
        result = fit_pca(df, "EUR")
        assert result is None

    def test_pca_insufficient_dates(self) -> None:
        """Fewer than n_components+1 dates returns None."""
        df = _make_synthetic_curves("USD", [2.0, 5.0, 10.0, 30.0], num_days=3)
        # n_components=3, need at least 4 dates
        result = fit_pca(df, "USD", n_components=3)
        assert result is None

    def test_pca_sparse_currency(self) -> None:
        """GBP with only 3 tenors caps at 3 PCs."""
        df = _make_synthetic_curves("GBP", [5.0, 10.0, 20.0], num_days=50)
        result = fit_pca(df, "GBP", n_components=3)

        assert result is not None
        assert result.tenors == [5.0, 10.0, 20.0]
        assert result.components.shape == (3, 3)
        assert result.explained_variance.shape == (3,)
        # Variance should sum to exactly 1.0 when K == T
        assert result.explained_variance.sum() == pytest.approx(1.0)
        # scores_df should still have pc1, pc2, pc3 columns
        assert "pc3" in result.scores_df.columns

    def test_pca_two_tenors(self) -> None:
        """Currency with 2 tenors and n_components=3 caps at 2 PCs."""
        df = _make_synthetic_curves("EUR", [5.0, 10.0], num_days=50)
        result = fit_pca(df, "EUR", n_components=3)

        assert result is not None
        assert result.components.shape == (2, 2)
        assert result.explained_variance.shape == (2,)
        # pc3 should be filled with zeros
        assert result.scores_df["pc3"].to_list() == [0.0] * 50

    def test_pca_constant_tenor(self) -> None:
        """A tenor with zero variance should not cause division by zero."""
        # Create data where one tenor is constant
        rows: list[dict] = []
        for i in range(20):
            d = date(2024, 1, 1) + timedelta(days=i)
            rows.append({"date": d, "currency": "USD", "curve_type": "govt_par",
                         "tenor_years": 2.0, "yield_pct": 3.0})  # constant
            rows.append({"date": d, "currency": "USD", "curve_type": "govt_par",
                         "tenor_years": 10.0, "yield_pct": 4.0 + 0.01 * i})
        df = pl.DataFrame(rows).cast(CURVE_INPUT_SCHEMA)

        result = fit_pca(df, "USD", n_components=2)
        assert result is not None
        # std for the constant tenor should have been replaced with 1.0
        assert result.std[0] == 1.0

    def test_fit_all_pca_skips_insufficient(self) -> None:
        """fit_all_pca skips currencies where fit_pca returns None."""
        df_usd = _make_synthetic_curves("USD", [2.0, 5.0, 10.0], num_days=50, seed=1)
        # EUR with only 2 dates: too few for 3 PCs
        df_eur = _make_synthetic_curves("EUR", [2.0, 5.0, 10.0], num_days=2, seed=2)
        combined = pl.concat([df_usd, df_eur])

        results = fit_all_pca(combined, n_components=3)
        assert "USD" in results
        assert "EUR" not in results
