"""Tests for the analytics module (slopes, curvature, cross-market spreads)."""

from __future__ import annotations

from datetime import date

import polars as pl
import pytest

from thalweg.analytics.spreads import (
    CROSS_MARKET_SCHEMA,
    CURVATURE_SCHEMA,
    SLOPES_SCHEMA,
    compute_cross_market_spreads,
    compute_curvature,
    compute_slopes,
)


# ---------------------------------------------------------------------------
# Helper
# ---------------------------------------------------------------------------


def _make_curves(
    obs_date: date,
    tenors_by_currency: dict[str, dict[float, float]],
) -> pl.DataFrame:
    """Create multi-currency curve data for testing.

    Args:
        obs_date: Observation date.
        tenors_by_currency: Mapping like
            ``{"CAD": {2.0: 3.5, 5.0: 3.7, 10.0: 3.9, 30.0: 4.2}, ...}``.

    Returns:
        DataFrame matching the standard curve schema.
    """
    rows: list[dict] = []
    for ccy, tenors in tenors_by_currency.items():
        for tenor, yld in tenors.items():
            rows.append(
                {
                    "date": obs_date,
                    "currency": ccy,
                    "curve_type": "govt_par",
                    "tenor_years": tenor,
                    "yield_pct": yld,
                }
            )
    return pl.DataFrame(rows).cast(
        {"date": pl.Date, "tenor_years": pl.Float64, "yield_pct": pl.Float64}
    )


# ---------------------------------------------------------------------------
# Slopes
# ---------------------------------------------------------------------------


class TestComputeSlopes:
    """Tests for compute_slopes."""

    def test_compute_slopes_basic(self) -> None:
        """Single date, single currency with 2/5/10/30yr tenors."""
        df = _make_curves(
            date(2024, 1, 15),
            {"CAD": {2.0: 3.50, 5.0: 3.70, 10.0: 3.90, 30.0: 4.20}},
        )
        result = compute_slopes(df)

        assert result.shape[0] == 3
        assert set(result["slope_name"].to_list()) == {"2s10s", "2s30s", "5s30s"}

        slopes = dict(
            zip(result["slope_name"].to_list(), result["value_bp"].to_list())
        )
        assert slopes["2s10s"] == pytest.approx((3.90 - 3.50) * 100)
        assert slopes["2s30s"] == pytest.approx((4.20 - 3.50) * 100)
        assert slopes["5s30s"] == pytest.approx((4.20 - 3.70) * 100)

    def test_compute_slopes_multiple_currencies(self) -> None:
        """Two currencies should both appear in output."""
        df = _make_curves(
            date(2024, 1, 15),
            {
                "CAD": {2.0: 3.50, 5.0: 3.70, 10.0: 3.90, 30.0: 4.20},
                "USD": {2.0: 4.00, 5.0: 4.10, 10.0: 4.30, 30.0: 4.60},
            },
        )
        result = compute_slopes(df)

        currencies = result["currency"].unique().to_list()
        assert sorted(currencies) == ["CAD", "USD"]
        # 3 slopes per currency
        assert result.shape[0] == 6

    def test_compute_slopes_missing_tenor(self) -> None:
        """Currency with only 5/10/20yr tenors skips 2s10s, 2s30s, 5s30s."""
        df = _make_curves(
            date(2024, 1, 15),
            {"GBP": {5.0: 4.00, 10.0: 4.30, 20.0: 4.60}},
        )
        # Default pairs (2,10), (2,30), (5,30) -- all require 2yr or 30yr
        result = compute_slopes(df)
        assert result.is_empty()

        # But a custom pair that fits should work
        result2 = compute_slopes(df, pairs=[(5, 10)])
        assert result2.shape[0] == 1
        assert result2["slope_name"][0] == "5s10s"
        assert result2["value_bp"][0] == pytest.approx((4.30 - 4.00) * 100)

    def test_compute_slopes_custom_pairs(self) -> None:
        """Custom pairs=[(5,10)] should produce only that slope."""
        df = _make_curves(
            date(2024, 1, 15),
            {"CAD": {2.0: 3.50, 5.0: 3.70, 10.0: 3.90, 30.0: 4.20}},
        )
        result = compute_slopes(df, pairs=[(5, 10)])

        assert result.shape[0] == 1
        assert result["slope_name"][0] == "5s10s"
        assert result["value_bp"][0] == pytest.approx((3.90 - 3.70) * 100)


# ---------------------------------------------------------------------------
# Curvature
# ---------------------------------------------------------------------------


class TestComputeCurvature:
    """Tests for compute_curvature."""

    def test_compute_curvature_basic(self) -> None:
        """Verify 2s5s10s and 5s10s30s formulas."""
        df = _make_curves(
            date(2024, 1, 15),
            {"CAD": {2.0: 3.50, 5.0: 3.70, 10.0: 3.90, 30.0: 4.20}},
        )
        result = compute_curvature(df)

        assert result.shape[0] == 2
        bflies = dict(
            zip(
                result["butterfly_name"].to_list(),
                result["value_bp"].to_list(),
            )
        )

        expected_2s5s10s = (2 * 3.70 - 3.50 - 3.90) * 100
        expected_5s10s30s = (2 * 3.90 - 3.70 - 4.20) * 100

        assert bflies["2s5s10s"] == pytest.approx(expected_2s5s10s)
        assert bflies["5s10s30s"] == pytest.approx(expected_5s10s30s)

    def test_compute_curvature_missing_tenor(self) -> None:
        """Currency missing 2yr tenor should skip 2s5s10s but keep 5s10s30s if available."""
        df = _make_curves(
            date(2024, 1, 15),
            {"USD": {5.0: 4.10, 10.0: 4.30, 30.0: 4.60}},
        )
        result = compute_curvature(df)

        assert result.shape[0] == 1
        assert result["butterfly_name"][0] == "5s10s30s"
        expected = (2 * 4.30 - 4.10 - 4.60) * 100
        assert result["value_bp"][0] == pytest.approx(expected)


# ---------------------------------------------------------------------------
# Cross-market spreads
# ---------------------------------------------------------------------------


class TestComputeCrossMarketSpreads:
    """Tests for compute_cross_market_spreads."""

    def test_compute_cross_market_spreads_basic(self) -> None:
        """Two currencies with overlapping tenors."""
        df = _make_curves(
            date(2024, 1, 15),
            {
                "USD": {2.0: 4.00, 5.0: 4.10, 10.0: 4.30},
                "CAD": {2.0: 3.50, 5.0: 3.70, 10.0: 3.90},
            },
        )
        result = compute_cross_market_spreads(df, pairs=[("USD", "CAD")])

        assert result.shape[0] == 3
        assert (result["pair"] == "USD-CAD").all()

        spreads = dict(
            zip(
                result["tenor_years"].to_list(),
                result["spread_bp"].to_list(),
            )
        )
        assert spreads[2.0] == pytest.approx((4.00 - 3.50) * 100)
        assert spreads[5.0] == pytest.approx((4.10 - 3.70) * 100)
        assert spreads[10.0] == pytest.approx((4.30 - 3.90) * 100)

    def test_compute_cross_market_spreads_partial_overlap(self) -> None:
        """Currencies with different tenor sets produce only matched tenors."""
        df = _make_curves(
            date(2024, 1, 15),
            {
                "USD": {2.0: 4.00, 5.0: 4.10, 10.0: 4.30, 30.0: 4.60},
                "GBP": {5.0: 4.00, 10.0: 4.30, 20.0: 4.50},
            },
        )
        result = compute_cross_market_spreads(df, pairs=[("USD", "GBP")])

        # Only 5yr and 10yr are in both curves
        assert result.shape[0] == 2
        tenors = sorted(result["tenor_years"].to_list())
        assert tenors == [5.0, 10.0]

    def test_compute_cross_market_spreads_no_overlap_date(self) -> None:
        """One currency on a different date produces no spreads."""
        rows_usd = _make_curves(
            date(2024, 1, 15),
            {"USD": {2.0: 4.00, 10.0: 4.30}},
        )
        rows_cad = _make_curves(
            date(2024, 1, 16),
            {"CAD": {2.0: 3.50, 10.0: 3.90}},
        )
        df = pl.concat([rows_usd, rows_cad])

        result = compute_cross_market_spreads(df, pairs=[("USD", "CAD")])
        assert result.is_empty()


# ---------------------------------------------------------------------------
# Edge cases
# ---------------------------------------------------------------------------


class TestEmptyInput:
    """All three functions should handle empty input gracefully."""

    def test_empty_slopes(self) -> None:
        empty = pl.DataFrame(schema={
            "date": pl.Date,
            "currency": pl.Utf8,
            "curve_type": pl.Utf8,
            "tenor_years": pl.Float64,
            "yield_pct": pl.Float64,
        })
        result = compute_slopes(empty)
        assert result.is_empty()
        assert result.schema == SLOPES_SCHEMA

    def test_empty_curvature(self) -> None:
        empty = pl.DataFrame(schema={
            "date": pl.Date,
            "currency": pl.Utf8,
            "curve_type": pl.Utf8,
            "tenor_years": pl.Float64,
            "yield_pct": pl.Float64,
        })
        result = compute_curvature(empty)
        assert result.is_empty()
        assert result.schema == CURVATURE_SCHEMA

    def test_empty_cross_market(self) -> None:
        empty = pl.DataFrame(schema={
            "date": pl.Date,
            "currency": pl.Utf8,
            "curve_type": pl.Utf8,
            "tenor_years": pl.Float64,
            "yield_pct": pl.Float64,
        })
        result = compute_cross_market_spreads(empty)
        assert result.is_empty()
        assert result.schema == CROSS_MARKET_SCHEMA
