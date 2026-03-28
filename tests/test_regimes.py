"""Tests for the regime classification module."""

from __future__ import annotations

from datetime import date, timedelta

import polars as pl
import pytest

from thalweg.analytics.regimes import REGIME_SCHEMA, classify_regimes


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_curves(
    obs_date: date,
    tenors_by_currency: dict[str, dict[float, float]],
    curve_type: str = "govt_par",
) -> pl.DataFrame:
    """Create multi-currency curve data for a single date."""
    rows: list[dict] = []
    for ccy, tenors in tenors_by_currency.items():
        for tenor, yld in tenors.items():
            rows.append({
                "date": obs_date,
                "currency": ccy,
                "curve_type": curve_type,
                "tenor_years": tenor,
                "yield_pct": yld,
            })
    return pl.DataFrame(rows).cast(
        {"date": pl.Date, "tenor_years": pl.Float64, "yield_pct": pl.Float64}
    )


def _make_history(
    currency: str,
    start: date,
    num_days: int,
    short_tenor: float,
    short_yields: list[float],
    ten_yr_yields: list[float],
    curve_type: str = "govt_par",
) -> pl.DataFrame:
    """Create a time series of curve observations for testing dynamic regimes.

    Args:
        currency: Currency code.
        start: Starting date.
        num_days: Number of business days.
        short_tenor: Short-end tenor (2.0 or 5.0).
        short_yields: List of short-tenor yields, one per day.
        ten_yr_yields: List of 10yr yields, one per day.
        curve_type: Curve type string.

    Returns:
        DataFrame with two tenors per date over the history.
    """
    rows: list[dict] = []
    d = start
    for i in range(num_days):
        rows.append({
            "date": d,
            "currency": currency,
            "curve_type": curve_type,
            "tenor_years": short_tenor,
            "yield_pct": short_yields[i],
        })
        rows.append({
            "date": d,
            "currency": currency,
            "curve_type": curve_type,
            "tenor_years": 10.0,
            "yield_pct": ten_yr_yields[i],
        })
        d += timedelta(days=1)
    return pl.DataFrame(rows).cast(
        {"date": pl.Date, "tenor_years": pl.Float64, "yield_pct": pl.Float64}
    )


# ---------------------------------------------------------------------------
# Static regime tests
# ---------------------------------------------------------------------------


class TestStaticRegimes:
    """Tests for regimes determined by current slope value only."""

    def test_inverted_regime(self) -> None:
        """2s10s < -25bp should classify as inverted."""
        df = _make_curves(
            date(2024, 1, 15),
            {"USD": {2.0: 5.0, 10.0: 4.5}},  # slope = -50bp
        )
        result = classify_regimes(df)

        assert result.shape[0] == 1
        assert result["regime"][0] == "inverted"
        assert result["slope_2s10s_bp"][0] == pytest.approx(-50.0)
        assert result["level_10y"][0] == pytest.approx(4.5)

    def test_flat_regime(self) -> None:
        """|2s10s| < 25bp should classify as flat."""
        df = _make_curves(
            date(2024, 1, 15),
            {"USD": {2.0: 4.0, 10.0: 4.2}},  # slope = +20bp
        )
        result = classify_regimes(df)

        assert result.shape[0] == 1
        assert result["regime"][0] == "flat"

    def test_normal_steep(self) -> None:
        """2s10s > 100bp with level < median should classify as normal_steep."""
        # Single date: level IS the median, so we need level < median.
        # Use two dates: first with high level, second with low level + steep slope.
        df = pl.concat([
            _make_curves(date(2024, 1, 10), {"USD": {2.0: 5.0, 10.0: 6.0}}),
            _make_curves(date(2024, 1, 15), {"USD": {2.0: 2.0, 10.0: 4.0}}),
        ])
        # Median of 10yr [6.0, 4.0] = 5.0, current level 4.0 < 5.0, slope = 200bp
        result = classify_regimes(df)
        row = result.filter(pl.col("date") == date(2024, 1, 15))

        assert row.shape[0] == 1
        assert row["regime"][0] == "normal_steep"

    def test_normal_fallback(self) -> None:
        """Moderate slope that doesn't match any specific rule → normal."""
        df = _make_curves(
            date(2024, 1, 15),
            {"USD": {2.0: 3.5, 10.0: 4.0}},  # slope = +50bp, level = 4.0
        )
        result = classify_regimes(df)

        assert result.shape[0] == 1
        assert result["regime"][0] == "normal"


# ---------------------------------------------------------------------------
# Dynamic regime tests
# ---------------------------------------------------------------------------


class TestDynamicRegimes:
    """Tests for regimes requiring lookback history."""

    def test_bear_steep(self) -> None:
        """Rising level + rising slope → bear_steep."""
        n = 25
        # 10yr rises from 3.0 to 4.0, 2yr rises from 2.5 to 3.0
        # slope goes from 50bp to 100bp (rising)
        ten_yr = [3.0 + (1.0 * i / (n - 1)) for i in range(n)]
        two_yr = [2.5 + (0.5 * i / (n - 1)) for i in range(n)]
        df = _make_history("USD", date(2024, 1, 1), n, 2.0, two_yr, ten_yr)

        result = classify_regimes(df, lookback_days=20)
        last = result.filter(pl.col("date") == date(2024, 1, 25))

        assert last.shape[0] == 1
        assert last["regime"][0] == "bear_steep"

    def test_bull_flat(self) -> None:
        """Falling level + falling slope → bull_flat."""
        n = 25
        # 10yr falls from 5.0 to 4.0, 2yr falls from 4.0 to 3.5
        # slope goes from 100bp to 50bp (falling)
        ten_yr = [5.0 - (1.0 * i / (n - 1)) for i in range(n)]
        two_yr = [4.0 - (0.5 * i / (n - 1)) for i in range(n)]
        df = _make_history("USD", date(2024, 1, 1), n, 2.0, two_yr, ten_yr)

        result = classify_regimes(df, lookback_days=20)
        last = result.filter(pl.col("date") == date(2024, 1, 25))

        assert last.shape[0] == 1
        assert last["regime"][0] == "bull_flat"

    def test_bear_flat(self) -> None:
        """Rising level + falling slope → bear_flat."""
        n = 25
        # 10yr rises from 3.0 to 4.0, 2yr rises from 1.5 to 3.5
        # slope goes from 150bp to 50bp (falling, but stays above flat threshold)
        ten_yr = [3.0 + (1.0 * i / (n - 1)) for i in range(n)]
        two_yr = [1.5 + (2.0 * i / (n - 1)) for i in range(n)]
        df = _make_history("USD", date(2024, 1, 1), n, 2.0, two_yr, ten_yr)

        result = classify_regimes(df, lookback_days=20)
        last = result.filter(pl.col("date") == date(2024, 1, 25))

        assert last.shape[0] == 1
        assert last["regime"][0] == "bear_flat"

    def test_bull_steep(self) -> None:
        """Falling level + rising slope → bull_steep."""
        n = 25
        # 10yr falls from 5.0 to 4.5, 2yr falls from 4.5 to 3.0
        # slope goes from 50bp to 150bp (rising)
        ten_yr = [5.0 - (0.5 * i / (n - 1)) for i in range(n)]
        two_yr = [4.5 - (1.5 * i / (n - 1)) for i in range(n)]
        df = _make_history("USD", date(2024, 1, 1), n, 2.0, two_yr, ten_yr)

        result = classify_regimes(df, lookback_days=20)
        last = result.filter(pl.col("date") == date(2024, 1, 25))

        assert last.shape[0] == 1
        assert last["regime"][0] == "bull_steep"


# ---------------------------------------------------------------------------
# Edge cases
# ---------------------------------------------------------------------------


class TestEdgeCases:
    """Edge case tests for regime classification."""

    def test_empty_input(self) -> None:
        """Empty DataFrame returns empty result with correct schema."""
        empty = pl.DataFrame(schema={
            "date": pl.Date,
            "currency": pl.Utf8,
            "curve_type": pl.Utf8,
            "tenor_years": pl.Float64,
            "yield_pct": pl.Float64,
        })
        result = classify_regimes(empty)
        assert result.is_empty()
        assert result.schema == REGIME_SCHEMA

    def test_insufficient_history(self) -> None:
        """With < lookback_days of history, only static rules apply."""
        # 5 days of data with moderate slope (not inverted/flat) — should get
        # static classification (normal or normal_steep) not dynamic
        n = 5
        ten_yr = [3.0 + (0.5 * i / (n - 1)) for i in range(n)]
        two_yr = [2.0 + (0.1 * i / (n - 1)) for i in range(n)]
        df = _make_history("USD", date(2024, 1, 1), n, 2.0, two_yr, ten_yr)

        result = classify_regimes(df, lookback_days=20)
        # All rows should exist but none should be dynamic regimes
        dynamic = {"bear_steep", "bull_flat", "bear_flat", "bull_steep"}
        for regime in result["regime"].to_list():
            assert regime not in dynamic

    def test_gbp_5s10s_fallback(self) -> None:
        """GBP without 2yr tenor uses 5s10s with adjusted thresholds."""
        # GBP with 5/10/20yr tenors, 5s10s slope = -20bp
        # With GBP thresholds: inverted < -15bp
        df = _make_curves(
            date(2024, 1, 15),
            {"GBP": {5.0: 4.5, 10.0: 4.3, 20.0: 4.0}},
        )
        result = classify_regimes(df)

        assert result.shape[0] == 1
        assert result["currency"][0] == "GBP"
        assert result["regime"][0] == "inverted"
        # slope_2s10s_bp column stores whatever slope was used
        assert result["slope_2s10s_bp"][0] == pytest.approx(-20.0)

    def test_multiple_currencies(self) -> None:
        """Multiple currencies are classified independently."""
        df = _make_curves(
            date(2024, 1, 15),
            {
                "USD": {2.0: 5.0, 10.0: 4.5},  # inverted
                "CAD": {2.0: 3.5, 10.0: 4.0},  # normal (+50bp)
            },
        )
        result = classify_regimes(df)

        assert result.shape[0] == 2
        regimes = dict(zip(
            result["currency"].to_list(),
            result["regime"].to_list(),
        ))
        assert regimes["USD"] == "inverted"
        assert regimes["CAD"] == "normal"
