"""Tests for the analog forecasting module."""

from __future__ import annotations

from datetime import date, timedelta

import numpy as np
import polars as pl
import pytest

from thalweg.analytics.forecasts import (
    ANALOG_SCHEMA,
    FORECAST_SCHEMA,
    _DEFAULT_BUFFER_DAYS,
    find_analogs,
    forecast_from_analogs,
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
    currency: str = "USD",
    tenors: list[float] | None = None,
    num_days: int = 200,
    start: date = date(2023, 1, 1),
    curve_type: str = "govt_par",
    seed: int = 42,
) -> pl.DataFrame:
    """Generate synthetic curve history with random perturbations.

    Creates a panel of yield curves with a shared level factor (strong
    PC1) and tenor-specific noise so PCA finds meaningful structure.
    """
    if tenors is None:
        tenors = [2.0, 5.0, 10.0, 30.0]

    rng = np.random.default_rng(seed)
    base_yields = [2.0 + 0.5 * i for i in range(len(tenors))]

    rows: list[dict] = []
    level_shocks = np.cumsum(rng.normal(0, 0.02, num_days))

    for day_idx in range(num_days):
        d = start + timedelta(days=day_idx)
        for tenor_idx, tenor in enumerate(tenors):
            yld = (
                base_yields[tenor_idx]
                + level_shocks[day_idx]
                + rng.normal(0, 0.01)
            )
            rows.append({
                "date": d,
                "currency": currency,
                "curve_type": curve_type,
                "tenor_years": tenor,
                "yield_pct": yld,
            })

    return pl.DataFrame(rows).cast(CURVE_INPUT_SCHEMA)


@pytest.fixture()
def synth_data() -> tuple[pl.DataFrame, PCAResult]:
    """Return (curves_df, pca_result) for 200-day USD synthetic history."""
    curves = _make_synthetic_curves(num_days=200)
    result = fit_pca(curves, "USD", n_components=3)
    assert result is not None
    return curves, result


# ---------------------------------------------------------------------------
# Tests — find_analogs
# ---------------------------------------------------------------------------


class TestFindAnalogsCount:
    """find_analogs returns the expected number of analogs."""

    def test_find_analogs_count(self, synth_data: tuple[pl.DataFrame, PCAResult]) -> None:
        """Returns exactly K analogs when enough data is available."""
        _, pca_result = synth_data
        k = 10
        analogs = find_analogs(pca_result, k=k)

        assert analogs.height == k
        assert set(analogs.columns) == set(ANALOG_SCHEMA.keys())

    def test_find_analogs_fewer_than_k(self) -> None:
        """Returns fewer than K if not enough candidate dates exist."""
        curves = _make_synthetic_curves(num_days=30)
        result = fit_pca(curves, "USD", n_components=3)
        assert result is not None

        # With 30 days and a 21-day buffer, very few candidates remain
        analogs = find_analogs(result, k=50)
        assert analogs.height < 50
        assert analogs.height > 0


class TestFindAnalogsSorted:
    """find_analogs results are sorted by distance ascending."""

    def test_find_analogs_sorted_by_distance(
        self, synth_data: tuple[pl.DataFrame, PCAResult]
    ) -> None:
        """Distance values are non-decreasing."""
        _, pca_result = synth_data
        analogs = find_analogs(pca_result, k=20)

        distances = analogs["distance"].to_list()
        assert distances == sorted(distances)
        assert all(d >= 0 for d in distances)


class TestAnalogExcludesRecent:
    """find_analogs excludes dates within the buffer of the target."""

    def test_analog_excludes_recent(
        self, synth_data: tuple[pl.DataFrame, PCAResult]
    ) -> None:
        """No analog is within buffer_days of the target date."""
        _, pca_result = synth_data
        target = pca_result.scores_df["date"].max()
        buffer = _DEFAULT_BUFFER_DAYS

        analogs = find_analogs(pca_result, target_date=target, k=20, buffer_days=buffer)

        for analog_date in analogs["analog_date"].to_list():
            gap = abs((target - analog_date).days)
            assert gap > buffer, (
                f"Analog {analog_date} is only {gap} days from target {target}"
            )

    def test_custom_buffer(
        self, synth_data: tuple[pl.DataFrame, PCAResult]
    ) -> None:
        """A larger buffer excludes more dates near the target."""
        _, pca_result = synth_data
        target = pca_result.scores_df["date"].max()

        analogs_small = find_analogs(pca_result, target_date=target, k=50, buffer_days=10)
        analogs_large = find_analogs(pca_result, target_date=target, k=50, buffer_days=60)

        # Larger buffer should leave fewer (or equal) candidates
        assert analogs_large.height <= analogs_small.height


class TestFindAnalogsTargetDate:
    """find_analogs handles explicit and default target dates."""

    def test_explicit_target_date(
        self, synth_data: tuple[pl.DataFrame, PCAResult]
    ) -> None:
        """Specifying a known date in the middle of the range works."""
        _, pca_result = synth_data
        all_dates = pca_result.scores_df["date"].sort().to_list()
        mid = all_dates[len(all_dates) // 2]

        analogs = find_analogs(pca_result, target_date=mid, k=10)
        assert analogs.height == 10
        # Target itself should never appear in the results
        assert mid not in analogs["analog_date"].to_list()

    def test_missing_target_returns_empty(
        self, synth_data: tuple[pl.DataFrame, PCAResult]
    ) -> None:
        """A target date not in the data returns an empty frame."""
        _, pca_result = synth_data
        analogs = find_analogs(pca_result, target_date=date(1900, 1, 1), k=10)
        assert analogs.is_empty()
        assert analogs.schema == ANALOG_SCHEMA


# ---------------------------------------------------------------------------
# Tests — forecast_from_analogs
# ---------------------------------------------------------------------------


class TestForecastHasFutureCurves:
    """forecast_from_analogs returns future curves for each analog."""

    def test_forecast_has_future_curves(
        self, synth_data: tuple[pl.DataFrame, PCAResult]
    ) -> None:
        """Each analog should have curves at t+horizon in the result."""
        curves, pca_result = synth_data

        # Use a target early enough that analogs have futures in the data
        all_dates = pca_result.scores_df["date"].sort().to_list()
        target = all_dates[len(all_dates) // 2]

        fc = forecast_from_analogs(
            curves, pca_result, target_date=target, k=5, horizon_days=30,
        )

        assert not fc.is_empty()
        assert set(fc.columns) == set(FORECAST_SCHEMA.keys())

        # Individual forecasts (not median) should exist
        individuals = fc.filter(pl.col("is_median") == False)  # noqa: E712
        assert individuals.height > 0

        # Each analog_date in individuals should map to a future_date
        for row in individuals.iter_rows(named=True):
            gap = (row["future_date"] - row["analog_date"]).days
            # Should be close to horizon_days (+/- date_tolerance)
            assert abs(gap - 30) <= 5


class TestForecastMedianComputed:
    """forecast_from_analogs produces median rows."""

    def test_forecast_median_computed(
        self, synth_data: tuple[pl.DataFrame, PCAResult]
    ) -> None:
        """Rows with is_median=True exist, one per tenor."""
        curves, pca_result = synth_data
        all_dates = pca_result.scores_df["date"].sort().to_list()
        target = all_dates[len(all_dates) // 2]

        fc = forecast_from_analogs(
            curves, pca_result, target_date=target, k=5, horizon_days=30,
        )

        median_rows = fc.filter(pl.col("is_median") == True)  # noqa: E712
        assert not median_rows.is_empty()

        # Should have one median row per tenor
        median_tenors = sorted(median_rows["tenor_years"].to_list())
        expected_tenors = sorted(pca_result.tenors)
        assert median_tenors == expected_tenors

    def test_median_is_between_min_and_max(
        self, synth_data: tuple[pl.DataFrame, PCAResult]
    ) -> None:
        """Median yield for each tenor lies between the individual min and max."""
        curves, pca_result = synth_data
        all_dates = pca_result.scores_df["date"].sort().to_list()
        target = all_dates[len(all_dates) // 2]

        fc = forecast_from_analogs(
            curves, pca_result, target_date=target, k=5, horizon_days=30,
        )

        individuals = fc.filter(pl.col("is_median") == False)  # noqa: E712
        medians = fc.filter(pl.col("is_median") == True)  # noqa: E712

        for tenor in pca_result.tenors:
            ind_yields = individuals.filter(
                pl.col("tenor_years") == tenor
            )["yield_pct"]
            if ind_yields.is_empty():
                continue
            med_yield = medians.filter(
                pl.col("tenor_years") == tenor
            )["yield_pct"][0]

            assert med_yield >= ind_yields.min() - 1e-9
            assert med_yield <= ind_yields.max() + 1e-9


# ---------------------------------------------------------------------------
# Tests — empty / edge cases
# ---------------------------------------------------------------------------


class TestEmptyData:
    """Functions return empty DataFrames with correct schema on invalid input."""

    def test_find_analogs_empty_scores(self) -> None:
        """Empty scores_df returns empty analog frame."""
        empty_scores = pl.DataFrame(schema={
            "date": pl.Date, "currency": pl.Utf8,
            "curve_type": pl.Utf8, "pc1": pl.Float64,
            "pc2": pl.Float64, "pc3": pl.Float64,
        })
        pca_result = PCAResult(
            currency="USD",
            curve_type="govt_par",
            tenors=[2.0, 5.0, 10.0],
            mean=np.zeros(3),
            std=np.ones(3),
            components=np.eye(3),
            explained_variance=np.array([0.8, 0.15, 0.05]),
            scores_df=empty_scores,
        )

        analogs = find_analogs(pca_result, k=10)
        assert analogs.is_empty()
        assert analogs.schema == ANALOG_SCHEMA

    def test_forecast_empty_scores(self) -> None:
        """Empty scores_df returns empty forecast frame."""
        empty_scores = pl.DataFrame(schema={
            "date": pl.Date, "currency": pl.Utf8,
            "curve_type": pl.Utf8, "pc1": pl.Float64,
            "pc2": pl.Float64, "pc3": pl.Float64,
        })
        pca_result = PCAResult(
            currency="USD",
            curve_type="govt_par",
            tenors=[2.0, 5.0, 10.0],
            mean=np.zeros(3),
            std=np.ones(3),
            components=np.eye(3),
            explained_variance=np.array([0.8, 0.15, 0.05]),
            scores_df=empty_scores,
        )
        empty_curves = pl.DataFrame(schema=CURVE_INPUT_SCHEMA)

        fc = forecast_from_analogs(empty_curves, pca_result, k=10)
        assert fc.is_empty()
        assert fc.schema == FORECAST_SCHEMA

    def test_forecast_empty_curves(self) -> None:
        """If curves_df has no matching rows, returns empty forecast."""
        curves = _make_synthetic_curves(currency="CAD", num_days=200)
        # PCA on USD but curves only have CAD
        usd_curves = _make_synthetic_curves(currency="USD", num_days=200)
        result = fit_pca(usd_curves, "USD", n_components=3)
        assert result is not None

        # Pass CAD curves for a USD PCA result
        fc = forecast_from_analogs(curves, result, k=5, horizon_days=30)
        assert fc.is_empty()
        assert fc.schema == FORECAST_SCHEMA
