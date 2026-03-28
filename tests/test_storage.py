"""Tests for the parquet storage layer."""

from datetime import date

import polars as pl
import pytest

from thalweg import storage


@pytest.fixture()
def tmp_data_dir(tmp_path, monkeypatch):
    """Point storage at a temporary directory."""
    curves_dir = tmp_path / "curves"
    curves_dir.mkdir()
    monkeypatch.setattr(storage, "CURVES_DIR", curves_dir)
    return curves_dir


def _make_curve(
    obs_date: date,
    currency: str = "CAD",
    curve_type: str = "govt_par",
    tenors: list[float] | None = None,
    base_yield: float = 3.0,
) -> pl.DataFrame:
    tenors = tenors or [2.0, 5.0, 10.0, 30.0]
    return pl.DataFrame(
        {
            "date": [obs_date] * len(tenors),
            "currency": [currency] * len(tenors),
            "curve_type": [curve_type] * len(tenors),
            "tenor_years": tenors,
            "yield_pct": [base_yield + i * 0.25 for i in range(len(tenors))],
        }
    ).cast({"date": pl.Date, "tenor_years": pl.Float64, "yield_pct": pl.Float64})


def test_append_and_read_roundtrip(tmp_data_dir):
    df = _make_curve(date(2024, 1, 15))
    storage.append_curves(df)

    result = storage.read_curves(currency="CAD", curve_type="govt_par")
    assert result.shape == df.shape
    assert result.sort("tenor_years").equals(df.sort("tenor_years"))


def test_append_deduplicates(tmp_data_dir):
    df1 = _make_curve(date(2024, 1, 15), base_yield=3.0)
    df2 = _make_curve(date(2024, 1, 15), base_yield=3.5)  # same date, new values

    storage.append_curves(df1)
    storage.append_curves(df2)

    result = storage.read_curves(currency="CAD", curve_type="govt_par")
    assert result.shape[0] == 4  # 4 tenors, not 8
    # Should keep the latest values (3.5 base)
    assert result.sort("tenor_years")["yield_pct"][0] == pytest.approx(3.5)


def test_append_multiple_dates(tmp_data_dir):
    df1 = _make_curve(date(2024, 1, 15))
    df2 = _make_curve(date(2024, 1, 16))

    storage.append_curves(df1)
    storage.append_curves(df2)

    result = storage.read_curves(currency="CAD", curve_type="govt_par")
    assert result.shape[0] == 8  # 4 tenors × 2 dates


def test_read_with_date_filter(tmp_data_dir):
    storage.append_curves(_make_curve(date(2024, 1, 15)))
    storage.append_curves(_make_curve(date(2024, 3, 15)))

    result = storage.read_curves(
        currency="CAD",
        curve_type="govt_par",
        start_date=date(2024, 2, 1),
        end_date=date(2024, 12, 31),
    )
    assert result.shape[0] == 4  # only March
    assert result["date"][0] == date(2024, 3, 15)


def test_get_latest_date(tmp_data_dir):
    storage.append_curves(_make_curve(date(2024, 1, 15)))
    storage.append_curves(_make_curve(date(2024, 3, 15)))

    assert storage.get_latest_date("CAD", "govt_par") == date(2024, 3, 15)


def test_get_latest_date_missing_file(tmp_data_dir):
    assert storage.get_latest_date("CAD", "govt_par") is None


def test_routes_to_correct_file(tmp_data_dir):
    storage.append_curves(_make_curve(date(2024, 1, 15), currency="CAD"))
    storage.append_curves(_make_curve(date(2024, 1, 15), currency="USD"))

    assert (tmp_data_dir / "gov_cad.parquet").exists()
    assert (tmp_data_dir / "gov_usd.parquet").exists()

    cad = storage.read_curves(currency="CAD", curve_type="govt_par")
    assert cad.shape[0] == 4
    assert cad["currency"][0] == "CAD"

    # Verify swap routing
    storage.append_curves(_make_curve(date(2024, 1, 15), currency="USD", curve_type="swap"))
    assert (tmp_data_dir / "swap_usd.parquet").exists()


# ---------------------------------------------------------------------------
# Derived data + regime storage
# ---------------------------------------------------------------------------


@pytest.fixture()
def tmp_derived_dir(tmp_path, monkeypatch):
    """Point derived storage at a temporary directory."""
    derived_dir = tmp_path / "derived"
    derived_dir.mkdir()
    monkeypatch.setattr(storage, "DERIVED_DIR", derived_dir)
    # Also set up curves dir for get_available_dates
    curves_dir = tmp_path / "curves"
    curves_dir.mkdir()
    monkeypatch.setattr(storage, "CURVES_DIR", curves_dir)
    return derived_dir


def _make_regimes(obs_date: date, currency: str = "USD") -> pl.DataFrame:
    return pl.DataFrame({
        "date": [obs_date],
        "currency": [currency],
        "curve_type": ["govt_par"],
        "regime": ["normal"],
        "slope_2s10s_bp": [50.0],
        "level_10y": [4.0],
    }).cast({"date": pl.Date, "slope_2s10s_bp": pl.Float64, "level_10y": pl.Float64})


def test_read_derived_missing_file(tmp_derived_dir):
    result = storage.read_derived("nonexistent")
    assert result.is_empty()


def test_append_and_read_regimes(tmp_derived_dir):
    df = _make_regimes(date(2024, 1, 15))
    storage.append_regimes(df)

    result = storage.read_regimes()
    assert result.shape[0] == 1
    assert result["regime"][0] == "normal"


def test_regime_dedup(tmp_derived_dir):
    df1 = _make_regimes(date(2024, 1, 15))
    df2 = pl.DataFrame({
        "date": [date(2024, 1, 15)],
        "currency": ["USD"],
        "curve_type": ["govt_par"],
        "regime": ["inverted"],
        "slope_2s10s_bp": [-50.0],
        "level_10y": [4.5],
    }).cast({"date": pl.Date, "slope_2s10s_bp": pl.Float64, "level_10y": pl.Float64})

    storage.append_regimes(df1)
    storage.append_regimes(df2)

    result = storage.read_regimes()
    assert result.shape[0] == 1
    assert result["regime"][0] == "inverted"  # latest wins


def test_read_regimes_filtered(tmp_derived_dir):
    df = pl.concat([
        _make_regimes(date(2024, 1, 15), "USD"),
        _make_regimes(date(2024, 1, 15), "CAD"),
    ])
    storage.append_regimes(df)

    result = storage.read_regimes(currency="USD")
    assert result.shape[0] == 1
    assert result["currency"][0] == "USD"


def test_read_regimes_missing_file(tmp_derived_dir):
    result = storage.read_regimes()
    assert result.is_empty()


def test_get_available_dates(tmp_derived_dir):
    storage.append_curves(_make_curve(date(2024, 1, 15)))
    storage.append_curves(_make_curve(date(2024, 3, 15)))

    dates = storage.get_available_dates()
    assert dates == [date(2024, 1, 15), date(2024, 3, 15)]


def test_get_available_dates_filtered(tmp_derived_dir):
    storage.append_curves(_make_curve(date(2024, 1, 15), currency="CAD"))
    storage.append_curves(_make_curve(date(2024, 1, 16), currency="USD"))

    dates = storage.get_available_dates(currency="CAD")
    assert dates == [date(2024, 1, 15)]
