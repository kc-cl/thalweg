"""Tests for the overnight rate storage layer."""

from datetime import date

import polars as pl
import pytest

from thalweg import storage


@pytest.fixture()
def tmp_rates_dir(tmp_path, monkeypatch):
    """Point rate storage at a temporary directory."""
    rates_dir = tmp_path / "rates"
    rates_dir.mkdir()
    monkeypatch.setattr(storage, "RATES_DIR", rates_dir)
    return rates_dir


def _make_rates(
    obs_date: date,
    rate_name: str = "CORRA",
    value_pct: float = 4.5,
) -> pl.DataFrame:
    return pl.DataFrame(
        {
            "date": [obs_date],
            "rate_name": [rate_name],
            "value_pct": [value_pct],
        }
    ).cast({"date": pl.Date, "value_pct": pl.Float64})


def test_append_and_read_rates_roundtrip(tmp_rates_dir):
    df = _make_rates(date(2024, 1, 15))
    storage.append_rates(df)

    result = storage.read_rates(rate_name="CORRA")
    assert result.shape == df.shape
    assert result["value_pct"][0] == pytest.approx(4.5)


def test_append_rates_deduplicates(tmp_rates_dir):
    df1 = _make_rates(date(2024, 1, 15), value_pct=4.5)
    df2 = _make_rates(date(2024, 1, 15), value_pct=4.75)  # same date, new value

    storage.append_rates(df1)
    storage.append_rates(df2)

    result = storage.read_rates(rate_name="CORRA")
    assert result.shape[0] == 1
    # Should keep the latest value (4.75)
    assert result["value_pct"][0] == pytest.approx(4.75)


def test_append_rates_multiple_dates(tmp_rates_dir):
    df1 = _make_rates(date(2024, 1, 15))
    df2 = _make_rates(date(2024, 1, 16))

    storage.append_rates(df1)
    storage.append_rates(df2)

    result = storage.read_rates(rate_name="CORRA")
    assert result.shape[0] == 2


def test_read_rates_with_date_filter(tmp_rates_dir):
    storage.append_rates(_make_rates(date(2024, 1, 15)))
    storage.append_rates(_make_rates(date(2024, 3, 15)))

    result = storage.read_rates(
        rate_name="CORRA",
        start_date=date(2024, 2, 1),
        end_date=date(2024, 12, 31),
    )
    assert result.shape[0] == 1
    assert result["date"][0] == date(2024, 3, 15)


def test_read_rates_by_name(tmp_rates_dir):
    storage.append_rates(_make_rates(date(2024, 1, 15), rate_name="CORRA", value_pct=4.5))
    storage.append_rates(_make_rates(date(2024, 1, 15), rate_name="SOFR", value_pct=5.3))

    corra = storage.read_rates(rate_name="CORRA")
    assert corra.shape[0] == 1
    assert corra["rate_name"][0] == "CORRA"

    sofr = storage.read_rates(rate_name="SOFR")
    assert sofr.shape[0] == 1
    assert sofr["rate_name"][0] == "SOFR"

    all_rates = storage.read_rates()
    assert all_rates.shape[0] == 2


def test_get_latest_rate_date(tmp_rates_dir):
    storage.append_rates(_make_rates(date(2024, 1, 15)))
    storage.append_rates(_make_rates(date(2024, 3, 15)))

    assert storage.get_latest_rate_date("CORRA") == date(2024, 3, 15)


def test_get_latest_rate_date_missing_file(tmp_rates_dir):
    assert storage.get_latest_rate_date("CORRA") is None


def test_append_rates_validates_columns(tmp_rates_dir):
    bad_df = pl.DataFrame({"date": [date(2024, 1, 15)], "wrong_col": ["x"]})
    with pytest.raises(ValueError, match="Missing required columns"):
        storage.append_rates(bad_df)
