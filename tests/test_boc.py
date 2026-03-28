"""Tests for the Bank of Canada fetcher."""

import json
from datetime import date
from pathlib import Path

import polars as pl
import pytest

from thalweg.fetchers.boc import BoCFetcher

FIXTURES_DIR = Path(__file__).parent / "fixtures"


@pytest.fixture()
def boc_sample_json() -> dict:
    return json.loads((FIXTURES_DIR / "boc_benchmark_sample.json").read_text())


@pytest.fixture()
def fetcher() -> BoCFetcher:
    return BoCFetcher()


def test_name(fetcher):
    assert fetcher.name == "boc"


def test_parse_observations(fetcher, boc_sample_json):
    df = fetcher._parse_observations(boc_sample_json)

    # Should have one row per tenor per day
    assert df.shape[0] >= 6  # at least 6 tenors
    assert set(df.columns) == {"date", "currency", "curve_type", "tenor_years", "yield_pct"}
    assert df["currency"][0] == "CAD"
    assert df["curve_type"][0] == "govt_par"

    # Check tenor values
    tenors = sorted(df["tenor_years"].to_list())
    assert 2.0 in tenors
    assert 5.0 in tenors
    assert 10.0 in tenors
    assert 30.0 in tenors

    # Yields should be positive numbers
    assert df["yield_pct"].min() > 0
    assert df["yield_pct"].max() < 20


def test_parse_observations_multi_day(fetcher):
    """Test parsing a response with multiple days of data."""
    data = {
        "observations": [
            {
                "d": "2024-01-15",
                "BD.CDN.2YR.DQ.YLD": {"v": "4.10"},
                "BD.CDN.5YR.DQ.YLD": {"v": "3.50"},
                "BD.CDN.10YR.DQ.YLD": {"v": "3.40"},
            },
            {
                "d": "2024-01-16",
                "BD.CDN.2YR.DQ.YLD": {"v": "4.15"},
                "BD.CDN.5YR.DQ.YLD": {"v": "3.55"},
                "BD.CDN.10YR.DQ.YLD": {"v": "3.45"},
            },
        ]
    }
    df = fetcher._parse_observations(data)
    assert df.shape[0] == 6  # 3 tenors x 2 days
    assert df.filter(pl.col("date") == date(2024, 1, 15)).shape[0] == 3


def test_parse_skips_missing_values(fetcher):
    """Values with empty 'v' or missing keys are skipped."""
    data = {
        "observations": [
            {
                "d": "2024-01-15",
                "BD.CDN.2YR.DQ.YLD": {"v": "4.10"},
                "BD.CDN.5YR.DQ.YLD": {"v": ""},
            },
        ]
    }
    df = fetcher._parse_observations(data)
    assert df.shape[0] == 1  # only the valid row


def test_tenor_mapping(fetcher):
    assert fetcher.TENOR_MAP["BD.CDN.2YR.DQ.YLD"] == 2.0
    assert fetcher.TENOR_MAP["BD.CDN.LONG.DQ.YLD"] == 30.0
