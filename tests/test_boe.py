"""Tests for the BoE gilt nominal par yield fetcher."""

from datetime import date
from pathlib import Path

import polars as pl
import pytest

from thalweg.fetchers.boe import BoEFetcher

FIXTURES_DIR = Path(__file__).parent / "fixtures"


@pytest.fixture()
def fetcher() -> BoEFetcher:
    return BoEFetcher()


@pytest.fixture()
def boe_sample_csv() -> str:
    return (FIXTURES_DIR / "boe_gilt_sample.csv").read_text()


def test_name(fetcher):
    assert fetcher.name == "boe"


def test_parse_csv(fetcher, boe_sample_csv):
    df = fetcher._parse_csv(boe_sample_csv)

    # 3 tenors x 3 days = 9 rows
    assert df.shape[0] == 9
    assert set(df.columns) == {"date", "currency", "curve_type", "tenor_years", "yield_pct"}
    assert df["currency"][0] == "GBP"
    assert df["curve_type"][0] == "govt_par"

    # Yields should be positive, reasonable numbers
    assert df["yield_pct"].min() > 0
    assert df["yield_pct"].max() < 20

    # Check dates are present
    dates = df["date"].unique().sort().to_list()
    assert len(dates) == 3
    assert dates[0] == date(2025, 1, 2)
    assert dates[2] == date(2025, 1, 6)


def test_parse_csv_tenors(fetcher, boe_sample_csv):
    """Verify that all three tenor values are present."""
    df = fetcher._parse_csv(boe_sample_csv)

    tenors = sorted(df["tenor_years"].unique().to_list())
    assert tenors == [5.0, 10.0, 20.0]

    # Each day should have 3 tenors
    for d in df["date"].unique().to_list():
        day_df = df.filter(pl.col("date") == d)
        assert day_df.shape[0] == 3


def test_parse_csv_empty(fetcher):
    """Empty input returns an empty DataFrame with correct schema."""
    df = fetcher._parse_csv("")
    assert df.shape[0] == 0
    assert set(df.columns) == {"date", "currency", "curve_type", "tenor_years", "yield_pct"}
    assert df.schema["date"] == pl.Date
    assert df.schema["tenor_years"] == pl.Float64
    assert df.schema["yield_pct"] == pl.Float64


def test_parse_csv_skips_missing_values(fetcher):
    """Rows with empty yield values are skipped."""
    csv = (
        "DATE,IUDSNPY,IUDMNPY,IUDLNPY\n"
        "02 Jan 2025,4.1994,,4.8645\n"
        "03 Jan 2025,,,\n"
    )
    df = fetcher._parse_csv(csv)

    # First row: 5yr and 20yr present, 10yr missing -> 2 rows
    # Second row: all missing -> 0 rows
    assert df.shape[0] == 2
    tenors = sorted(df["tenor_years"].to_list())
    assert tenors == [5.0, 20.0]


def test_tenor_mapping(fetcher):
    assert fetcher.TENOR_MAP["IUDSNPY"] == pytest.approx(5.0)
    assert fetcher.TENOR_MAP["IUDMNPY"] == pytest.approx(10.0)
    assert fetcher.TENOR_MAP["IUDLNPY"] == pytest.approx(20.0)
    assert len(fetcher.TENOR_MAP) == 3


def test_format_date(fetcher):
    assert fetcher._format_date(date(2025, 1, 1)) == "01/Jan/2025"
    assert fetcher._format_date(date(2024, 12, 31)) == "31/Dec/2024"
    assert fetcher._format_date(date(2000, 3, 15)) == "15/Mar/2000"
