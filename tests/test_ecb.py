"""Tests for the ECB yield curve fetcher."""

from datetime import date
from pathlib import Path

import polars as pl
import pytest

from thalweg.fetchers.ecb import ECBFetcher

FIXTURES_DIR = Path(__file__).parent / "fixtures"


@pytest.fixture()
def fetcher() -> ECBFetcher:
    return ECBFetcher()


@pytest.fixture()
def ecb_sample_csv() -> str:
    return (FIXTURES_DIR / "ecb_yc_sample.csv").read_text()


def test_name(fetcher):
    assert fetcher.name == "ecb"


def test_parse_csv(fetcher, ecb_sample_csv):
    df = fetcher._parse_csv(ecb_sample_csv)

    # 9 tenors x 2 days = 18 rows
    assert df.shape[0] == 18
    assert set(df.columns) == {"date", "currency", "curve_type", "tenor_years", "yield_pct"}
    assert df["currency"][0] == "EUR"
    assert df["curve_type"][0] == "govt_zero"

    # Check tenor values
    tenors = sorted(df["tenor_years"].unique().to_list())
    assert 0.25 in tenors  # 3M
    assert 1.0 in tenors
    assert 2.0 in tenors
    assert 10.0 in tenors
    assert 30.0 in tenors
    assert len(tenors) == 9

    # Yields should be positive, reasonable numbers
    assert df["yield_pct"].min() > 0
    assert df["yield_pct"].max() < 20


def test_parse_csv_multi_day(fetcher, ecb_sample_csv):
    """Fixture contains 2 days of data (2024-12-30 and 2024-12-31)."""
    df = fetcher._parse_csv(ecb_sample_csv)

    dates = df["date"].unique().sort().to_list()
    assert len(dates) == 2
    assert dates[0] == date(2024, 12, 30)
    assert dates[1] == date(2024, 12, 31)

    # Each day should have 9 tenors
    assert df.filter(pl.col("date") == date(2024, 12, 30)).shape[0] == 9
    assert df.filter(pl.col("date") == date(2024, 12, 31)).shape[0] == 9


def test_parse_csv_empty(fetcher):
    """Empty input returns an empty DataFrame with correct schema."""
    df = fetcher._parse_csv("")
    assert df.shape[0] == 0
    assert set(df.columns) == {"date", "currency", "curve_type", "tenor_years", "yield_pct"}
    assert df.schema["date"] == pl.Date
    assert df.schema["tenor_years"] == pl.Float64
    assert df.schema["yield_pct"] == pl.Float64


def test_parse_csv_skips_missing_values(fetcher):
    """Rows with missing OBS_VALUE are skipped."""
    csv = (
        "KEY,FREQ,REF_AREA,CURRENCY,PROVIDER_FM,DATA_TYPE_FM,DATA_TYPE_FM,"
        "TIME_PERIOD,OBS_VALUE,OBS_STATUS,OBS_CONF\n"
        "YC.B.U2.EUR.4F.G_N_A.SV_C_YM.SR_10Y,B,U2,EUR,4F,G_N_A,SV_C_YM,"
        "2024-12-31,2.40,A,F\n"
        "YC.B.U2.EUR.4F.G_N_A.SV_C_YM.SR_30Y,B,U2,EUR,4F,G_N_A,SV_C_YM,"
        "2024-12-31,,A,F\n"
    )
    df = fetcher._parse_csv(csv)
    assert df.shape[0] == 1
    assert df["tenor_years"][0] == pytest.approx(10.0)


def test_tenor_mapping(fetcher):
    assert fetcher.TENOR_MAP["SR_3M"] == pytest.approx(0.25)
    assert fetcher.TENOR_MAP["SR_1Y"] == 1.0
    assert fetcher.TENOR_MAP["SR_10Y"] == 10.0
    assert fetcher.TENOR_MAP["SR_30Y"] == 30.0
    assert len(fetcher.TENOR_MAP) == 9


def test_build_url(fetcher):
    """URL construction joins tenor keys correctly."""
    url = fetcher._build_url(["SR_3M", "SR_10Y"])
    assert "SR_3M+SR_10Y" in url
    assert url.startswith("https://data-api.ecb.europa.eu/service/data/YC/")


def test_build_url_default(fetcher):
    """Default URL includes all tenors."""
    url = fetcher._build_url()
    for tenor in fetcher.TENOR_MAP:
        assert tenor in url
