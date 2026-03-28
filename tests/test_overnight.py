"""Tests for the overnight rates fetcher."""

import json
from datetime import date
from pathlib import Path

import polars as pl
import pytest

from thalweg.fetchers.overnight import OvernightRatesFetcher

FIXTURES_DIR = Path(__file__).parent / "fixtures"


@pytest.fixture()
def fetcher() -> OvernightRatesFetcher:
    return OvernightRatesFetcher()


@pytest.fixture()
def sofr_sample_json() -> dict:
    return json.loads((FIXTURES_DIR / "sofr_sample.json").read_text())


# ------------------------------------------------------------------
# Name
# ------------------------------------------------------------------


def test_name(fetcher):
    assert fetcher.name == "overnight"


# ------------------------------------------------------------------
# CORRA parsing
# ------------------------------------------------------------------


def test_parse_corra_json(fetcher):
    data = {
        "observations": [
            {"d": "2025-03-26", "AVG.INTWO": {"v": "2.95"}},
            {"d": "2025-03-27", "AVG.INTWO": {"v": "2.96"}},
        ]
    }
    df = fetcher._parse_corra_json(data)

    assert df.shape[0] == 2
    assert set(df.columns) == {"date", "rate_name", "value_pct"}
    assert df.schema["date"] == pl.Date
    assert df.schema["value_pct"] == pl.Float64
    assert df["rate_name"][0] == "CORRA"
    assert df["date"][0] == date(2025, 3, 26)
    assert df["value_pct"][0] == pytest.approx(2.95)
    assert df["value_pct"][1] == pytest.approx(2.96)


def test_parse_corra_json_empty(fetcher):
    df = fetcher._parse_corra_json({"observations": []})

    assert df.shape[0] == 0
    assert set(df.columns) == {"date", "rate_name", "value_pct"}
    assert df.schema["date"] == pl.Date


def test_parse_corra_json_skips_empty_values(fetcher):
    data = {
        "observations": [
            {"d": "2025-03-26", "AVG.INTWO": {"v": "2.95"}},
            {"d": "2025-03-27", "AVG.INTWO": {"v": ""}},
        ]
    }
    df = fetcher._parse_corra_json(data)
    assert df.shape[0] == 1


# ------------------------------------------------------------------
# SOFR parsing
# ------------------------------------------------------------------


def test_parse_sofr_json(fetcher, sofr_sample_json):
    df = fetcher._parse_sofr_json(sofr_sample_json)

    assert df.shape[0] == 2
    assert set(df.columns) == {"date", "rate_name", "value_pct"}
    assert df.schema["date"] == pl.Date
    assert df.schema["value_pct"] == pl.Float64
    assert df["rate_name"][0] == "SOFR"
    assert df["date"][0] == date(2025, 3, 26)
    assert df["value_pct"][0] == pytest.approx(4.30)
    assert df["date"][1] == date(2025, 3, 27)
    assert df["value_pct"][1] == pytest.approx(4.31)


def test_parse_sofr_json_empty(fetcher):
    df = fetcher._parse_sofr_json({"refRates": []})

    assert df.shape[0] == 0
    assert set(df.columns) == {"date", "rate_name", "value_pct"}
    assert df.schema["date"] == pl.Date


def test_parse_sofr_json_inline(fetcher):
    """Test SOFR parsing with inline data (not fixture file)."""
    data = {
        "refRates": [
            {"effectiveDate": "2025-01-02", "percentRate": 4.50, "type": "SOFR"},
        ]
    }
    df = fetcher._parse_sofr_json(data)
    assert df.shape[0] == 1
    assert df["rate_name"][0] == "SOFR"
    assert df["value_pct"][0] == pytest.approx(4.50)


# ------------------------------------------------------------------
# eSTR parsing
# ------------------------------------------------------------------


def test_parse_estr_csv(fetcher):
    csv_text = (
        "KEY,FREQ,REF_AREA,CURRENCY,TIME_PERIOD,OBS_VALUE,OBS_STATUS\n"
        "EST.B.EU000A2X2A25.WT,B,U2,EUR,2025-03-26,2.40,A\n"
        "EST.B.EU000A2X2A25.WT,B,U2,EUR,2025-03-27,2.41,A\n"
    )
    df = fetcher._parse_estr_csv(csv_text)

    assert df.shape[0] == 2
    assert set(df.columns) == {"date", "rate_name", "value_pct"}
    assert df.schema["date"] == pl.Date
    assert df.schema["value_pct"] == pl.Float64
    assert df["rate_name"][0] == "ESTR"
    assert df["date"][0] == date(2025, 3, 26)
    assert df["value_pct"][0] == pytest.approx(2.40)
    assert df["value_pct"][1] == pytest.approx(2.41)


def test_parse_estr_csv_empty(fetcher):
    df = fetcher._parse_estr_csv("")

    assert df.shape[0] == 0
    assert set(df.columns) == {"date", "rate_name", "value_pct"}
    assert df.schema["date"] == pl.Date


def test_parse_estr_csv_missing_columns(fetcher):
    """CSV without expected columns returns empty DataFrame."""
    csv_text = "FOO,BAR\n1,2\n"
    df = fetcher._parse_estr_csv(csv_text)
    assert df.shape[0] == 0


# ------------------------------------------------------------------
# SONIA parsing
# ------------------------------------------------------------------


def test_parse_sonia_csv(fetcher):
    csv_text = "DATE,IUDSOIA\n26 Mar 2025,4.45\n27 Mar 2025,4.46\n"
    df = fetcher._parse_sonia_csv(csv_text)

    assert df.shape[0] == 2
    assert set(df.columns) == {"date", "rate_name", "value_pct"}
    assert df.schema["date"] == pl.Date
    assert df.schema["value_pct"] == pl.Float64
    assert df["rate_name"][0] == "SONIA"
    assert df["date"][0] == date(2025, 3, 26)
    assert df["value_pct"][0] == pytest.approx(4.45)
    assert df["value_pct"][1] == pytest.approx(4.46)


def test_parse_sonia_csv_empty(fetcher):
    df = fetcher._parse_sonia_csv("")

    assert df.shape[0] == 0
    assert set(df.columns) == {"date", "rate_name", "value_pct"}
    assert df.schema["date"] == pl.Date


def test_parse_sonia_csv_missing_column(fetcher):
    """CSV without IUDSOIA column returns empty DataFrame."""
    csv_text = "DATE,OTHER\n26 Mar 2025,1.0\n"
    df = fetcher._parse_sonia_csv(csv_text)
    assert df.shape[0] == 0


def test_parse_sonia_csv_skips_empty_values(fetcher):
    csv_text = "DATE,IUDSOIA\n26 Mar 2025,4.45\n27 Mar 2025,\n"
    df = fetcher._parse_sonia_csv(csv_text)
    assert df.shape[0] == 1
