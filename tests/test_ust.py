"""Tests for the US Treasury fetcher."""

from pathlib import Path

import pytest

from thalweg.fetchers.ust import USTFetcher

FIXTURES_DIR = Path(__file__).parent / "fixtures"


@pytest.fixture()
def fetcher() -> USTFetcher:
    return USTFetcher()


@pytest.fixture()
def xml_text() -> str:
    return (FIXTURES_DIR / "ust_xml_sample.xml").read_text()


@pytest.fixture()
def csv_text() -> str:
    return (FIXTURES_DIR / "ust_csv_sample.csv").read_text()


def test_name(fetcher):
    assert fetcher.name == "ust"


def test_parse_xml(fetcher, xml_text):
    df = fetcher._parse_xml(xml_text)

    assert set(df.columns) == {"date", "currency", "curve_type", "tenor_years", "yield_pct"}
    assert df["currency"][0] == "USD"
    assert df["curve_type"][0] == "govt_par"
    assert df.shape[0] > 0

    # Should have multiple tenors per date
    dates = df["date"].unique()
    assert len(dates) >= 1

    # Tenors should include standard maturities
    tenors = df["tenor_years"].unique().sort().to_list()
    assert 1.0 in tenors
    assert 10.0 in tenors
    assert 30.0 in tenors

    # Yields should be positive
    assert df["yield_pct"].min() > 0


def test_parse_xml_missing_tenors(fetcher):
    """XML entries with missing tenor elements should be handled gracefully."""
    xml = '''<?xml version="1.0" encoding="utf-8"?>
<feed xmlns="http://www.w3.org/2005/Atom"
      xmlns:d="http://schemas.microsoft.com/ado/2007/08/dataservices"
      xmlns:m="http://schemas.microsoft.com/ado/2007/08/dataservices/metadata">
<entry><content type="application/xml"><m:properties>
<d:NEW_DATE>2025-01-02T00:00:00</d:NEW_DATE>
<d:BC_10YEAR m:type="Edm.Double">4.57</d:BC_10YEAR>
<d:BC_30YEAR m:type="Edm.Double">4.78</d:BC_30YEAR>
</m:properties></content></entry>
</feed>'''
    df = fetcher._parse_xml(xml)
    assert df.shape[0] == 2  # only the two present tenors


def test_parse_csv(fetcher, csv_text):
    df = fetcher._parse_csv(csv_text)

    assert set(df.columns) == {"date", "currency", "curve_type", "tenor_years", "yield_pct"}
    assert df["currency"][0] == "USD"
    assert df["curve_type"][0] == "govt_par"

    # Should have 3 valid days (row 4 is all empty)
    dates = df["date"].unique()
    assert len(dates) == 3


def test_parse_csv_missing_values(fetcher):
    """Empty cells in CSV should be skipped, not produce NaN rows."""
    csv = "Date,1 Mo,10 Yr,30 Yr\n01/02/2024,,4.00,4.10\n"
    df = fetcher._parse_csv(csv)
    # 1 Mo is empty, so only 2 tenors for that day
    assert df.shape[0] == 2


def test_tenor_mapping(fetcher):
    assert fetcher.XML_TENOR_MAP["BC_1MONTH"] == pytest.approx(1 / 12)
    assert fetcher.XML_TENOR_MAP["BC_1YEAR"] == 1.0
    assert fetcher.XML_TENOR_MAP["BC_10YEAR"] == 10.0
    assert fetcher.XML_TENOR_MAP["BC_30YEAR"] == 30.0
