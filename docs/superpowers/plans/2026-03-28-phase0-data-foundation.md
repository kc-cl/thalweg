# Phase 0: Data Foundation — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a working data pipeline that fetches GoC and UST yield curves, stores them as parquet, and exposes them via CLI — with 10+ years of backfilled history.

**Architecture:** Each data source has a fetcher class (async, inherits `BaseFetcher`) that normalizes API responses to a common schema `(date, currency, curve_type, tenor_years, yield_pct)`. A storage module handles parquet read/write with deduplication. The CLI bridges sync Click commands to async fetchers via `asyncio.run()`.

**Tech Stack:** Python 3.12, Polars, PyArrow, httpx, Click, pytest, xml.etree.ElementTree

---

## File Map

| File | Responsibility |
|------|---------------|
| `src/thalweg/storage.py` | Parquet read/write/append/query for curve data |
| `src/thalweg/fetchers/boc.py` | Bank of Canada Valet API fetcher |
| `src/thalweg/fetchers/ust.py` | US Treasury XML + CSV fetcher |
| `src/thalweg/cli.py` | Wire CLI commands to fetchers + storage |
| `tests/test_storage.py` | Storage layer tests |
| `tests/test_boc.py` | BoC fetcher parsing tests |
| `tests/test_ust.py` | UST fetcher parsing tests |
| `tests/fixtures/boc_benchmark_sample.json` | Saved BoC API response |
| `tests/fixtures/ust_xml_sample.xml` | Saved UST XML response |
| `tests/fixtures/ust_csv_sample.csv` | Saved UST CSV snippet |

---

### Task 1: Storage Layer

**Files:**
- Modify: `src/thalweg/storage.py`
- Create: `tests/test_storage.py`

#### Step 1: Write failing tests for append_curves and read_curves

- [ ] **1a: Write test_storage.py with core tests**

```python
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
    return pl.DataFrame({
        "date": [obs_date] * len(tenors),
        "currency": [currency] * len(tenors),
        "curve_type": [curve_type] * len(tenors),
        "tenor_years": tenors,
        "yield_pct": [base_yield + i * 0.25 for i in range(len(tenors))],
    }).cast({"date": pl.Date, "tenor_years": pl.Float64, "yield_pct": pl.Float64})


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
        currency="CAD", curve_type="govt_par",
        start_date=date(2024, 2, 1), end_date=date(2024, 12, 31),
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
```

- [ ] **1b: Run tests to verify they fail**

Run: `uv run pytest tests/test_storage.py -v`
Expected: FAIL — `storage` has no functions yet

#### Step 2: Implement storage.py

- [ ] **2a: Write the storage module**

```python
"""Parquet storage layer for yield curve data.

Handles reading, writing, appending, and querying curve data stored
as parquet files in the configured data directory.
"""

from __future__ import annotations

import logging
from datetime import date
from pathlib import Path

import polars as pl

from thalweg.config import CURVES_DIR

logger = logging.getLogger(__name__)

EXPECTED_SCHEMA = {
    "date": pl.Date,
    "currency": pl.Utf8,
    "curve_type": pl.Utf8,
    "tenor_years": pl.Float64,
    "yield_pct": pl.Float64,
}

DEDUP_KEYS = ["date", "currency", "curve_type", "tenor_years"]


def _parquet_path(currency: str, curve_type: str) -> Path:
    """Determine the parquet file path for a given currency and curve type."""
    if curve_type.startswith("swap"):
        prefix = "swap"
    else:
        prefix = "gov"
    return CURVES_DIR / f"{prefix}_{currency.lower()}.parquet"


def append_curves(df: pl.DataFrame) -> None:
    """Append curve data to the appropriate parquet file.

    Deduplicates on (date, currency, curve_type, tenor_years), keeping the
    most recently appended values.

    Args:
        df: DataFrame with columns matching EXPECTED_SCHEMA.
    """
    df = df.cast({k: v for k, v in EXPECTED_SCHEMA.items() if k in df.columns})

    # Group by (currency, curve_type) to route to correct files
    groups = df.partition_by(["currency", "curve_type"], as_dict=True)

    for (currency, curve_type), group_df in groups.items():
        path = _parquet_path(currency, curve_type)

        if path.exists():
            existing = pl.read_parquet(path)
            combined = pl.concat([existing, group_df])
        else:
            combined = group_df

        # Deduplicate: keep last occurrence (the new data wins)
        deduped = combined.unique(subset=DEDUP_KEYS, keep="last", maintain_order=True)
        deduped = deduped.sort(["date", "tenor_years"])

        deduped.write_parquet(path)
        logger.info(
            "Wrote %d rows to %s (was %d)",
            deduped.shape[0],
            path.name,
            combined.shape[0],
        )


def read_curves(
    currency: str | None = None,
    curve_type: str | None = None,
    start_date: date | None = None,
    end_date: date | None = None,
) -> pl.DataFrame:
    """Read curve data from parquet files with optional filters.

    Args:
        currency: Filter by currency code (e.g. 'CAD', 'USD').
        curve_type: Filter by curve type (e.g. 'govt_par', 'govt_zero').
        start_date: Include only dates on or after this date.
        end_date: Include only dates on or before this date.

    Returns:
        Filtered DataFrame, or empty DataFrame if no data found.
    """
    if currency and curve_type:
        path = _parquet_path(currency, curve_type)
        if not path.exists():
            return pl.DataFrame(schema=EXPECTED_SCHEMA)
        lf = pl.scan_parquet(path)
    else:
        paths = list(CURVES_DIR.glob("*.parquet"))
        if not paths:
            return pl.DataFrame(schema=EXPECTED_SCHEMA)
        lf = pl.scan_parquet(paths)

    if currency:
        lf = lf.filter(pl.col("currency") == currency)
    if curve_type:
        lf = lf.filter(pl.col("curve_type") == curve_type)
    if start_date:
        lf = lf.filter(pl.col("date") >= start_date)
    if end_date:
        lf = lf.filter(pl.col("date") <= end_date)

    return lf.collect()


def get_latest_date(currency: str, curve_type: str) -> date | None:
    """Get the most recent date in a parquet file.

    Args:
        currency: Currency code.
        curve_type: Curve type.

    Returns:
        The latest date, or None if the file doesn't exist.
    """
    path = _parquet_path(currency, curve_type)
    if not path.exists():
        return None

    result = (
        pl.scan_parquet(path)
        .select(pl.col("date").max())
        .collect()
    )
    return result["date"][0]
```

- [ ] **2b: Run tests to verify they pass**

Run: `uv run pytest tests/test_storage.py -v`
Expected: All 7 tests PASS

- [ ] **2c: Commit**

```bash
git add src/thalweg/storage.py tests/test_storage.py
git commit -m "feat: parquet storage layer with append, read, and dedup"
```

---

### Task 2: BoC Fetcher

**Files:**
- Create: `src/thalweg/fetchers/boc.py`
- Create: `tests/test_boc.py`
- Create: `tests/fixtures/boc_benchmark_sample.json`

#### Step 1: Capture a real API fixture

- [ ] **1a: Fetch a real BoC response and save as fixture**

```bash
curl -s 'https://www.bankofcanada.ca/valet/observations/group/bond_yields_benchmark/json?start_date=2025-03-24&end_date=2025-03-24' > tests/fixtures/boc_benchmark_sample.json
```

Verify it has the expected structure:
```bash
uv run python -c "
import json, pathlib
data = json.loads(pathlib.Path('tests/fixtures/boc_benchmark_sample.json').read_text())
obs = data['observations'][0]
print('Date:', obs['d'])
for k, v in obs.items():
    if k != 'd':
        print(f'  {k}: {v}')
"
```

Expected: prints the date and series values like `BD.CDN.2YR.DQ.YLD: {'v': '2.54'}`.

#### Step 2: Write failing tests

- [ ] **2a: Write test_boc.py**

```python
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
    assert df.shape[0] == 6  # 3 tenors × 2 days
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
```

- [ ] **2b: Run tests to verify they fail**

Run: `uv run pytest tests/test_boc.py -v`
Expected: FAIL — `boc` module doesn't exist yet

#### Step 3: Implement the BoC fetcher

- [ ] **3a: Write boc.py**

```python
"""Bank of Canada Valet API fetcher.

Fetches benchmark Government of Canada bond yields from the Valet API.
"""

from __future__ import annotations

import logging
from datetime import date, timedelta

import polars as pl

from thalweg.config import BOC_BASE_URL
from thalweg.fetchers.base import BaseFetcher

logger = logging.getLogger(__name__)

# Benchmark yield series → tenor in years
_TENOR_MAP: dict[str, float] = {
    "BD.CDN.2YR.DQ.YLD": 2.0,
    "BD.CDN.3YR.DQ.YLD": 3.0,
    "BD.CDN.5YR.DQ.YLD": 5.0,
    "BD.CDN.7YR.DQ.YLD": 7.0,
    "BD.CDN.10YR.DQ.YLD": 10.0,
    "BD.CDN.LONG.DQ.YLD": 30.0,
}


class BoCFetcher(BaseFetcher):
    """Fetches GoC benchmark bond yields from the Bank of Canada Valet API."""

    TENOR_MAP = _TENOR_MAP

    @property
    def name(self) -> str:
        return "boc"

    def _parse_observations(self, data: dict) -> pl.DataFrame:
        """Parse Valet API JSON response into a normalized DataFrame.

        Args:
            data: Parsed JSON response containing an 'observations' key.

        Returns:
            DataFrame with columns: date, currency, curve_type, tenor_years, yield_pct.
        """
        rows: list[dict] = []
        for obs in data.get("observations", []):
            obs_date = obs["d"]
            for series, tenor in _TENOR_MAP.items():
                value = obs.get(series)
                if value is None:
                    continue
                v_str = value.get("v", "")
                if not v_str:
                    continue
                try:
                    yield_pct = float(v_str)
                except ValueError:
                    logger.warning("Invalid yield value for %s on %s: %r", series, obs_date, v_str)
                    continue
                rows.append({
                    "date": obs_date,
                    "currency": "CAD",
                    "curve_type": "govt_par",
                    "tenor_years": tenor,
                    "yield_pct": yield_pct,
                })

        if not rows:
            return pl.DataFrame(schema={
                "date": pl.Date,
                "currency": pl.Utf8,
                "curve_type": pl.Utf8,
                "tenor_years": pl.Float64,
                "yield_pct": pl.Float64,
            })

        return pl.DataFrame(rows).cast({"date": pl.Date, "tenor_years": pl.Float64})

    async def fetch_latest(self) -> pl.DataFrame:
        """Fetch the most recent benchmark yield data."""
        url = f"{BOC_BASE_URL}/observations/group/bond_yields_benchmark/json"
        params = {"recent": "1"}

        async with self._get_client() as client:
            resp = await client.get(url, params=params)
            resp.raise_for_status()
            raw = resp.content
            self.save_raw(raw, "json")
            data = resp.json()

        df = self._parse_observations(data)
        logger.info("Fetched %d rows for latest BoC benchmark yields", df.shape[0])
        return df

    async def backfill(self, start_date: date, end_date: date) -> pl.DataFrame:
        """Fetch historical benchmark yields, chunked by year."""
        all_frames: list[pl.DataFrame] = []

        current = start_date
        while current <= end_date:
            chunk_end = min(
                date(current.year, 12, 31),
                end_date,
            )
            logger.info("BoC backfill: %s to %s", current, chunk_end)

            url = f"{BOC_BASE_URL}/observations/group/bond_yields_benchmark/json"
            params = {
                "start_date": current.isoformat(),
                "end_date": chunk_end.isoformat(),
            }

            async with self._get_client() as client:
                resp = await client.get(url, params=params)
                resp.raise_for_status()
                data = resp.json()

            df = self._parse_observations(data)
            if df.shape[0] > 0:
                all_frames.append(df)
                logger.info("  → %d rows", df.shape[0])

            current = date(current.year + 1, 1, 1)

        if not all_frames:
            return self._parse_observations({"observations": []})

        return pl.concat(all_frames)
```

- [ ] **3b: Run tests to verify they pass**

Run: `uv run pytest tests/test_boc.py -v`
Expected: All 5 tests PASS

- [ ] **3c: Commit**

```bash
git add src/thalweg/fetchers/boc.py tests/test_boc.py tests/fixtures/boc_benchmark_sample.json
git commit -m "feat: Bank of Canada benchmark yield fetcher with tests"
```

---

### Task 3: UST Fetcher

**Files:**
- Create: `src/thalweg/fetchers/ust.py`
- Create: `tests/test_ust.py`
- Create: `tests/fixtures/ust_xml_sample.xml`
- Create: `tests/fixtures/ust_csv_sample.csv`

#### Step 1: Capture real API fixtures

- [ ] **1a: Fetch a real UST XML response and save as fixture**

```bash
curl -s 'https://home.treasury.gov/resource-center/data-chart-center/interest-rates/pages/xml?data=daily_treasury_yield_curve&field_tdr_date_value=2025' > tests/fixtures/ust_xml_sample.xml
```

If the Treasury site is slow/down, create a minimal synthetic fixture instead:

```bash
uv run python -c "
import pathlib
pathlib.Path('tests/fixtures/ust_xml_sample.xml').write_text('''<?xml version=\"1.0\" encoding=\"utf-8\"?>
<feed xml:base=\"https://home.treasury.gov\" xmlns=\"http://www.w3.org/2005/Atom\" xmlns:d=\"http://schemas.microsoft.com/ado/2007/08/dataservices\" xmlns:m=\"http://schemas.microsoft.com/ado/2007/08/dataservices/metadata\">
<entry>
<content type=\"application/xml\">
<m:properties>
<d:NEW_DATE>2025-01-02T00:00:00</d:NEW_DATE>
<d:BC_1MONTH m:type=\"Edm.Double\">4.31</d:BC_1MONTH>
<d:BC_2MONTH m:type=\"Edm.Double\">4.28</d:BC_2MONTH>
<d:BC_3MONTH m:type=\"Edm.Double\">4.29</d:BC_3MONTH>
<d:BC_4MONTH m:type=\"Edm.Double\">4.31</d:BC_4MONTH>
<d:BC_6MONTH m:type=\"Edm.Double\">4.27</d:BC_6MONTH>
<d:BC_1YEAR m:type=\"Edm.Double\">4.17</d:BC_1YEAR>
<d:BC_2YEAR m:type=\"Edm.Double\">4.25</d:BC_2YEAR>
<d:BC_3YEAR m:type=\"Edm.Double\">4.28</d:BC_3YEAR>
<d:BC_5YEAR m:type=\"Edm.Double\">4.38</d:BC_5YEAR>
<d:BC_7YEAR m:type=\"Edm.Double\">4.50</d:BC_7YEAR>
<d:BC_10YEAR m:type=\"Edm.Double\">4.57</d:BC_10YEAR>
<d:BC_20YEAR m:type=\"Edm.Double\">4.85</d:BC_20YEAR>
<d:BC_30YEAR m:type=\"Edm.Double\">4.78</d:BC_30YEAR>
</m:properties>
</content>
</entry>
<entry>
<content type=\"application/xml\">
<m:properties>
<d:NEW_DATE>2025-01-03T00:00:00</d:NEW_DATE>
<d:BC_1MONTH m:type=\"Edm.Double\">4.32</d:BC_1MONTH>
<d:BC_2MONTH m:type=\"Edm.Double\">4.29</d:BC_2MONTH>
<d:BC_3MONTH m:type=\"Edm.Double\">4.30</d:BC_3MONTH>
<d:BC_6MONTH m:type=\"Edm.Double\">4.28</d:BC_6MONTH>
<d:BC_1YEAR m:type=\"Edm.Double\">4.18</d:BC_1YEAR>
<d:BC_2YEAR m:type=\"Edm.Double\">4.26</d:BC_2YEAR>
<d:BC_3YEAR m:type=\"Edm.Double\">4.29</d:BC_3YEAR>
<d:BC_5YEAR m:type=\"Edm.Double\">4.39</d:BC_5YEAR>
<d:BC_7YEAR m:type=\"Edm.Double\">4.51</d:BC_7YEAR>
<d:BC_10YEAR m:type=\"Edm.Double\">4.58</d:BC_10YEAR>
<d:BC_20YEAR m:type=\"Edm.Double\">4.86</d:BC_20YEAR>
<d:BC_30YEAR m:type=\"Edm.Double\">4.79</d:BC_30YEAR>
</m:properties>
</content>
</entry>
</feed>
''')
print('Wrote synthetic XML fixture')
"
```

- [ ] **1b: Create a CSV fixture**

```bash
uv run python -c "
import pathlib
pathlib.Path('tests/fixtures/ust_csv_sample.csv').write_text('''Date,1 Mo,2 Mo,3 Mo,4 Mo,6 Mo,1 Yr,2 Yr,3 Yr,5 Yr,7 Yr,10 Yr,20 Yr,30 Yr
01/02/2024,5.54,5.53,5.46,5.43,5.28,4.85,4.38,4.15,3.93,3.95,3.95,4.24,4.09
01/03/2024,5.54,5.52,5.46,5.42,5.30,4.88,4.40,4.19,3.97,3.99,4.00,4.28,4.14
01/04/2024,5.54,5.53,5.47,5.42,5.30,4.87,4.38,4.15,3.93,3.97,3.99,4.26,4.13
01/05/2024,,,,,,,,,,,,,,
''')
print('Wrote CSV fixture')
"
```

Note: The last row has all empty values (weekend/holiday) to test missing data handling.

#### Step 2: Write failing tests

- [ ] **2a: Write test_ust.py**

```python
"""Tests for the US Treasury fetcher."""

from datetime import date
from pathlib import Path

import polars as pl
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
```

- [ ] **2b: Run tests to verify they fail**

Run: `uv run pytest tests/test_ust.py -v`
Expected: FAIL — `ust` module doesn't exist yet

#### Step 3: Implement the UST fetcher

- [ ] **3a: Write ust.py**

```python
"""US Treasury yield curve fetcher.

Fetches Constant Maturity Treasury (CMT) par yield curve data from the
Treasury's XML feed (daily) and CSV archives (backfill).
"""

from __future__ import annotations

import io
import logging
import xml.etree.ElementTree as ET
from datetime import date

import polars as pl

from thalweg.config import UST_BASE_URL
from thalweg.fetchers.base import BaseFetcher

logger = logging.getLogger(__name__)

# XML element name → tenor in years
_XML_TENOR_MAP: dict[str, float] = {
    "BC_1MONTH": 1 / 12,
    "BC_2MONTH": 2 / 12,
    "BC_3MONTH": 3 / 12,
    "BC_4MONTH": 4 / 12,
    "BC_6MONTH": 6 / 12,
    "BC_1YEAR": 1.0,
    "BC_2YEAR": 2.0,
    "BC_3YEAR": 3.0,
    "BC_5YEAR": 5.0,
    "BC_7YEAR": 7.0,
    "BC_10YEAR": 10.0,
    "BC_20YEAR": 20.0,
    "BC_30YEAR": 30.0,
}

# CSV column name → tenor in years
_CSV_TENOR_MAP: dict[str, float] = {
    "1 Mo": 1 / 12,
    "2 Mo": 2 / 12,
    "3 Mo": 3 / 12,
    "4 Mo": 4 / 12,
    "6 Mo": 6 / 12,
    "1 Yr": 1.0,
    "2 Yr": 2.0,
    "3 Yr": 3.0,
    "5 Yr": 5.0,
    "7 Yr": 7.0,
    "10 Yr": 10.0,
    "20 Yr": 20.0,
    "30 Yr": 30.0,
}

# XML namespaces used in the Treasury OData feed
_NS = {
    "atom": "http://www.w3.org/2005/Atom",
    "d": "http://schemas.microsoft.com/ado/2007/08/dataservices",
    "m": "http://schemas.microsoft.com/ado/2007/08/dataservices/metadata",
}


class USTFetcher(BaseFetcher):
    """Fetches UST CMT par yield curve from Treasury XML/CSV feeds."""

    XML_TENOR_MAP = _XML_TENOR_MAP
    CSV_TENOR_MAP = _CSV_TENOR_MAP

    @property
    def name(self) -> str:
        return "ust"

    def _parse_xml(self, xml_text: str) -> pl.DataFrame:
        """Parse Treasury XML feed into a normalized DataFrame.

        Args:
            xml_text: Raw XML string from the Treasury feed.

        Returns:
            DataFrame with columns: date, currency, curve_type, tenor_years, yield_pct.
        """
        root = ET.fromstring(xml_text)
        rows: list[dict] = []

        for entry in root.findall("atom:entry", _NS):
            props = entry.find("atom:content/m:properties", _NS)
            if props is None:
                continue

            date_el = props.find("d:NEW_DATE", _NS)
            if date_el is None or date_el.text is None:
                continue
            # Date format: "2025-01-02T00:00:00" — take just the date part
            obs_date = date_el.text[:10]

            for xml_name, tenor in _XML_TENOR_MAP.items():
                el = props.find(f"d:{xml_name}", _NS)
                if el is None or el.text is None:
                    continue
                try:
                    yield_pct = float(el.text)
                except ValueError:
                    continue
                rows.append({
                    "date": obs_date,
                    "currency": "USD",
                    "curve_type": "govt_par",
                    "tenor_years": tenor,
                    "yield_pct": yield_pct,
                })

        if not rows:
            return pl.DataFrame(schema={
                "date": pl.Date,
                "currency": pl.Utf8,
                "curve_type": pl.Utf8,
                "tenor_years": pl.Float64,
                "yield_pct": pl.Float64,
            })

        return pl.DataFrame(rows).cast({"date": pl.Date, "tenor_years": pl.Float64})

    def _parse_csv(self, csv_text: str) -> pl.DataFrame:
        """Parse Treasury CSV data into a normalized DataFrame.

        Args:
            csv_text: Raw CSV string.

        Returns:
            DataFrame with columns: date, currency, curve_type, tenor_years, yield_pct.
        """
        raw = pl.read_csv(io.StringIO(csv_text), infer_schema_length=0)

        rows: list[dict] = []
        date_col = raw.columns[0]  # "Date"

        for row in raw.iter_rows(named=True):
            raw_date = row[date_col]
            if not raw_date:
                continue

            # Handle both MM/DD/YYYY and YYYY-MM-DD formats
            if "/" in raw_date:
                parts = raw_date.split("/")
                obs_date = f"{parts[2]}-{parts[0].zfill(2)}-{parts[1].zfill(2)}"
            else:
                obs_date = raw_date

            for csv_col, tenor in _CSV_TENOR_MAP.items():
                val = row.get(csv_col)
                if val is None or val == "" or val == "N/A":
                    continue
                try:
                    yield_pct = float(val)
                except ValueError:
                    continue
                rows.append({
                    "date": obs_date,
                    "currency": "USD",
                    "curve_type": "govt_par",
                    "tenor_years": tenor,
                    "yield_pct": yield_pct,
                })

        if not rows:
            return pl.DataFrame(schema={
                "date": pl.Date,
                "currency": pl.Utf8,
                "curve_type": pl.Utf8,
                "tenor_years": pl.Float64,
                "yield_pct": pl.Float64,
            })

        return pl.DataFrame(rows).cast({"date": pl.Date, "tenor_years": pl.Float64})

    async def fetch_latest(self) -> pl.DataFrame:
        """Fetch the current year's XML and return the most recent entries."""
        today = date.today()
        url = UST_BASE_URL
        params = {
            "data": "daily_treasury_yield_curve",
            "field_tdr_date_value": str(today.year),
        }

        async with self._get_client() as client:
            resp = await client.get(url, params=params)
            resp.raise_for_status()
            raw = resp.content
            self.save_raw(raw, "xml")
            xml_text = resp.text

        df = self._parse_xml(xml_text)
        if df.shape[0] == 0:
            logger.warning("No UST data found for %d", today.year)
            return df

        # Return only the most recent date
        latest = df["date"].max()
        result = df.filter(pl.col("date") == latest)
        logger.info("Fetched %d rows for UST latest (%s)", result.shape[0], latest)
        return result

    async def backfill(self, start_date: date, end_date: date) -> pl.DataFrame:
        """Fetch historical UST data using CSV (per-year) and XML feeds."""
        all_frames: list[pl.DataFrame] = []

        for year in range(start_date.year, end_date.year + 1):
            logger.info("UST backfill: year %d", year)

            csv_url = (
                "https://home.treasury.gov/resource-center/data-chart-center"
                "/interest-rates/daily-treasury-rates.csv"
                f"/{year}/all"
                "?type=daily_treasury_yield_curve"
                f"&field_tdr_date_value={year}"
                "&page&_format=csv"
            )

            async with self._get_client() as client:
                resp = await client.get(csv_url)

                if resp.status_code == 200 and "Date" in resp.text[:100]:
                    df = self._parse_csv(resp.text)
                else:
                    # Fall back to XML
                    logger.info("  CSV not available for %d, trying XML", year)
                    xml_url = UST_BASE_URL
                    params = {
                        "data": "daily_treasury_yield_curve",
                        "field_tdr_date_value": str(year),
                    }
                    resp = await client.get(xml_url, params=params)
                    resp.raise_for_status()
                    df = self._parse_xml(resp.text)

            if df.shape[0] > 0:
                all_frames.append(df)
                logger.info("  → %d rows for %d", df.shape[0], year)

        if not all_frames:
            return self._parse_csv("")

        result = pl.concat(all_frames)

        # Filter to requested date range
        result = result.filter(
            (pl.col("date") >= start_date) & (pl.col("date") <= end_date)
        )
        return result
```

- [ ] **3b: Run tests to verify they pass**

Run: `uv run pytest tests/test_ust.py -v`
Expected: All 6 tests PASS

- [ ] **3c: Commit**

```bash
git add src/thalweg/fetchers/ust.py tests/test_ust.py tests/fixtures/ust_xml_sample.xml tests/fixtures/ust_csv_sample.csv
git commit -m "feat: US Treasury CMT yield curve fetcher with tests"
```

---

### Task 4: CLI Wiring

**Files:**
- Modify: `src/thalweg/cli.py`
- Modify: `src/thalweg/fetchers/__init__.py`

#### Step 1: Wire fetch and backfill commands

- [ ] **1a: Update fetchers/__init__.py with a registry**

```python
"""Data source fetchers."""

from thalweg.fetchers.boc import BoCFetcher
from thalweg.fetchers.ust import USTFetcher

FETCHERS = {
    "boc": BoCFetcher,
    "ust": USTFetcher,
}

__all__ = ["FETCHERS", "BoCFetcher", "USTFetcher"]
```

- [ ] **1b: Rewrite cli.py with real implementations**

```python
"""CLI entry point for Thalweg."""

from __future__ import annotations

import asyncio
import logging
from datetime import date

import click

from thalweg import __version__

logger = logging.getLogger(__name__)


def _setup_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)-8s %(name)s — %(message)s",
        datefmt="%H:%M:%S",
    )


@click.group()
@click.version_option(version=__version__, prog_name="thalweg")
def cli() -> None:
    """Thalweg — yield curve observatory."""
    _setup_logging()


@cli.command()
@click.option(
    "--source",
    type=click.Choice(["boc", "ust", "all"]),
    default="all",
    help="Data source to fetch.",
)
def fetch(source: str) -> None:
    """Fetch latest yield curve data."""
    from thalweg import storage
    from thalweg.fetchers import FETCHERS

    async def _run() -> None:
        sources = list(FETCHERS) if source == "all" else [source]
        for src in sources:
            fetcher = FETCHERS[src]()
            click.echo(f"Fetching latest from {src}...")
            df = await fetcher.fetch_latest()
            if df.shape[0] == 0:
                click.echo(f"  No data returned from {src}")
                continue
            storage.append_curves(df)
            click.echo(f"  Stored {df.shape[0]} rows")

    asyncio.run(_run())


@cli.command()
@click.option(
    "--source",
    type=click.Choice(["boc", "ust", "all"]),
    required=True,
    help="Data source to backfill.",
)
@click.option("--start", required=True, type=click.DateTime(formats=["%Y-%m-%d"]),
              help="Start date (YYYY-MM-DD).")
@click.option("--end", required=True, type=click.DateTime(formats=["%Y-%m-%d"]),
              help="End date (YYYY-MM-DD).")
def backfill(source: str, start: click.DateTime, end: click.DateTime) -> None:
    """Backfill historical yield curve data."""
    from thalweg import storage
    from thalweg.fetchers import FETCHERS

    start_date = start.date()
    end_date = end.date()

    async def _run() -> None:
        sources = list(FETCHERS) if source == "all" else [source]
        for src in sources:
            fetcher = FETCHERS[src]()
            click.echo(f"Backfilling {src} from {start_date} to {end_date}...")
            df = await fetcher.backfill(start_date, end_date)
            if df.shape[0] == 0:
                click.echo(f"  No data returned from {src}")
                continue
            storage.append_curves(df)
            click.echo(f"  Stored {df.shape[0]} rows")

    asyncio.run(_run())


@cli.command()
def analyze() -> None:
    """Recompute derived analytics (spreads, slopes, regimes)."""
    click.echo("analyze: not implemented yet")


@cli.command()
@click.option("--port", default=8001, help="Port to serve on.")
def serve(port: int) -> None:
    """Start the Thalweg web server."""
    click.echo(f"serve --port {port}: not implemented yet")


@cli.command()
def status() -> None:
    """Show data status (latest dates, row counts)."""
    from thalweg.config import CURVES_DIR

    import polars as pl

    parquet_files = sorted(CURVES_DIR.glob("*.parquet"))
    if not parquet_files:
        click.echo("No data files found.")
        return

    click.echo(f"{'File':<25} {'Latest Date':<15} {'Rows':>10}")
    click.echo("-" * 52)
    for path in parquet_files:
        df = pl.read_parquet(path)
        latest = df["date"].max()
        rows = df.shape[0]
        click.echo(f"{path.name:<25} {str(latest):<15} {rows:>10,}")
```

- [ ] **1c: Verify CLI help still works**

Run: `uv run thalweg --help`
Expected: Shows all commands

Run: `uv run thalweg fetch --help`
Expected: Shows `--source` with choices `boc`, `ust`, `all`

- [ ] **1d: Commit**

```bash
git add src/thalweg/cli.py src/thalweg/fetchers/__init__.py
git commit -m "feat: wire CLI fetch, backfill, and status commands"
```

---

### Task 5: Integration Test — Fetch Latest

- [ ] **5a: Test live fetch from BoC**

Run: `uv run thalweg fetch --source boc`
Expected output:
```
Fetching latest from boc...
  Stored 6 rows
```

Run: `uv run thalweg status`
Expected: Shows `gov_cad.parquet` with today's (or most recent business day's) date.

- [ ] **5b: Test live fetch from UST**

Run: `uv run thalweg fetch --source ust`
Expected output:
```
Fetching latest from ust...
  Stored 13 rows
```

Run: `uv run thalweg status`
Expected: Shows both `gov_cad.parquet` and `gov_usd.parquet`.

- [ ] **5c: Commit data directory note (no data committed)**

Verify that `data/` is gitignored:
```bash
git status
```
Expected: no files from `data/` appear.

---

### Task 6: Backfill 10+ Years

- [ ] **6a: Backfill BoC data**

```bash
uv run thalweg backfill --source boc --start 2014-01-01 --end 2026-03-28
```

Expected: Logs yearly chunks, stores ~15,000 rows total.

- [ ] **6b: Backfill UST data**

```bash
uv run thalweg backfill --source ust --start 2014-01-01 --end 2026-03-28
```

Expected: Logs yearly CSV/XML fetches, stores ~30,000+ rows total.

- [ ] **6c: Verify with status**

```bash
uv run thalweg status
```

Expected output (approximate):
```
File                      Latest Date     Rows
----------------------------------------------------
gov_cad.parquet           2026-03-27        15,000+
gov_usd.parquet           2026-03-27        30,000+
```

- [ ] **6d: Spot-check a known date**

```bash
uv run python -c "
from thalweg.storage import read_curves
from datetime import date
df = read_curves('USD', 'govt_par', start_date=date(2024, 1, 2), end_date=date(2024, 1, 2))
print(df.sort('tenor_years'))
"
```

Expected: ~13 rows for Jan 2, 2024 with reasonable yield values (10yr ~3.95%).

---

### Task 7: Run Full Test Suite

- [ ] **7a: Run all tests**

```bash
uv run pytest tests/ -v
```

Expected: All tests pass (storage, boc, ust).

- [ ] **7b: Run linter**

```bash
uv run ruff check src/ tests/
```

Expected: No errors.

- [ ] **7c: Final commit and push**

```bash
git add -A
git status  # verify nothing unexpected
git commit -m "test: fixture files for BoC and UST fetchers"
git push
```

---

## Exit Criteria

- [ ] `uv run thalweg fetch --source boc` fetches today's GoC benchmark yields
- [ ] `uv run thalweg fetch --source ust` fetches today's UST CMT curve
- [ ] `uv run thalweg status` shows parquet files with 10+ years of data
- [ ] `uv run pytest` passes all tests
- [ ] `uv run ruff check src/ tests/` passes clean
- [ ] Parquet files follow the schema: date, currency, curve_type, tenor_years, yield_pct
