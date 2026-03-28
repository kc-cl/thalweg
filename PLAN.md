# Phase 0: Data Foundation — Implementation Plan

Goal: 10+ years of daily GoC and UST yield curves in parquet files, accessible via CLI.

## Task Order

Tasks are dependency-ordered. Storage comes first (everything depends on it), then fetchers (independent of each other), then CLI wiring, backfill, and tests.

```
1. Storage Layer ──┬── 2. BoC Fetcher ──┬── 4. CLI Wiring ── 5. Backfill
                   └── 3. UST Fetcher ──┘                    6. Tests (alongside each task)
```

---

## Task 1: Parquet Storage Layer

**Files:** `src/thalweg/storage.py`

**What to build:**

Three core functions for curve data:

```python
def append_curves(df: pl.DataFrame) -> None
```
- Determine target parquet file from `currency` and `curve_type` columns
  - Routing: `gov_cad.parquet`, `gov_usd.parquet`, `swap_usd.parquet`, etc.
  - Pattern: `{curve_type_prefix}_{currency.lower()}.parquet` where govt_zero/govt_par → `gov_`, swap → `swap_`
- If file exists: read it, concatenate with new data, deduplicate on `(date, currency, curve_type, tenor_years)` keeping last, write back
- If file doesn't exist: write directly
- Validate schema before writing: columns must be `date` (Date), `currency` (Utf8), `curve_type` (Utf8), `tenor_years` (Float64), `yield_pct` (Float64)

```python
def read_curves(
    currency: str | None = None,
    curve_type: str | None = None,
    start_date: date | None = None,
    end_date: date | None = None,
) -> pl.DataFrame
```
- Use `pl.scan_parquet()` for lazy reads with filter pushdown
- If currency/curve_type specified, read only the relevant parquet file
- If not specified, scan all parquet files in `CURVES_DIR`
- Apply date filters

```python
def get_latest_date(currency: str, curve_type: str) -> date | None
```
- Read the max `date` from the relevant parquet file
- Return None if file doesn't exist

**Done test:**
- Create a synthetic DataFrame with known values
- `append_curves()` writes it
- `read_curves()` reads it back with matching values
- Append duplicate + new rows → dedup works correctly
- `get_latest_date()` returns the expected date

---

## Task 2: BoC Fetcher

**Files:** `src/thalweg/fetchers/boc.py`

**API details — Benchmark bond yields:**

- Endpoint: `GET https://www.bankofcanada.ca/valet/observations/group/bond_yields_benchmark/json`
- Params: `start_date=YYYY-MM-DD`, `end_date=YYYY-MM-DD`
- No auth required

Response structure:
```json
{
  "observations": [
    {
      "d": "2026-03-27",
      "BD.CDN.2YR.DQ.YLD": {"v": "3.85"},
      "BD.CDN.3YR.DQ.YLD": {"v": "3.92"},
      "BD.CDN.5YR.DQ.YLD": {"v": "3.45"},
      "BD.CDN.7YR.DQ.YLD": {"v": "3.38"},
      "BD.CDN.10YR.DQ.YLD": {"v": "3.25"},
      "BD.CDN.LONG.DQ.YLD": {"v": "3.10"}
    }
  ]
}
```

Tenor mapping:
| Series                  | tenor_years |
|-------------------------|-------------|
| BD.CDN.2YR.DQ.YLD      | 2.0         |
| BD.CDN.3YR.DQ.YLD      | 3.0         |
| BD.CDN.5YR.DQ.YLD      | 5.0         |
| BD.CDN.7YR.DQ.YLD      | 7.0         |
| BD.CDN.10YR.DQ.YLD     | 10.0        |
| BD.CDN.LONG.DQ.YLD     | 30.0        |

Output: `currency="CAD"`, `curve_type="govt_par"` (these are benchmark par yields, not zero-coupon).

**Zero-coupon curve (stretch goal for Task 2):**
- Need to discover the group name via `GET /valet/groups/json`
- Zero-coupon curves are published weekly (Thursdays, 2-week lag) — different cadence than daily benchmarks
- Will have more tenors (0.25yr through 30yr in fine increments)
- Output: `currency="CAD"`, `curve_type="govt_zero"`

**Implementation:**

```python
class BoCFetcher(BaseFetcher):
    name = "boc"

    async def fetch_latest(self) -> pl.DataFrame:
        # Fetch benchmark yields for today (or most recent business day)
        # Parse JSON, normalize to common schema
        ...

    async def backfill(self, start_date, end_date) -> pl.DataFrame:
        # Chunk by year to avoid huge responses
        # For each year chunk: fetch, parse, collect
        # Concatenate all chunks
        ...
```

**Backfill strategy:**
- Chunk requests by year (e.g., 2014-01-01 to 2014-12-31)
- BoC Valet handles large date ranges but chunking is polite
- Data available back to ~1993 for benchmark yields

**Done test:**
- `fetch_latest()` returns a DataFrame with correct schema and 6 rows (one per tenor)
- `backfill("2024-01-01", "2024-01-31")` returns ~132 rows (22 trading days × 6 tenors)
- Saved fixture: one day's raw JSON response, parseable offline

---

## Task 3: UST Fetcher

**Files:** `src/thalweg/fetchers/ust.py`

**API details — Daily fetch (XML):**

- Endpoint: `GET https://home.treasury.gov/resource-center/data-chart-center/interest-rates/pages/xml`
- Params: `data=daily_treasury_yield_curve`, `field_tdr_date_value={year}`
- Returns XML with `<entry>` elements, one per trading day

Key XML fields per entry:
| XML Element     | tenor_years |
|-----------------|-------------|
| `BC_1MONTH`     | 0.083       |
| `BC_2MONTH`     | 0.167       |
| `BC_3MONTH`     | 0.25        |
| `BC_4MONTH`     | 0.333       |
| `BC_6MONTH`     | 0.5         |
| `BC_1YEAR`      | 1.0         |
| `BC_2YEAR`      | 2.0         |
| `BC_3YEAR`      | 3.0         |
| `BC_5YEAR`      | 5.0         |
| `BC_7YEAR`      | 7.0         |
| `BC_10YEAR`     | 10.0        |
| `BC_20YEAR`     | 20.0        |
| `BC_30YEAR`     | 30.0        |

Date field: `d:NEW_DATE`

Missing data: elements are simply absent from the XML (not null-flagged).

Output: `currency="USD"`, `curve_type="govt_par"` (CMT is a par yield curve).

**API details — Backfill (CSV archive):**

- URL: `https://home.treasury.gov/interest-rates-data-csv-archive`
- Files like `yield-curve-rates-1990-2024.csv`
- CSV columns: `Date,1 Mo,2 Mo,3 Mo,4 Mo,6 Mo,1 Yr,2 Yr,3 Yr,5 Yr,7 Yr,10 Yr,20 Yr,30 Yr`
- Date format: MM/DD/YYYY
- Missing data: empty strings
- Much simpler than paginated XML — use CSV for backfill, XML for daily

**Implementation:**

```python
class USTFetcher(BaseFetcher):
    name = "ust"

    async def fetch_latest(self) -> pl.DataFrame:
        # Fetch current year XML, parse last entry (or entries for recent days)
        ...

    async def backfill(self, start_date, end_date) -> pl.DataFrame:
        # Download CSV archive, parse, filter to date range
        # For dates in current year not in archive: fall back to XML
        ...
```

**Backfill strategy:**
- Download the CSV archive file (covers 1990–2024)
- For 2025+ data: use XML feed with year parameter
- Parse CSV with polars (fast, handles empty values)
- Filter to requested date range

**Done test:**
- `fetch_latest()` returns DataFrame with correct schema, ~13 rows (one per tenor)
- `backfill("2024-01-01", "2024-01-31")` returns ~286 rows (22 days × 13 tenors)
- Saved fixtures: one day's raw XML, one month's raw CSV

---

## Task 4: CLI Wiring

**Files:** `src/thalweg/cli.py` (modify existing stubs)

**What to build:**

Wire the stub commands to real implementations:

```bash
# Fetch latest data
thalweg fetch --source boc    # → BoCFetcher.fetch_latest() + storage.append_curves()
thalweg fetch --source ust    # → USTFetcher.fetch_latest() + storage.append_curves()
thalweg fetch --source all    # → fetch both

# Backfill historical data
thalweg backfill --source boc --start 2014-01-01 --end 2026-03-28
thalweg backfill --source ust --start 2014-01-01 --end 2026-03-28

# Check status
thalweg status                # → list parquet files, latest date, row count per file
```

**Bridge async to sync:** Click commands are sync. Use `asyncio.run()` to call async fetchers.

**Status output format:**
```
Source    File              Latest Date    Rows
------    ----              -----------    ----
CAD gov   gov_cad.parquet   2026-03-27     15,000
USD gov   gov_usd.parquet   2026-03-27     82,000
```

**Done test:**
- `thalweg fetch --source boc` runs without error, creates/updates `data/curves/gov_cad.parquet`
- `thalweg status` shows the file with correct latest date
- `thalweg backfill --source ust --start 2024-01-01 --end 2024-01-31` populates data

---

## Task 5: Backfill 10+ Years

**What to do:**

Run the backfill commands to populate the full dataset:

```bash
thalweg backfill --source boc --start 2014-01-01 --end 2026-03-28
thalweg backfill --source ust --start 2014-01-01 --end 2026-03-28
```

**Expected results:**
- `gov_cad.parquet`: ~2,500 trading days × 6 tenors = ~15,000 rows
- `gov_usd.parquet`: ~2,500 trading days × 13 tenors = ~32,500 rows
- Date range: 2014-01-02 through most recent available

**Done test:**
- `thalweg status` shows both files with 10+ years of data
- Spot-check: query a known historical date and verify yields match published values
- File sizes are reasonable (parquet compression: expect < 5MB each)

---

## Task 6: Tests

**Files:** `tests/test_storage.py`, `tests/test_boc.py`, `tests/test_ust.py`, `tests/fixtures/`

**test_storage.py:**
- `test_append_and_read_roundtrip` — write synthetic data, read it back
- `test_append_deduplicates` — append overlapping data, verify no duplicates
- `test_read_with_date_filter` — filter by date range
- `test_get_latest_date` — returns correct max date
- `test_get_latest_date_missing_file` — returns None

**test_boc.py:**
- Save a real BoC API response to `tests/fixtures/boc_benchmark_sample.json`
- `test_parse_benchmark_yields` — parse the fixture, verify schema and values
- `test_tenor_mapping` — verify all tenors map correctly

**test_ust.py:**
- Save a real UST XML response to `tests/fixtures/ust_xml_sample.xml`
- Save a CSV snippet to `tests/fixtures/ust_csv_sample.csv`
- `test_parse_xml_response` — parse XML fixture, verify schema
- `test_parse_csv_response` — parse CSV fixture, verify schema
- `test_missing_values_handled` — verify NaN/missing tenors don't crash

**Done test:** `uv run pytest` passes all tests.

---

## Exit Criteria (Phase 0 Complete)

- [ ] `uv run thalweg fetch --source boc` fetches today's GoC benchmark yields
- [ ] `uv run thalweg fetch --source ust` fetches today's UST CMT curve
- [ ] `uv run thalweg status` shows parquet files with 10+ years of data
- [ ] `uv run pytest` passes all tests
- [ ] Parquet files follow the schema: date, currency, curve_type, tenor_years, yield_pct
