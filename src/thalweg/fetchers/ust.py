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

# XML element name -> tenor in years
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

# CSV column name -> tenor in years
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
            # Date format: "2025-01-02T00:00:00" -- take just the date part
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
        if not csv_text.strip():
            return pl.DataFrame(schema={
                "date": pl.Date,
                "currency": pl.Utf8,
                "curve_type": pl.Utf8,
                "tenor_years": pl.Float64,
                "yield_pct": pl.Float64,
            })

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
                logger.info("  -> %d rows for %d", df.shape[0], year)

        if not all_frames:
            return self._parse_csv("")

        result = pl.concat(all_frames)

        # Filter to requested date range
        result = result.filter(
            (pl.col("date") >= start_date) & (pl.col("date") <= end_date)
        )
        return result
