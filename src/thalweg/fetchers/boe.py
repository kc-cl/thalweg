"""Bank of England gilt nominal par yield fetcher.

Fetches UK gilt nominal par yield data (5yr, 10yr, 20yr) from the
Bank of England IADB (Interactive Analytical DataBase) via its CSV export API.
"""

from __future__ import annotations

import io
import logging
from datetime import date, timedelta

import httpx
import polars as pl

from thalweg.fetchers.base import BaseFetcher

logger = logging.getLogger(__name__)

# Old IADB URL — works reliably without bot blocking
_IADB_URL = "http://www.bankofengland.co.uk/boeapps/iadb/fromshowcolumns.asp"

# Series code -> tenor in years
_TENOR_MAP: dict[str, float] = {
    "IUDSNPY": 5.0,
    "IUDMNPY": 10.0,
    "IUDLNPY": 20.0,
}

# Series codes as a comma-separated string for the API
_SERIES_CODES = ",".join(_TENOR_MAP.keys())


class BoEFetcher(BaseFetcher):
    """Fetches UK gilt nominal par yields from the BoE IADB CSV API."""

    TENOR_MAP = _TENOR_MAP

    @property
    def name(self) -> str:
        return "boe"

    @staticmethod
    def _format_date(d: date) -> str:
        """Convert a date to BoE's ``DD/Mon/YYYY`` format.

        Args:
            d: The date to format.

        Returns:
            Date string like ``"01/Jan/2025"``.
        """
        return d.strftime("%d/%b/%Y")

    def _parse_csv(self, csv_text: str) -> pl.DataFrame:
        """Parse BoE IADB CSV response into a normalized DataFrame.

        The CSV has columns DATE, IUDSNPY, IUDMNPY, IUDLNPY in wide format.
        This melts it to long format matching the standard schema.

        Args:
            csv_text: Raw CSV string from the BoE IADB API.

        Returns:
            DataFrame with columns: date, currency, curve_type, tenor_years, yield_pct.
        """
        empty_schema = {
            "date": pl.Date,
            "currency": pl.Utf8,
            "curve_type": pl.Utf8,
            "tenor_years": pl.Float64,
            "yield_pct": pl.Float64,
        }

        if not csv_text.strip():
            return pl.DataFrame(schema=empty_schema)

        raw = pl.read_csv(io.StringIO(csv_text), infer_schema_length=0)

        if "DATE" not in raw.columns:
            logger.warning("BoE CSV missing DATE column: %s", raw.columns)
            return pl.DataFrame(schema=empty_schema)

        # Identify which tenor columns are present
        tenor_cols = [c for c in raw.columns if c in _TENOR_MAP]
        if not tenor_cols:
            logger.warning("BoE CSV has no recognized tenor columns: %s", raw.columns)
            return pl.DataFrame(schema=empty_schema)

        rows: list[dict] = []
        for row in raw.iter_rows(named=True):
            date_str = row.get("DATE", "")
            if not date_str:
                continue

            # Parse "DD Mon YYYY" date format
            try:
                parsed_date = _parse_boe_date(date_str)
            except ValueError:
                logger.warning("Could not parse BoE date: %r", date_str)
                continue

            for series in tenor_cols:
                value = row.get(series, "")
                if not value or not str(value).strip():
                    continue
                try:
                    yield_pct = float(value)
                except ValueError:
                    logger.warning(
                        "Invalid yield value for %s on %s: %r", series, date_str, value
                    )
                    continue

                rows.append({
                    "date": parsed_date,
                    "currency": "GBP",
                    "curve_type": "govt_par",
                    "tenor_years": _TENOR_MAP[series],
                    "yield_pct": yield_pct,
                })

        if not rows:
            return pl.DataFrame(schema=empty_schema)

        return pl.DataFrame(rows).cast({"date": pl.Date, "tenor_years": pl.Float64})

    def _build_params(self, start_date: date, end_date: date) -> dict[str, str]:
        """Build query parameters for the IADB CSV request.

        Args:
            start_date: First date (inclusive).
            end_date: Last date (inclusive).

        Returns:
            Dictionary of query parameters.
        """
        return {
            "csv.x": "yes",
            "SeriesCodes": _SERIES_CODES,
            "Datefrom": self._format_date(start_date),
            "Dateto": self._format_date(end_date),
            "CSVF": "TN",
            "UsingCodes": "Y",
            "VPD": "Y",
            "VFD": "N",
        }

    def _get_client(self) -> httpx.AsyncClient:
        """Create an httpx client with a browser-like User-Agent for BoE."""
        transport = httpx.AsyncHTTPTransport(retries=3)
        return httpx.AsyncClient(
            transport=transport,
            timeout=httpx.Timeout(30.0),
            follow_redirects=True,
            headers={
                "User-Agent": (
                    "Mozilla/5.0 (compatible; Thalweg/0.1; "
                    "+https://github.com/kc-cl/thalweg)"
                ),
                "Accept": "text/csv, text/plain, */*",
            },
        )

    async def fetch_latest(self) -> pl.DataFrame:
        """Fetch the most recent BoE gilt nominal par yields.

        Requests the last 7 days of data to account for weekends and
        holidays, then filters to the latest available date.
        """
        end = date.today()
        start = end - timedelta(days=7)
        params = self._build_params(start, end)

        async with self._get_client() as client:
            resp = await client.get(_IADB_URL, params=params)
            resp.raise_for_status()
            raw = resp.content
            self.save_raw(raw, "csv")
            csv_text = resp.text

        df = self._parse_csv(csv_text)
        if df.shape[0] == 0:
            logger.warning("No BoE data returned for latest fetch")
            return df

        latest = df["date"].max()
        result = df.filter(pl.col("date") == latest)
        logger.info("Fetched %d rows for BoE latest (%s)", result.shape[0], latest)
        return result

    async def backfill(self, start_date: date, end_date: date) -> pl.DataFrame:
        """Fetch historical BoE gilt nominal par yields for the given date range.

        Chunks requests by year since the BoE API may not handle very large
        date ranges reliably.

        Args:
            start_date: First date (inclusive).
            end_date: Last date (inclusive).

        Returns:
            DataFrame with columns: date, currency, curve_type, tenor_years, yield_pct.
        """
        all_frames: list[pl.DataFrame] = []

        current = start_date
        while current <= end_date:
            chunk_end = min(date(current.year, 12, 31), end_date)
            logger.info("BoE backfill: %s to %s", current, chunk_end)

            params = self._build_params(current, chunk_end)

            async with self._get_client() as client:
                resp = await client.get(_IADB_URL, params=params)
                if resp.status_code == 403:
                    logger.warning("  BoE returned 403 for %d — skipping", current.year)
                    current = date(current.year + 1, 1, 1)
                    continue
                resp.raise_for_status()
                csv_text = resp.text

            df = self._parse_csv(csv_text)
            if df.shape[0] > 0:
                all_frames.append(df)
                logger.info("  -> %d rows", df.shape[0])

            current = date(current.year + 1, 1, 1)

        if not all_frames:
            return self._parse_csv("")

        return pl.concat(all_frames)


def _parse_boe_date(date_str: str) -> date:
    """Parse a BoE date string in ``DD Mon YYYY`` format.

    Args:
        date_str: Date string like ``"02 Jan 2025"``.

    Returns:
        Parsed date object.

    Raises:
        ValueError: If the string cannot be parsed.
    """
    from datetime import datetime

    return datetime.strptime(date_str.strip(), "%d %b %Y").date()
