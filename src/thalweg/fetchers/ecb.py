"""ECB AAA government yield curve fetcher.

Fetches Euro area AAA-rated government zero-coupon yield curve data from the
ECB Statistical Data Warehouse via the SDMX CSV API.  The curve is estimated
by the ECB using a Svensson model fitted to AAA-rated nominal government bonds.
"""

from __future__ import annotations

import io
import logging
from datetime import date

import polars as pl

from thalweg.config import ECB_BASE_URL
from thalweg.fetchers.base import BaseFetcher

logger = logging.getLogger(__name__)

# SDMX tenor suffix -> tenor in years
_TENOR_MAP: dict[str, float] = {
    "SR_3M": 0.25,
    "SR_1Y": 1.0,
    "SR_2Y": 2.0,
    "SR_3Y": 3.0,
    "SR_5Y": 5.0,
    "SR_7Y": 7.0,
    "SR_10Y": 10.0,
    "SR_20Y": 20.0,
    "SR_30Y": 30.0,
}


class ECBFetcher(BaseFetcher):
    """Fetches Euro area AAA zero-coupon yield curve from the ECB SDMX CSV API."""

    TENOR_MAP = _TENOR_MAP

    @property
    def name(self) -> str:
        return "ecb"

    def _build_url(self, tenor_keys: list[str] | None = None) -> str:
        """Construct the SDMX data URL for the given tenor keys.

        Args:
            tenor_keys: List of tenor suffixes (e.g. ``["SR_3M", "SR_10Y"]``).
                If ``None``, all tenors in ``_TENOR_MAP`` are requested.

        Returns:
            Full URL for the ECB SDMX CSV endpoint.
        """
        if tenor_keys is None:
            tenor_keys = list(_TENOR_MAP.keys())
        joined = "+".join(tenor_keys)
        return f"{ECB_BASE_URL}/YC/B.U2.EUR.4F.G_N_A.SV_C_YM.{joined}"

    def _parse_csv(self, csv_text: str) -> pl.DataFrame:
        """Parse ECB SDMX CSV response into a normalized DataFrame.

        Args:
            csv_text: Raw CSV string from the ECB SDMX API.

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

        # Validate expected columns are present
        if "KEY" not in raw.columns or "TIME_PERIOD" not in raw.columns:
            logger.warning("ECB CSV missing expected columns: %s", raw.columns)
            return pl.DataFrame(schema=empty_schema)

        rows: list[dict] = []
        for row in raw.iter_rows(named=True):
            key = row.get("KEY", "")
            time_period = row.get("TIME_PERIOD", "")
            obs_value = row.get("OBS_VALUE", "")

            if not key or not time_period or not obs_value:
                continue

            # Extract tenor suffix from key: "YC.B.U2.EUR.4F.G_N_A.SV_C_YM.SR_10Y" -> "SR_10Y"
            tenor_key = key.rsplit(".", maxsplit=1)[-1]
            tenor_years = _TENOR_MAP.get(tenor_key)
            if tenor_years is None:
                logger.debug("Unknown tenor key in ECB data: %s", tenor_key)
                continue

            try:
                yield_pct = float(obs_value)
            except ValueError:
                logger.warning(
                    "Invalid OBS_VALUE for %s on %s: %r", tenor_key, time_period, obs_value
                )
                continue

            rows.append({
                "date": time_period,
                "currency": "EUR",
                "curve_type": "govt_zero",
                "tenor_years": tenor_years,
                "yield_pct": yield_pct,
            })

        if not rows:
            return pl.DataFrame(schema=empty_schema)

        return pl.DataFrame(rows).cast({"date": pl.Date, "tenor_years": pl.Float64})

    async def fetch_latest(self) -> pl.DataFrame:
        """Fetch the most recent ECB AAA yield curve observations."""
        url = self._build_url()
        params = {"format": "csvdata", "lastNObservations": "5"}

        async with self._get_client() as client:
            resp = await client.get(url, params=params)
            resp.raise_for_status()
            raw = resp.content
            self.save_raw(raw, "csv")
            csv_text = resp.text

        df = self._parse_csv(csv_text)
        if df.shape[0] == 0:
            logger.warning("No ECB data returned for latest fetch")
            return df

        # Return only the most recent date
        latest = df["date"].max()
        result = df.filter(pl.col("date") == latest)
        logger.info("Fetched %d rows for ECB latest (%s)", result.shape[0], latest)
        return result

    async def backfill(self, start_date: date, end_date: date) -> pl.DataFrame:
        """Fetch historical ECB AAA yield curve data for the given date range.

        The ECB API handles large date ranges in a single request, so no
        chunking is required.

        Args:
            start_date: First date (inclusive).
            end_date: Last date (inclusive).

        Returns:
            DataFrame with columns: date, currency, curve_type, tenor_years, yield_pct.
        """
        url = self._build_url()
        params = {
            "format": "csvdata",
            "startPeriod": start_date.isoformat(),
            "endPeriod": end_date.isoformat(),
        }

        logger.info("ECB backfill: %s to %s", start_date, end_date)

        async with self._get_client() as client:
            resp = await client.get(url, params=params)
            resp.raise_for_status()
            csv_text = resp.text

        df = self._parse_csv(csv_text)
        logger.info("ECB backfill: %d rows", df.shape[0])
        return df
