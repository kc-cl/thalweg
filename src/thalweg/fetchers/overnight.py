"""Overnight rates fetcher for CORRA, SOFR, eSTR, and SONIA.

Fetches overnight benchmark rates from four central bank sources and
returns them in the common rate schema (date, rate_name, value_pct).
"""

from __future__ import annotations

import io
import logging
from datetime import date, timedelta

import httpx
import polars as pl

from thalweg.config import BOC_BASE_URL, ECB_BASE_URL
from thalweg.fetchers.base import BaseFetcher

logger = logging.getLogger(__name__)

_NYFED_BASE_URL = "https://markets.newyorkfed.org/api/rates/secured/sofr"
_BOE_IADB_URL = "http://www.bankofengland.co.uk/boeapps/iadb/fromshowcolumns.asp"

_EMPTY_RATE_SCHEMA = {"date": pl.Date, "rate_name": pl.Utf8, "value_pct": pl.Float64}


class OvernightRatesFetcher(BaseFetcher):
    """Fetches overnight benchmark rates from CORRA, SOFR, eSTR, and SONIA sources."""

    @property
    def name(self) -> str:
        return "overnight"

    # ------------------------------------------------------------------
    # Parsers
    # ------------------------------------------------------------------

    @staticmethod
    def _parse_corra_json(data: dict) -> pl.DataFrame:
        """Parse BoC Valet JSON for the CORRA overnight rate.

        Args:
            data: Parsed JSON from the BoC Valet API containing an
                'observations' key.

        Returns:
            DataFrame with columns: date, rate_name, value_pct.
        """
        rows: list[dict] = []
        for obs in data.get("observations", []):
            obs_date = obs.get("d", "")
            entry = obs.get("AVG.INTWO")
            if entry is None:
                continue
            v_str = entry.get("v", "")
            if not v_str:
                continue
            try:
                value = float(v_str)
            except ValueError:
                logger.warning("Invalid CORRA value on %s: %r", obs_date, v_str)
                continue
            rows.append({"date": obs_date, "rate_name": "CORRA", "value_pct": value})

        if not rows:
            return pl.DataFrame(schema=_EMPTY_RATE_SCHEMA)

        return pl.DataFrame(rows).cast({"date": pl.Date, "value_pct": pl.Float64})

    @staticmethod
    def _parse_sofr_json(data: dict) -> pl.DataFrame:
        """Parse NY Fed JSON for the SOFR overnight rate.

        Args:
            data: Parsed JSON from the NY Fed Markets API containing a
                'refRates' key.

        Returns:
            DataFrame with columns: date, rate_name, value_pct.
        """
        rows: list[dict] = []
        for entry in data.get("refRates", []):
            eff_date = entry.get("effectiveDate", "")
            rate = entry.get("percentRate")
            if not eff_date or rate is None:
                continue
            try:
                value = float(rate)
            except (ValueError, TypeError):
                logger.warning("Invalid SOFR value on %s: %r", eff_date, rate)
                continue
            rows.append({"date": eff_date, "rate_name": "SOFR", "value_pct": value})

        if not rows:
            return pl.DataFrame(schema=_EMPTY_RATE_SCHEMA)

        return pl.DataFrame(rows).cast({"date": pl.Date, "value_pct": pl.Float64})

    @staticmethod
    def _parse_estr_csv(csv_text: str) -> pl.DataFrame:
        """Parse ECB SDMX CSV for the eSTR overnight rate.

        Args:
            csv_text: Raw CSV string from the ECB SDMX API.

        Returns:
            DataFrame with columns: date, rate_name, value_pct.
        """
        if not csv_text.strip():
            return pl.DataFrame(schema=_EMPTY_RATE_SCHEMA)

        raw = pl.read_csv(io.StringIO(csv_text), infer_schema_length=0)

        if "TIME_PERIOD" not in raw.columns or "OBS_VALUE" not in raw.columns:
            logger.warning("eSTR CSV missing expected columns: %s", raw.columns)
            return pl.DataFrame(schema=_EMPTY_RATE_SCHEMA)

        rows: list[dict] = []
        for row in raw.iter_rows(named=True):
            time_period = row.get("TIME_PERIOD", "")
            obs_value = row.get("OBS_VALUE", "")
            if not time_period or not obs_value:
                continue
            try:
                value = float(obs_value)
            except ValueError:
                logger.warning("Invalid eSTR value on %s: %r", time_period, obs_value)
                continue
            rows.append({"date": time_period, "rate_name": "ESTR", "value_pct": value})

        if not rows:
            return pl.DataFrame(schema=_EMPTY_RATE_SCHEMA)

        return pl.DataFrame(rows).cast({"date": pl.Date, "value_pct": pl.Float64})

    @staticmethod
    def _parse_sonia_csv(csv_text: str) -> pl.DataFrame:
        """Parse BoE IADB CSV for the SONIA overnight rate.

        Args:
            csv_text: Raw CSV string from the BoE IADB API.

        Returns:
            DataFrame with columns: date, rate_name, value_pct.
        """
        if not csv_text.strip():
            return pl.DataFrame(schema=_EMPTY_RATE_SCHEMA)

        raw = pl.read_csv(io.StringIO(csv_text), infer_schema_length=0)

        if "DATE" not in raw.columns:
            logger.warning("SONIA CSV missing DATE column: %s", raw.columns)
            return pl.DataFrame(schema=_EMPTY_RATE_SCHEMA)

        if "IUDSOIA" not in raw.columns:
            logger.warning("SONIA CSV missing IUDSOIA column: %s", raw.columns)
            return pl.DataFrame(schema=_EMPTY_RATE_SCHEMA)

        rows: list[dict] = []
        for row in raw.iter_rows(named=True):
            date_str = row.get("DATE", "")
            value_str = row.get("IUDSOIA", "")
            if not date_str or not value_str or not str(value_str).strip():
                continue
            try:
                parsed_date = _parse_boe_date(date_str)
            except ValueError:
                logger.warning("Could not parse SONIA date: %r", date_str)
                continue
            try:
                value = float(value_str)
            except ValueError:
                logger.warning("Invalid SONIA value on %s: %r", date_str, value_str)
                continue
            rows.append({"date": parsed_date, "rate_name": "SONIA", "value_pct": value})

        if not rows:
            return pl.DataFrame(schema=_EMPTY_RATE_SCHEMA)

        return pl.DataFrame(rows).cast({"date": pl.Date, "value_pct": pl.Float64})

    # ------------------------------------------------------------------
    # Latest fetch helpers
    # ------------------------------------------------------------------

    async def _fetch_corra_latest(self, client: httpx.AsyncClient) -> pl.DataFrame:
        """Fetch the latest CORRA rate from the BoC Valet API."""
        url = f"{BOC_BASE_URL}/observations/AVG.INTWO/json"
        resp = await client.get(url, params={"recent": "1"})
        resp.raise_for_status()
        self.save_raw(resp.content, "json")
        return self._parse_corra_json(resp.json())

    async def _fetch_sofr_latest(self, client: httpx.AsyncClient) -> pl.DataFrame:
        """Fetch the latest SOFR rate from the NY Fed Markets API."""
        url = f"{_NYFED_BASE_URL}/last/1.json"
        resp = await client.get(url)
        resp.raise_for_status()
        self.save_raw(resp.content, "json")
        return self._parse_sofr_json(resp.json())

    async def _fetch_estr_latest(self, client: httpx.AsyncClient) -> pl.DataFrame:
        """Fetch the latest eSTR rate from the ECB SDMX API."""
        url = f"{ECB_BASE_URL}/EST/B.EU000A2X2A25.WT"
        resp = await client.get(url, params={"format": "csvdata", "lastNObservations": "5"})
        resp.raise_for_status()
        self.save_raw(resp.content, "csv")
        df = self._parse_estr_csv(resp.text)
        if df.shape[0] == 0:
            return df
        latest = df["date"].max()
        return df.filter(pl.col("date") == latest)

    async def _fetch_sonia_latest(self, client: httpx.AsyncClient) -> pl.DataFrame:
        """Fetch the latest SONIA rate from the BoE IADB API."""
        end = date.today()
        start = end - timedelta(days=7)
        params = _build_boe_params(start, end)
        resp = await client.get(_BOE_IADB_URL, params=params)
        resp.raise_for_status()
        self.save_raw(resp.content, "csv")
        df = self._parse_sonia_csv(resp.text)
        if df.shape[0] == 0:
            return df
        latest = df["date"].max()
        return df.filter(pl.col("date") == latest)

    # ------------------------------------------------------------------
    # Backfill helpers
    # ------------------------------------------------------------------

    async def _backfill_corra(
        self,
        client: httpx.AsyncClient,
        start_date: date,
        end_date: date,
    ) -> pl.DataFrame:
        """Backfill CORRA rates, chunked by year."""
        frames: list[pl.DataFrame] = []
        current = start_date
        while current <= end_date:
            chunk_end = min(date(current.year, 12, 31), end_date)
            logger.info("CORRA backfill: %s to %s", current, chunk_end)
            url = f"{BOC_BASE_URL}/observations/AVG.INTWO/json"
            params = {
                "start_date": current.isoformat(),
                "end_date": chunk_end.isoformat(),
            }
            resp = await client.get(url, params=params)
            resp.raise_for_status()
            df = self._parse_corra_json(resp.json())
            if df.shape[0] > 0:
                frames.append(df)
                logger.info("  -> %d CORRA rows", df.shape[0])
            current = date(current.year + 1, 1, 1)

        if not frames:
            return pl.DataFrame(schema=_EMPTY_RATE_SCHEMA)
        return pl.concat(frames)

    async def _backfill_sofr(
        self,
        client: httpx.AsyncClient,
        start_date: date,
        end_date: date,
    ) -> pl.DataFrame:
        """Backfill SOFR rates from the NY Fed search endpoint."""
        url = f"{_NYFED_BASE_URL}/search.json"
        params = {
            "startDate": start_date.strftime("%m/%d/%Y"),
            "endDate": end_date.strftime("%m/%d/%Y"),
        }
        logger.info("SOFR backfill: %s to %s", start_date, end_date)
        resp = await client.get(url, params=params)
        resp.raise_for_status()
        df = self._parse_sofr_json(resp.json())
        logger.info("SOFR backfill: %d rows", df.shape[0])
        return df

    async def _backfill_estr(
        self,
        client: httpx.AsyncClient,
        start_date: date,
        end_date: date,
    ) -> pl.DataFrame:
        """Backfill eSTR rates from the ECB SDMX API."""
        url = f"{ECB_BASE_URL}/EST/B.EU000A2X2A25.WT"
        params = {
            "format": "csvdata",
            "startPeriod": start_date.isoformat(),
            "endPeriod": end_date.isoformat(),
        }
        logger.info("eSTR backfill: %s to %s", start_date, end_date)
        resp = await client.get(url, params=params)
        resp.raise_for_status()
        df = self._parse_estr_csv(resp.text)
        logger.info("eSTR backfill: %d rows", df.shape[0])
        return df

    async def _backfill_sonia(
        self,
        client: httpx.AsyncClient,
        start_date: date,
        end_date: date,
    ) -> pl.DataFrame:
        """Backfill SONIA rates, chunked by year."""
        frames: list[pl.DataFrame] = []
        current = start_date
        while current <= end_date:
            chunk_end = min(date(current.year, 12, 31), end_date)
            logger.info("SONIA backfill: %s to %s", current, chunk_end)
            params = _build_boe_params(current, chunk_end)
            resp = await client.get(_BOE_IADB_URL, params=params)
            resp.raise_for_status()
            df = self._parse_sonia_csv(resp.text)
            if df.shape[0] > 0:
                frames.append(df)
                logger.info("  -> %d SONIA rows", df.shape[0])
            current = date(current.year + 1, 1, 1)

        if not frames:
            return pl.DataFrame(schema=_EMPTY_RATE_SCHEMA)
        return pl.concat(frames)

    # ------------------------------------------------------------------
    # Public interface
    # ------------------------------------------------------------------

    async def fetch_latest(self) -> pl.DataFrame:
        """Fetch the latest overnight rates from all four sources.

        Individual source failures are logged and skipped so that a
        single unavailable API does not prevent the others from returning
        data.

        Returns:
            DataFrame with columns: date, rate_name, value_pct.
        """
        frames: list[pl.DataFrame] = []
        async with self._get_client() as client:
            for label, method in [
                ("CORRA", self._fetch_corra_latest),
                ("SOFR", self._fetch_sofr_latest),
                ("ESTR", self._fetch_estr_latest),
                ("SONIA", self._fetch_sonia_latest),
            ]:
                try:
                    df = await method(client)
                    if df.shape[0] > 0:
                        frames.append(df)
                        logger.info("Fetched %d %s rows", df.shape[0], label)
                except Exception:
                    logger.warning("Failed to fetch %s latest rate", label, exc_info=True)

        if not frames:
            return pl.DataFrame(schema=_EMPTY_RATE_SCHEMA)

        result = pl.concat(frames)
        logger.info("Overnight fetch_latest: %d total rows", result.shape[0])
        return result

    async def backfill(self, start_date: date, end_date: date) -> pl.DataFrame:
        """Backfill overnight rates from all four sources.

        Individual source failures are logged and skipped.

        Args:
            start_date: First date (inclusive).
            end_date: Last date (inclusive).

        Returns:
            DataFrame with columns: date, rate_name, value_pct.
        """
        frames: list[pl.DataFrame] = []
        async with self._get_client() as client:
            for label, method in [
                ("CORRA", self._backfill_corra),
                ("SOFR", self._backfill_sofr),
                ("ESTR", self._backfill_estr),
                ("SONIA", self._backfill_sonia),
            ]:
                try:
                    df = await method(client, start_date, end_date)
                    if df.shape[0] > 0:
                        frames.append(df)
                        logger.info("Backfilled %d %s rows", df.shape[0], label)
                except Exception:
                    logger.warning("Failed to backfill %s", label, exc_info=True)

        if not frames:
            return pl.DataFrame(schema=_EMPTY_RATE_SCHEMA)

        result = pl.concat(frames)
        logger.info("Overnight backfill: %d total rows", result.shape[0])
        return result


# ------------------------------------------------------------------
# Module-level helpers
# ------------------------------------------------------------------


def _format_boe_date(d: date) -> str:
    """Convert a date to BoE's ``DD/Mon/YYYY`` format.

    Args:
        d: The date to format.

    Returns:
        Date string like ``"01/Jan/2025"``.
    """
    return d.strftime("%d/%b/%Y")


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


def _build_boe_params(start_date: date, end_date: date) -> dict[str, str]:
    """Build query parameters for the BoE IADB CSV request for SONIA.

    Args:
        start_date: First date (inclusive).
        end_date: Last date (inclusive).

    Returns:
        Dictionary of query parameters.
    """
    return {
        "csv.x": "yes",
        "SeriesCodes": "IUDSOIA",
        "Datefrom": _format_boe_date(start_date),
        "Dateto": _format_boe_date(end_date),
        "CSVF": "TN",
        "UsingCodes": "Y",
        "VPD": "Y",
        "VFD": "N",
    }
