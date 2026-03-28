"""Bank of Canada Valet API fetcher.

Fetches benchmark Government of Canada bond yields from the Valet API.
"""

from __future__ import annotations

import logging
from datetime import date

import polars as pl

from thalweg.config import BOC_BASE_URL
from thalweg.fetchers.base import BaseFetcher

logger = logging.getLogger(__name__)

# Benchmark yield series -> tenor in years
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
                    logger.warning(
                        "Invalid yield value for %s on %s: %r", series, obs_date, v_str
                    )
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
                logger.info("  -> %d rows", df.shape[0])

            current = date(current.year + 1, 1, 1)

        if not all_frames:
            return self._parse_observations({"observations": []})

        return pl.concat(all_frames)
