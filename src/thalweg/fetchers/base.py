"""Abstract base class for all data fetchers."""

from __future__ import annotations

import abc
import logging
from datetime import date
from pathlib import Path
from typing import TYPE_CHECKING

import httpx

from thalweg.config import RAW_DIR

if TYPE_CHECKING:
    import polars as pl

logger = logging.getLogger(__name__)


class BaseFetcher(abc.ABC):
    """Base class for yield curve data fetchers.

    Each fetcher pulls data from a single source, normalizes it to the common
    schema (date, currency, curve_type, tenor_years, yield_pct), and returns
    a Polars DataFrame.
    """

    @property
    @abc.abstractmethod
    def name(self) -> str:
        """Short identifier for this data source (e.g. 'boc', 'ust')."""

    @abc.abstractmethod
    async def fetch_latest(self) -> pl.DataFrame:
        """Fetch the most recent available data from this source."""

    @abc.abstractmethod
    async def backfill(self, start_date: date, end_date: date) -> pl.DataFrame:
        """Fetch historical data for the given date range."""

    def save_raw(self, data: bytes, suffix: str) -> Path:
        """Save a raw API response to the raw data directory.

        Args:
            data: Raw response bytes.
            suffix: File extension (e.g. 'json', 'xml', 'csv').

        Returns:
            Path to the saved file.
        """
        today = date.today().isoformat()
        path = RAW_DIR / f"{self.name}_{today}.{suffix}"
        path.write_bytes(data)
        logger.info("Saved raw response to %s (%d bytes)", path, len(data))
        return path

    def _get_client(self) -> httpx.AsyncClient:
        """Create an httpx async client with retry-friendly settings."""
        transport = httpx.AsyncHTTPTransport(retries=3)
        return httpx.AsyncClient(
            transport=transport,
            timeout=httpx.Timeout(30.0),
            follow_redirects=True,
        )
