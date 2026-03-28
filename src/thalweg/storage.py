"""Parquet storage layer for yield curve data.

Handles reading, writing, appending, and querying curve data stored
as parquet files in the configured data directory.
"""

from __future__ import annotations

import logging
from datetime import date
from pathlib import Path

import polars as pl

from thalweg.config import CURVES_DIR, RATES_DIR

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
    missing = set(EXPECTED_SCHEMA) - set(df.columns)
    if missing:
        raise ValueError(f"Missing required columns: {sorted(missing)}")

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

    result = pl.scan_parquet(path).select(pl.col("date").max()).collect()
    return result["date"][0]


# ---------------------------------------------------------------------------
# Overnight rate storage
# ---------------------------------------------------------------------------

RATE_SCHEMA = {
    "date": pl.Date,
    "rate_name": pl.Utf8,
    "value_pct": pl.Float64,
}

RATE_DEDUP_KEYS = ["date", "rate_name"]


def _rate_parquet_path() -> Path:
    """Return the parquet file path for overnight rates."""
    return RATES_DIR / "overnight.parquet"


def append_rates(df: pl.DataFrame) -> None:
    """Append overnight rate data to the rates parquet file.

    Deduplicates on (date, rate_name), keeping the most recently
    appended values.

    Args:
        df: DataFrame with columns matching RATE_SCHEMA.
    """
    missing = set(RATE_SCHEMA) - set(df.columns)
    if missing:
        raise ValueError(f"Missing required columns: {sorted(missing)}")

    df = df.cast({k: v for k, v in RATE_SCHEMA.items() if k in df.columns})

    path = _rate_parquet_path()

    if path.exists():
        existing = pl.read_parquet(path)
        combined = pl.concat([existing, df])
    else:
        combined = df

    deduped = combined.unique(subset=RATE_DEDUP_KEYS, keep="last", maintain_order=True)
    deduped = deduped.sort(["date", "rate_name"])

    deduped.write_parquet(path)
    logger.info(
        "Wrote %d rows to %s (was %d)",
        deduped.shape[0],
        path.name,
        combined.shape[0],
    )


def read_rates(
    rate_name: str | None = None,
    start_date: date | None = None,
    end_date: date | None = None,
) -> pl.DataFrame:
    """Read overnight rate data from parquet with optional filters.

    Args:
        rate_name: Filter by rate name (e.g. 'CORRA', 'SOFR').
        start_date: Include only dates on or after this date.
        end_date: Include only dates on or before this date.

    Returns:
        Filtered DataFrame, or empty DataFrame if no data found.
    """
    path = _rate_parquet_path()
    if not path.exists():
        return pl.DataFrame(schema=RATE_SCHEMA)

    lf = pl.scan_parquet(path)

    if rate_name:
        lf = lf.filter(pl.col("rate_name") == rate_name)
    if start_date:
        lf = lf.filter(pl.col("date") >= start_date)
    if end_date:
        lf = lf.filter(pl.col("date") <= end_date)

    return lf.collect()


def get_latest_rate_date(rate_name: str) -> date | None:
    """Get the most recent date for a given overnight rate.

    Args:
        rate_name: The rate name (e.g. 'CORRA', 'SOFR').

    Returns:
        The latest date, or None if the file doesn't exist.
    """
    path = _rate_parquet_path()
    if not path.exists():
        return None

    result = (
        pl.scan_parquet(path)
        .filter(pl.col("rate_name") == rate_name)
        .select(pl.col("date").max())
        .collect()
    )
    val = result["date"][0]
    return val
