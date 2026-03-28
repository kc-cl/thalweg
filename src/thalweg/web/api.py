"""JSON API endpoints for Thalweg."""

from __future__ import annotations

from datetime import date, timedelta

import polars as pl
from fastapi import APIRouter, Query

router = APIRouter()


def _df_to_records(df: pl.DataFrame) -> list[dict]:
    """Convert a Polars DataFrame to a list of JSON-serializable dicts.

    Converts ``date`` values to ISO-format strings so FastAPI can serialize
    them without custom encoders.

    Args:
        df: Any Polars DataFrame.

    Returns:
        List of row dicts with all values JSON-serializable.
    """
    records = df.to_dicts()
    for row in records:
        for k, v in row.items():
            if isinstance(v, date):
                row[k] = v.isoformat()
    return records


def _latest_per_group(df: pl.DataFrame, group_col: str) -> pl.DataFrame:
    """Filter a DataFrame to only the latest date per group.

    Args:
        df: DataFrame with ``date`` and *group_col* columns.
        group_col: Column to group by when finding the latest date.

    Returns:
        Rows whose date matches the max date for their group.
    """
    latest = df.group_by(group_col).agg(pl.col("date").max().alias("_max_date"))
    return (
        df.join(latest, on=group_col)
        .filter(pl.col("date") == pl.col("_max_date"))
        .drop("_max_date")
    )


@router.get("/curves/latest")
async def curves_latest() -> dict:
    """Return the most recent curve data per currency.

    Each currency may have a different latest date, so we return the latest
    available date for each independently.
    """
    from thalweg import storage

    all_curves = storage.read_curves()
    if all_curves.is_empty():
        return {"curves": []}
    result = _latest_per_group(all_curves, "currency")
    return {"curves": _df_to_records(result)}


@router.get("/curves")
async def curves(
    currency: str | None = Query(None),
    curve_type: str | None = Query(None),
    start_date: date | None = Query(None),
    end_date: date | None = Query(None),
) -> dict:
    """Query curve data with optional filters.

    Args:
        currency: Filter by currency code (e.g. ``CAD``, ``USD``).
        curve_type: Filter by curve type (e.g. ``govt_par``).
        start_date: Earliest date to include.
        end_date: Latest date to include.
    """
    from thalweg import storage

    df = storage.read_curves(
        currency=currency,
        curve_type=curve_type,
        start_date=start_date,
        end_date=end_date,
    )
    return {"curves": _df_to_records(df)}


@router.get("/rates/overnight")
async def rates_overnight() -> dict:
    """Return the latest overnight rate for each rate name."""
    from thalweg import storage

    df = storage.read_rates()
    if df.is_empty():
        return {"rates": []}
    result = _latest_per_group(df, "rate_name")
    return {"rates": _df_to_records(result)}


@router.get("/analytics/slopes")
async def analytics_slopes() -> dict:
    """Compute yield curve slopes for the latest date per currency."""
    from thalweg import storage
    from thalweg.analytics import compute_slopes

    curves = storage.read_curves()
    if curves.is_empty():
        return {"slopes": []}
    latest_curves = _latest_per_group(curves, "currency")
    slopes = compute_slopes(latest_curves)
    return {"slopes": _df_to_records(slopes)}


@router.get("/analytics/curvature")
async def analytics_curvature() -> dict:
    """Compute butterfly spreads for the latest date per currency."""
    from thalweg import storage
    from thalweg.analytics import compute_curvature

    curves = storage.read_curves()
    if curves.is_empty():
        return {"curvature": []}
    latest_curves = _latest_per_group(curves, "currency")
    curvature = compute_curvature(latest_curves)
    return {"curvature": _df_to_records(curvature)}


@router.get("/analytics/spreads")
async def analytics_spreads() -> dict:
    """Compute cross-market yield spreads for the latest date per currency."""
    from thalweg import storage
    from thalweg.analytics import compute_cross_market_spreads

    curves = storage.read_curves()
    if curves.is_empty():
        return {"spreads": []}
    latest_curves = _latest_per_group(curves, "currency")
    spreads = compute_cross_market_spreads(latest_curves)
    return {"spreads": _df_to_records(spreads)}


@router.get("/curves/changes")
async def curves_changes() -> dict:
    """Compute yield changes vs previous periods (1d, 1w, 1m, 1y).

    For each horizon, the endpoint finds the closest available date on or
    before the target date and computes the change in yield.
    """
    from thalweg import storage

    curves = storage.read_curves()
    if curves.is_empty():
        return {"changes": []}

    today = curves["date"].max()
    horizons = {"1d": 1, "1w": 7, "1m": 30, "1y": 365}

    today_curves = curves.filter(pl.col("date") == today)
    changes: list[pl.DataFrame] = []

    for label, days in horizons.items():
        target_date = today - timedelta(days=days)
        past = curves.filter(pl.col("date") <= target_date)
        if past.is_empty():
            continue
        closest = past["date"].max()
        past_curves = curves.filter(pl.col("date") == closest)

        merged = today_curves.join(
            past_curves.select([
                "currency",
                "curve_type",
                "tenor_years",
                pl.col("yield_pct").alias("prev_yield"),
            ]),
            on=["currency", "curve_type", "tenor_years"],
        )
        merged = merged.with_columns(
            (pl.col("yield_pct") - pl.col("prev_yield")).alias("change_pct"),
            pl.lit(label).alias("horizon"),
        )
        changes.append(merged.select(["currency", "tenor_years", "horizon", "change_pct"]))

    if not changes:
        return {"changes": []}
    return {"changes": _df_to_records(pl.concat(changes))}
