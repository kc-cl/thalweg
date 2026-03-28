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
    """Return the latest overnight rate for each rate name with daily change."""
    from thalweg import storage

    df = storage.read_rates()
    if df.is_empty():
        return {"rates": []}
    result = _latest_per_group(df, "rate_name")
    records = _df_to_records(result)

    # Enrich with prior-day value and change in basis points
    for rec in records:
        rate_name = rec["rate_name"]
        latest_date = df.filter(pl.col("rate_name") == rate_name)["date"].max()
        prior = df.filter(
            (pl.col("rate_name") == rate_name) & (pl.col("date") < latest_date)
        )
        if prior.is_empty():
            rec["change_bp"] = None
        else:
            prev_date = prior["date"].max()
            prev_row = prior.filter(pl.col("date") == prev_date)
            prev_val = prev_row["value_pct"][0]
            rec["change_bp"] = round((rec["value_pct"] - prev_val) * 100, 2)

    return {"rates": records}


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

    horizons = {"1d": 1, "1w": 7, "1m": 30, "1y": 365}

    # Compute changes per currency using each currency's own latest date
    latest_dates = _latest_per_group(curves, "currency")
    changes: list[pl.DataFrame] = []

    for (ccy,), ccy_curves in curves.group_by(["currency"]):
        ccy_latest = latest_dates.filter(pl.col("currency") == ccy)
        today = ccy_latest["date"].max()
        today_curves = ccy_curves.filter(pl.col("date") == today)

        for label, days in horizons.items():
            target_date = today - timedelta(days=days)
            past = ccy_curves.filter(pl.col("date") <= target_date)
            if past.is_empty():
                continue
            closest = past["date"].max()
            past_curves = ccy_curves.filter(pl.col("date") == closest)

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
            changes.append(
                merged.select(["currency", "tenor_years", "horizon", "change_pct"])
            )

    if not changes:
        return {"changes": []}
    return {"changes": _df_to_records(pl.concat(changes))}


# ---------------------------------------------------------------------------
# History & regime endpoints (Phase 2)
# ---------------------------------------------------------------------------


@router.get("/curves/dates")
async def curves_dates(
    currency: str | None = Query(None),
) -> dict:
    """Return sorted list of available curve dates.

    Used by the Curve Explorer date slider to determine range.
    """
    from thalweg import storage

    dates = storage.get_available_dates(currency=currency)
    return {"dates": [d.isoformat() for d in dates]}


@router.get("/regimes/latest")
async def regimes_latest() -> dict:
    """Return the most recent regime classification per currency."""
    from thalweg import storage

    df = storage.read_regimes()
    if df.is_empty():
        return {"regimes": []}
    result = _latest_per_group(df, "currency")
    return {"regimes": _df_to_records(result)}


@router.get("/regimes")
async def regimes(
    currency: str | None = Query(None),
    start_date: date | None = Query(None),
    end_date: date | None = Query(None),
) -> dict:
    """Query regime classification history with optional filters."""
    from thalweg import storage

    df = storage.read_regimes(
        currency=currency, start_date=start_date, end_date=end_date
    )
    return {"regimes": _df_to_records(df)}


@router.get("/analytics/slopes/history")
async def slopes_history(
    currency: str | None = Query(None),
    slope_name: str | None = Query(None),
    start_date: date | None = Query(None),
    end_date: date | None = Query(None),
) -> dict:
    """Return slope time series from derived storage."""
    from thalweg import storage

    kwargs: dict[str, str | float] = {}
    if currency:
        kwargs["currency"] = currency
    if slope_name:
        kwargs["slope_name"] = slope_name

    df = storage.read_derived(
        "slopes", start_date=start_date, end_date=end_date, **kwargs
    )
    return {"slopes": _df_to_records(df)}


@router.get("/analytics/spreads/history")
async def spreads_history(
    pair: str | None = Query(None),
    tenor_years: float | None = Query(None),
    start_date: date | None = Query(None),
    end_date: date | None = Query(None),
) -> dict:
    """Return cross-market spread time series from derived storage."""
    from thalweg import storage

    kwargs: dict[str, str | float] = {}
    if pair:
        kwargs["pair"] = pair
    if tenor_years is not None:
        kwargs["tenor_years"] = tenor_years

    df = storage.read_derived(
        "spreads", start_date=start_date, end_date=end_date, **kwargs
    )
    return {"spreads": _df_to_records(df)}


# ---------------------------------------------------------------------------
# PCA, fan chart & analog forecast endpoints (Phase 3)
# ---------------------------------------------------------------------------


@router.get("/analytics/pca/scores")
async def pca_scores(
    currency: str | None = Query(None),
) -> dict:
    """Return PCA scores time series from derived storage.

    Args:
        currency: Filter by currency code (e.g. ``CAD``, ``USD``).
    """
    from thalweg import storage

    kwargs: dict[str, str | float] = {}
    if currency:
        kwargs["currency"] = currency

    df = storage.read_derived("pca_scores", **kwargs)
    return {"scores": _df_to_records(df)}


@router.get("/analytics/pca/loadings")
async def pca_loadings(
    currency: str | None = Query(None),
) -> dict:
    """Return PCA loadings per tenor and explained variance from derived storage.

    Args:
        currency: Filter by currency code (e.g. ``CAD``, ``USD``).
    """
    from thalweg import storage

    kwargs: dict[str, str | float] = {}
    if currency:
        kwargs["currency"] = currency

    df = storage.read_derived("pca_loadings", **kwargs)
    if df.is_empty():
        return {"loadings": [], "explained_variance": []}

    # Extract explained variance as a separate list of unique entries
    ev = (
        df.select(["currency", "component", "explained_variance_ratio"])
        .unique()
        .sort("component")
    )
    return {
        "loadings": _df_to_records(df.drop("explained_variance_ratio")),
        "explained_variance": _df_to_records(ev),
    }


@router.get("/analytics/fan")
async def analytics_fan(
    currency: str = Query(...),
    horizon: int = Query(21),
) -> dict:
    """Compute fan chart quantile bands for a currency.

    Fits PCA on the currency's curve history and projects empirical
    shock distributions at the requested horizon.

    Args:
        currency: Currency code (required).
        horizon: Number of business days for the shock horizon.
    """
    from thalweg import storage
    from thalweg.analytics.distributions import compute_shock_distribution
    from thalweg.analytics.pca import fit_pca

    curves = storage.read_curves(currency=currency)
    if curves.is_empty():
        return {"fan": [], "current": []}

    pca_result = fit_pca(curves, currency)
    if pca_result is None:
        return {"fan": [], "current": []}

    fan = compute_shock_distribution(pca_result, horizon_days=horizon)

    # Current curve (latest date)
    latest_date = curves["date"].max()
    current = curves.filter(pl.col("date") == latest_date)

    return {
        "fan": _df_to_records(fan),
        "current": _df_to_records(current),
    }


@router.get("/analytics/analogs")
async def analytics_analogs(
    currency: str = Query(...),
    k: int = Query(20),
    horizon: int = Query(63),
) -> dict:
    """Find analog dates and compute forecast paths.

    Fits PCA on the currency's curve history, identifies the k nearest
    historical analogs in PCA space, and tracks what happened to the
    curve over the forecast horizon.

    Args:
        currency: Currency code (required).
        k: Number of nearest analog dates to return.
        horizon: Number of calendar days for the forecast horizon.
    """
    from thalweg import storage
    from thalweg.analytics.forecasts import find_analogs, forecast_from_analogs
    from thalweg.analytics.pca import fit_pca

    curves = storage.read_curves(currency=currency)
    if curves.is_empty():
        return {"analogs": [], "forecasts": []}

    pca_result = fit_pca(curves, currency)
    if pca_result is None:
        return {"analogs": [], "forecasts": []}

    analogs = find_analogs(pca_result, k=k)
    forecasts = forecast_from_analogs(curves, pca_result, k=k, horizon_days=horizon)

    return {
        "analogs": _df_to_records(analogs),
        "forecasts": _df_to_records(forecasts),
    }
