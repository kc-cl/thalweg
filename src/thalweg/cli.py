"""CLI entry point for Thalweg."""

from __future__ import annotations

import asyncio
import logging

import click

from thalweg import __version__

logger = logging.getLogger(__name__)


def _setup_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)-8s %(name)s -- %(message)s",
        datefmt="%H:%M:%S",
    )


@click.group()
@click.version_option(version=__version__, prog_name="thalweg")
def cli() -> None:
    """Thalweg -- yield curve observatory."""
    _setup_logging()


@cli.command()
@click.option(
    "--source",
    type=click.Choice(["boc", "ust", "ecb", "boe", "overnight", "all"]),
    default="all",
    help="Data source to fetch.",
)
def fetch(source: str) -> None:
    """Fetch latest yield curve data."""
    from thalweg import storage
    from thalweg.fetchers import FETCHERS, RATE_FETCHERS

    async def _run() -> None:
        if source == "all":
            curve_sources = list(FETCHERS)
            rate_sources = list(RATE_FETCHERS)
        elif source in RATE_FETCHERS:
            curve_sources: list[str] = []
            rate_sources = [source]
        else:
            curve_sources = [source]
            rate_sources: list[str] = []

        for src in curve_sources:
            fetcher = FETCHERS[src]()
            click.echo(f"Fetching latest from {src}...")
            df = await fetcher.fetch_latest()
            if df.shape[0] == 0:
                click.echo(f"  No data returned from {src}")
                continue
            storage.append_curves(df)
            click.echo(f"  Stored {df.shape[0]} rows")

        for src in rate_sources:
            fetcher = RATE_FETCHERS[src]()
            click.echo(f"Fetching latest from {src}...")
            df = await fetcher.fetch_latest()
            if df.shape[0] == 0:
                click.echo(f"  No data returned from {src}")
                continue
            storage.append_rates(df)
            click.echo(f"  Stored {df.shape[0]} rows")

    asyncio.run(_run())


@cli.command()
@click.option(
    "--source",
    type=click.Choice(["boc", "ust", "ecb", "boe", "overnight", "all"]),
    required=True,
    help="Data source to backfill.",
)
@click.option(
    "--start",
    required=True,
    type=click.DateTime(formats=["%Y-%m-%d"]),
    help="Start date (YYYY-MM-DD).",
)
@click.option(
    "--end",
    required=True,
    type=click.DateTime(formats=["%Y-%m-%d"]),
    help="End date (YYYY-MM-DD).",
)
def backfill(source: str, start: click.DateTime, end: click.DateTime) -> None:
    """Backfill historical yield curve data."""
    from thalweg import storage
    from thalweg.fetchers import FETCHERS, RATE_FETCHERS

    start_date = start.date()
    end_date = end.date()

    async def _run() -> None:
        if source == "all":
            curve_sources = list(FETCHERS)
            rate_sources = list(RATE_FETCHERS)
        elif source in RATE_FETCHERS:
            curve_sources: list[str] = []
            rate_sources = [source]
        else:
            curve_sources = [source]
            rate_sources: list[str] = []

        for src in curve_sources:
            fetcher = FETCHERS[src]()
            click.echo(f"Backfilling {src} from {start_date} to {end_date}...")
            df = await fetcher.backfill(start_date, end_date)
            if df.shape[0] == 0:
                click.echo(f"  No data returned from {src}")
                continue
            storage.append_curves(df)
            click.echo(f"  Stored {df.shape[0]} rows")

        for src in rate_sources:
            fetcher = RATE_FETCHERS[src]()
            click.echo(f"Backfilling {src} from {start_date} to {end_date}...")
            df = await fetcher.backfill(start_date, end_date)
            if df.shape[0] == 0:
                click.echo(f"  No data returned from {src}")
                continue
            storage.append_rates(df)
            click.echo(f"  Stored {df.shape[0]} rows")

    asyncio.run(_run())


@cli.command()
def analyze() -> None:
    """Recompute derived analytics (spreads, slopes, curvature, PCA, fan charts)."""
    import polars as pl

    from thalweg import storage
    from thalweg.analytics import (
        classify_regimes,
        compute_cross_market_spreads,
        compute_curvature,
        compute_fan_chart,
        compute_slopes,
        fit_all_pca,
    )
    from thalweg.config import DERIVED_DIR

    curves = storage.read_curves()
    if curves.is_empty():
        click.echo("No curve data found. Run 'thalweg fetch' first.")
        return

    click.echo("Computing analytics...")

    slopes = compute_slopes(curves)
    if not slopes.is_empty():
        slopes.write_parquet(DERIVED_DIR / "slopes.parquet")
        click.echo(f"  Wrote {slopes.shape[0]} slope records")

    curvature = compute_curvature(curves)
    if not curvature.is_empty():
        curvature.write_parquet(DERIVED_DIR / "curvature.parquet")
        click.echo(f"  Wrote {curvature.shape[0]} curvature records")

    spreads = compute_cross_market_spreads(curves)
    if not spreads.is_empty():
        spreads.write_parquet(DERIVED_DIR / "spreads.parquet")
        click.echo(f"  Wrote {spreads.shape[0]} spread records")

    regimes = classify_regimes(curves)
    if not regimes.is_empty():
        storage.append_regimes(regimes)
        click.echo(f"  Wrote {regimes.shape[0]} regime records")

    # --- PCA + fan charts ---
    pca_results = fit_all_pca(curves)
    if pca_results:
        # Scores
        all_scores = pl.concat([r.scores_df for r in pca_results.values()])
        all_scores.write_parquet(DERIVED_DIR / "pca_scores.parquet")
        click.echo(f"  Wrote {all_scores.shape[0]} PCA score records")

        # Loadings
        loadings_rows = []
        for r in pca_results.values():
            for i in range(len(r.explained_variance)):
                for j, tenor in enumerate(r.tenors):
                    loadings_rows.append({
                        "currency": r.currency,
                        "curve_type": r.curve_type,
                        "component": f"pc{i + 1}",
                        "tenor_years": tenor,
                        "loading": float(r.components[i][j]),
                        "explained_variance_ratio": float(r.explained_variance[i]),
                    })
        loadings_df = pl.DataFrame(loadings_rows)
        loadings_df.write_parquet(DERIVED_DIR / "pca_loadings.parquet")
        click.echo(f"  Wrote {loadings_df.shape[0]} PCA loading records")

        # Fan charts
        fan_parts = []
        for r in pca_results.values():
            fan = compute_fan_chart(curves, r)
            if not fan.is_empty():
                fan_parts.append(fan)
        if fan_parts:
            all_fans = pl.concat(fan_parts)
            all_fans.write_parquet(DERIVED_DIR / "fan_charts.parquet")
            click.echo(f"  Wrote {all_fans.shape[0]} fan chart records")
    else:
        click.echo("  Skipping PCA (insufficient data)")

    click.echo("Analytics complete.")


@cli.command()
@click.option("--port", default=8001, help="Port to serve on.")
def serve(port: int) -> None:
    """Start the Thalweg web server."""
    import uvicorn

    from thalweg.web import create_app

    app = create_app()
    click.echo(f"Starting Thalweg server on port {port}...")
    uvicorn.run(app, host="0.0.0.0", port=port, log_level="info")


@cli.command()
def status() -> None:
    """Show data status (latest dates, row counts)."""
    import polars as pl

    from thalweg.config import CURVES_DIR, DERIVED_DIR, RATES_DIR

    all_dirs = [
        ("Curves", CURVES_DIR),
        ("Rates", RATES_DIR),
        ("Derived", DERIVED_DIR),
    ]

    found = False
    for label, data_dir in all_dirs:
        parquet_files = sorted(data_dir.glob("*.parquet"))
        if not parquet_files:
            continue
        if not found:
            click.echo(f"{'File':<30} {'Latest Date':<15} {'Rows':>10}")
            click.echo("-" * 57)
            found = True
        for path in parquet_files:
            df = pl.read_parquet(path)
            latest = df["date"].max()
            rows = df.shape[0]
            click.echo(f"{path.name:<30} {str(latest):<15} {rows:>10,}")

    if not found:
        click.echo("No data files found.")
