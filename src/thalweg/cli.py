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
    type=click.Choice(["boc", "ust", "all"]),
    default="all",
    help="Data source to fetch.",
)
def fetch(source: str) -> None:
    """Fetch latest yield curve data."""
    from thalweg import storage
    from thalweg.fetchers import FETCHERS

    async def _run() -> None:
        sources = list(FETCHERS) if source == "all" else [source]
        for src in sources:
            fetcher = FETCHERS[src]()
            click.echo(f"Fetching latest from {src}...")
            df = await fetcher.fetch_latest()
            if df.shape[0] == 0:
                click.echo(f"  No data returned from {src}")
                continue
            storage.append_curves(df)
            click.echo(f"  Stored {df.shape[0]} rows")

    asyncio.run(_run())


@cli.command()
@click.option(
    "--source",
    type=click.Choice(["boc", "ust", "all"]),
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
    from thalweg.fetchers import FETCHERS

    start_date = start.date()
    end_date = end.date()

    async def _run() -> None:
        sources = list(FETCHERS) if source == "all" else [source]
        for src in sources:
            fetcher = FETCHERS[src]()
            click.echo(f"Backfilling {src} from {start_date} to {end_date}...")
            df = await fetcher.backfill(start_date, end_date)
            if df.shape[0] == 0:
                click.echo(f"  No data returned from {src}")
                continue
            storage.append_curves(df)
            click.echo(f"  Stored {df.shape[0]} rows")

    asyncio.run(_run())


@cli.command()
def analyze() -> None:
    """Recompute derived analytics (spreads, slopes, regimes)."""
    click.echo("analyze: not implemented yet")


@cli.command()
@click.option("--port", default=8001, help="Port to serve on.")
def serve(port: int) -> None:
    """Start the Thalweg web server."""
    click.echo(f"serve --port {port}: not implemented yet")


@cli.command()
def status() -> None:
    """Show data status (latest dates, row counts)."""
    import polars as pl

    from thalweg.config import CURVES_DIR

    parquet_files = sorted(CURVES_DIR.glob("*.parquet"))
    if not parquet_files:
        click.echo("No data files found.")
        return

    click.echo(f"{'File':<25} {'Latest Date':<15} {'Rows':>10}")
    click.echo("-" * 52)
    for path in parquet_files:
        df = pl.read_parquet(path)
        latest = df["date"].max()
        rows = df.shape[0]
        click.echo(f"{path.name:<25} {str(latest):<15} {rows:>10,}")
