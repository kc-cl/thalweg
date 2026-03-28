"""CLI entry point for Thalweg."""

import click

from thalweg import __version__


@click.group()
@click.version_option(version=__version__, prog_name="thalweg")
def cli() -> None:
    """Thalweg — yield curve observatory."""


@cli.command()
@click.option(
    "--source",
    type=click.Choice(["boc", "ust", "ecb", "boe", "fed_h15", "all"]),
    default="all",
    help="Data source to fetch.",
)
def fetch(source: str) -> None:
    """Fetch latest yield curve data."""
    click.echo(f"fetch --source {source}: not implemented yet")


@cli.command()
@click.option(
    "--source",
    type=click.Choice(["boc", "ust", "ecb", "boe", "fed_h15", "all"]),
    required=True,
    help="Data source to backfill.",
)
@click.option("--start", required=True, type=click.DateTime(formats=["%Y-%m-%d"]),
              help="Start date (YYYY-MM-DD).")
@click.option("--end", required=True, type=click.DateTime(formats=["%Y-%m-%d"]),
              help="End date (YYYY-MM-DD).")
def backfill(source: str, start: click.DateTime, end: click.DateTime) -> None:
    """Backfill historical yield curve data."""
    click.echo(f"backfill --source {source} --start {start} --end {end}: not implemented yet")


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
    click.echo("status: not implemented yet")
