# CLAUDE.md — Thalweg

## Project Overview

Thalweg is a yield curve observatory: a data pipeline, analytics engine, and visualization platform for sovereign yield curves across four major markets (CAD, USD, EUR, GBP). It combines a practical ALM dashboard with beautiful topographic visualizations of curve dynamics.

The name comes from the deepest continuous line along a valley floor — the path water follows through a landscape.

## Architecture

See `spec.md` for the full architecture document including data sources, analytics design, visualization concepts, tech stack, and phased build plan.

## Tech Stack

- **Language:** Python 3.12+
- **Package manager:** uv (not pip, not poetry)
- **Data layer:** Polars for dataframes, PyArrow for parquet I/O
- **HTTP:** httpx (async)
- **Analytics:** scikit-learn (PCA), scipy (distributions, interpolation), numpy
- **Web server:** FastAPI + uvicorn
- **Frontend (MVP):** HTMX + D3.js, served from FastAPI static files
- **Frontend (later phases):** React + D3 + Three.js for interactive/3D views
- **Scheduling:** cron (external) or APScheduler (in-process)
- **Testing:** pytest
- **Linting:** ruff

## Project Structure

```
thalweg/
├── pyproject.toml
├── CLAUDE.md
├── spec.md
├── README.md
├── src/
│   └── thalweg/
│       ├── __init__.py
│       ├── config.py
│       ├── fetchers/
│       │   ├── __init__.py
│       │   ├── base.py
│       │   └── ... (one module per data source)
│       ├── storage.py
│       ├── analytics/
│       │   ├── __init__.py
│       │   └── ...
│       ├── web/
│       │   ├── __init__.py
│       │   ├── app.py
│       │   ├── api.py
│       │   └── static/
│       └── cli.py
├── frontend/               # Only when we move to React (Phase 3+)
├── tests/
│   ├── conftest.py
│   └── ...
└── data/                   # .gitignored, created at runtime
    ├── raw/
    ├── curves/
    ├── rates/
    └── derived/
```

## Conventions

### Python

- Use `src/thalweg/` layout (src-layout, PEP 621 pyproject.toml).
- All imports should be from `thalweg.*` (e.g., `from thalweg.fetchers.boc import BoCFetcher`).
- Type hints on all public function signatures.
- Docstrings on all public classes and functions (Google style).
- Use Polars, not pandas, for all dataframe operations. Use pandas only at boundaries where a library requires it (e.g., sklearn).
- Use `httpx.AsyncClient` for HTTP requests. All fetchers should be async.
- Use `pathlib.Path` for all file paths, never string concatenation.
- Configuration via a single `config.py` that reads from environment variables with sensible defaults. No YAML/TOML config files beyond pyproject.toml.
- Logging via stdlib `logging`, structured with module-level loggers.
- No print statements except in CLI output.

### Data

- All curve data stored as parquet files with the schema defined in spec.md.
- Parquet files live in `data/` (configurable via `THALWEG_DATA_DIR` env var).
- Raw API responses saved to `data/raw/` with ISO date in filename. Retained 7 days.
- Never store interpolated data as if it were observed data. Interpolation happens at query time.
- Dates are always timezone-naive dates (not datetimes). Yield curves are end-of-day snapshots.

### Fetchers

- Each fetcher inherits from `BaseFetcher` (in `fetchers/base.py`).
- `BaseFetcher` provides: retry logic, logging, raw response caching, and a standard interface.
- Each fetcher must implement:
  - `fetch_latest() -> pl.DataFrame` — fetch today's data
  - `backfill(start_date, end_date) -> pl.DataFrame` — fetch historical range
- Fetchers normalize all data to the common schema: `date`, `currency`, `curve_type`, `tenor_years`, `yield_pct`.
- Fetchers should be independently testable with saved fixtures from `data/raw/`.

### Testing

- Tests in `tests/`, mirroring `src/thalweg/` structure.
- Use pytest fixtures for sample data.
- Fetcher tests should work offline using saved raw responses (fixture files).
- Analytics tests should use known synthetic curves where the correct answer can be computed by hand.

### Git

- Conventional commits: `feat:`, `fix:`, `refactor:`, `docs:`, `test:`, `chore:`.
- Branch from `main`. Feature branches named `feat/description` or `fix/description`.
- `.gitignore` must exclude: `data/`, `*.parquet`, `.env`, `__pycache__/`, `.ruff_cache/`, `node_modules/`, `dist/`.

### CLI

- CLI entry point via `python -m thalweg.cli` or a `thalweg` console script.
- Commands: `fetch`, `backfill`, `analyze`, `serve`, `status`.
- Use `click` for CLI framework.

## Data Sources Quick Reference

|Source         |Module               |API Base URL                                                                          |Auth                       |
|---------------|---------------------|--------------------------------------------------------------------------------------|---------------------------|
|Bank of Canada |`fetchers/boc.py`    |`https://www.bankofcanada.ca/valet/`                                                  |None                       |
|US Treasury    |`fetchers/ust.py`    |`https://home.treasury.gov/resource-center/data-chart-center/interest-rates/pages/xml`|None                       |
|ECB            |`fetchers/ecb.py`    |`https://data-api.ecb.europa.eu/service/data/`                                        |None                       |
|Bank of England|`fetchers/boe.py`    |`https://www.bankofengland.co.uk/statistics/yield-curves`                             |None (spreadsheet download)|
|Fed H.15       |`fetchers/fed_h15.py`|`https://www.federalreserve.gov/datadownload/`                                        |None                       |

## Deployment

- Runs on a Hetzner VPS (CPX22) alongside other projects.
- Accessible via Tailscale for private use, optionally via Caddy reverse proxy for public access.
- Data fetch runs daily via cron at 18:00 ET (after all sources publish).
- Office display is a Raspberry Pi running Chromium in kiosk mode pointed at the dashboard endpoint.

## Current Phase

Phase 0: Data Foundation. Goal is to scaffold the project, implement BoC and UST fetchers, build parquet storage, backfill 10+ years of GoC and UST curves, and have a working CLI.

## Attribution Requirements

All public-facing pages must include attribution:

- US Treasury data: "Source: U.S. Department of the Treasury" (CC0 public domain)
- ECB data: "Source: European Central Bank" (free reuse with citation)
- Bank of Canada data: "Source: Bank of Canada" (attribute, don't imply endorsement)
- Bank of England data: "Source: Bank of England" (non-commercial, attribute)
- Fed H.15 data: "Source: Board of Governors of the Federal Reserve System" (public domain)
