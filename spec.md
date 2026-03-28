# Thalweg — Specification

## Vision

Thalweg is a yield curve observatory — part practical ALM dashboard, part visual exploration of the geometry and dynamics of interest rate term structures across four major sovereign markets (CAD, USD, EUR, GBP).

A thalweg is the deepest continuous line along a valley floor — the path water follows through a landscape. In geopolitics, the thalweg of a river defines the border between nations. The yield curve is both: the deepest channel through which capital flows, and the boundary between monetary regimes.

It answers three questions at a glance:

1. **What does the world look like right now?** (dashboard)
1. **How did we get here?** (history & regime context)
1. **Where might we be going?** (probabilistic projections & stress scenarios)

-----

## Data Architecture

### Sources (Tier 1 — Free APIs, Day One)

|Source                       |Data                                                                                                           |API                                           |License                                   |Update Freq       |
|-----------------------------|---------------------------------------------------------------------------------------------------------------|----------------------------------------------|------------------------------------------|------------------|
|**Bank of Canada — Valet**   |GoC zero-coupon curve (0.25–30yr), benchmark bond yields (2/3/5/7/10/30), T-bill yields, CORRA, BoC policy rate|REST, JSON/CSV, no auth                       |Non-commercial with attribution           |Daily, ~10:00 ET  |
|**US Treasury — XML Feed**   |CMT par yield curve (1mo–30yr), real yield curve (TIPS)                                                        |REST, XML                                     |**CC0 Public Domain**                     |Daily, ~6:00 PM ET|
|**Fed H.15 Release (direct)**|USD swap rates (ICE, 1–30yr), Fed Funds rate, SOFR                                                             |Web scrape or FRED (public domain series only)|Public Domain (citation requested)        |Daily             |
|**ECB — SDMX API**           |Euro area govt zero-coupon curve (AAA-rated, 0.25–30yr), €STR, ECB policy rates                                |SDMX REST, no auth                            |**Free reuse, commercial OK**, cite source|Daily, noon CET   |
|**NY Fed**                   |SOFR (overnight rate)                                                                                          |REST/CSV                                      |Public domain                             |Daily, ~8:00 AM ET|

### Sources (Tier 2 — Scraping, Week Two)

|Source              |Data                                                                     |Method                    |License                  |Update Freq                     |
|--------------------|-------------------------------------------------------------------------|--------------------------|-------------------------|--------------------------------|
|**Bank of England** |Gilt nominal/real yield curves (spot, forward, par), SONIA, BoE Bank Rate|Excel spreadsheet download|Non-commercial, attribute|Daily, by noon next business day|
|**BoC — Provincial**|Selected provincial benchmark yields                                     |Valet API                 |Same as BoC              |Daily                           |

### Storage Layer

```
thalweg/
├── data/
│   ├── raw/                    # Raw API responses (JSON/XML/CSV), retained 7 days
│   ├── curves/                 # Processed yield curves
│   │   ├── gov_cad.parquet     # GoC zero-coupon curve, daily
│   │   ├── gov_usd.parquet     # UST par yield curve, daily
│   │   ├── gov_eur.parquet     # ECB AAA govt curve, daily
│   │   ├── gov_gbp.parquet     # UK gilt nominal curve, daily
│   │   └── swap_usd.parquet    # H.15 USD swap rates, daily
│   ├── rates/                  # Overnight / policy rates
│   │   ├── overnight.parquet   # CORRA, SOFR, SONIA, €STR
│   │   └── policy.parquet      # BoC, Fed, ECB, BoE policy rates
│   └── derived/                # Computed analytics
│       ├── spreads.parquet     # Cross-market spreads
│       ├── slopes.parquet      # 2s10s, 2s30s, 5s30s per curve
│       ├── curvature.parquet   # Butterfly spreads
│       └── regimes.parquet     # Regime classification labels
```

**Schema for curve parquet files:**

|Column       |Type |Description                         |
|-------------|-----|------------------------------------|
|`date`       |date |Observation date                    |
|`currency`   |str  |CAD, USD, EUR, GBP                  |
|`curve_type` |str  |govt_zero, govt_par, swap           |
|`tenor_years`|float|0.25, 0.5, 1, 2, 3, 5, 7, 10, 20, 30|
|`yield_pct`  |float|Yield in percent (e.g. 3.45)        |

This flat schema allows arbitrary slicing: `df[df.date == today & df.currency == 'CAD']` gives you a curve.

### Fetch Pipeline

```
┌─────────────┐     ┌──────────────┐     ┌──────────────┐     ┌──────────────┐
│  Schedulers  │────▶│   Fetchers   │────▶│  Processors  │────▶│   Parquet    │
│  (cron/APSch)│     │  (per source)│     │ (normalize)  │     │   Storage    │
└─────────────┘     └──────────────┘     └──────────────┘     └──────────────┘
                                                                     │
                                                              ┌──────▼──────┐
                                                              │  Analytics  │
                                                              │  (derived)  │
                                                              └──────┬──────┘
                                                                     │
                                                              ┌──────▼──────┐
                                                              │   Web API   │
                                                              │  (FastAPI)  │
                                                              └─────────────┘
```

Each fetcher is a standalone Python module:

- `fetchers/boc.py` — Bank of Canada Valet
- `fetchers/ust.py` — US Treasury XML feed
- `fetchers/ecb.py` — ECB SDMX
- `fetchers/boe.py` — Bank of England spreadsheet
- `fetchers/fed_h15.py` — H.15 swap rates + SOFR

Each fetcher:

1. Pulls raw data, saves to `data/raw/` with timestamp
1. Normalizes to common schema (date, currency, curve_type, tenor_years, yield_pct)
1. Appends to the relevant parquet file
1. Logs success/failure

Schedule: single daily cron job at 18:00 ET (after all sources have published).

Backfill: each fetcher has a `backfill(start_date, end_date)` function for historical data loading.

-----

## Analytics Engine

### Core Derived Metrics (computed after each daily fetch)

**Slopes:**

- 2s10s, 2s30s, 5s30s for each curve
- Cross-market slope differentials

**Curvature (butterfly spreads):**

- 2s5s10s: 2×Y(5) − Y(2) − Y(10)
- 5s10s30s: 2×Y(10) − Y(5) − Y(30)

**Cross-market spreads:**

- UST–GoC spread at each matched tenor
- Bund–Gilt spread (EUR govt vs GBP govt)
- UST–Bund spread
- GoC–Gilt spread

**Swap spreads (USD only for MVP):**

- USD swap spread = swap rate − UST yield, per tenor

### Advanced Analytics

#### 1. Conditional Curve Forecasting

**Question:** Given today's yield curve shape, what is the distribution of shapes 1m/3m/6m/12m forward?

**Method — PCA + regime-conditioned simulation:**

```
Historical curves (N×T matrix, N = ~5000 days, T = ~12 tenors)
     │
     ▼
PCA decomposition → 3 principal components explain ~99% of variance
  PC1 ≈ level (parallel shift)
  PC2 ≈ slope (steepening/flattening)
  PC3 ≈ curvature (butterfly)
     │
     ▼
Today's curve → project into PC space → (pc1_today, pc2_today, pc3_today)
     │
     ▼
Find historical analogs: days where (PC1, PC2, PC3) were within
  ε-neighborhood of today's position
     │
     ▼
Observe what happened Δt days later → distribution of future PC positions
     │
     ▼
Reconstruct curve shapes from future PC distributions → fan chart
```

This is a non-parametric, historically-grounded approach. No model assumptions about mean reversion or drift — just "when the curve looked like this before, here's what happened next."

**Alternative/complementary:** VAR(1) model on PC scores for parametric fans.

#### 2. Shock Confidence Intervals

**Question:** What are the 95th/99th/99.5th percentile moves over a given horizon?

**Method — empirical quantile estimation on PC changes:**

```
Compute ΔPC(h) = PC(t+h) − PC(t) for all historical t, horizon h
     │
     ▼
Fit multivariate distribution (empirical or t-copula for fat tails)
     │
     ▼
Sample or compute quantiles at 95%, 99%, 99.5%
     │
     ▼
Reconstruct curve shapes at each quantile → shock surfaces
     │
     ▼
Visualize as graduated color bands around the current curve
```

The key insight: shocks are not parallel. A 99th percentile move has a *shape* — typically a bear steepener or bull flattener, not a uniform shift. The visualization should make this obvious.

#### 3. Regime Classification

**Question:** What "regime" is the curve in, and how does that contextualize everything?

**Method — simple rule-based + optional clustering:**

Rule-based regimes (interpretable):

- **Normal steep:** 2s10s > 100bp, level < historical median
- **Flat:** |2s10s| < 25bp
- **Inverted:** 2s10s < −25bp
- **Bear steep:** rising level + rising slope (hiking cycle beginning)
- **Bull flat:** falling level + falling slope (cutting cycle beginning)
- **Bear flat:** rising level + falling slope (late cycle)
- **Bull steep:** falling level + rising slope (recovery)

Optional: K-means on (level, slope, curvature) for data-driven regimes.

Display current regime label on dashboard with historical regime timeline.

-----

## Visualization Design

### Design Philosophy

The yield curve is a landscape. Its changes over time form a topography.
Thalweg should make you *feel* the terrain — not just read numbers.

Three visual registers:

1. **Glanceable** — the office wall display, dark mode, key numbers legible from 3 meters
1. **Analytical** — interactive charts for desktop exploration
1. **Beautiful** — the topographic / landscape visualizations that make curves art

### Dashboard Layout (Glanceable Mode — Office Display)

```
┌─────────────────────────────────────────────────────────────┐
│                     CURVEBOARD                    2026-03-27│
├──────────────────────────┬──────────────────────────────────┤
│                          │                                  │
│   YIELD CURVES (today)   │   CURVE CHANGES (vs yesterday,  │
│                          │   vs 1w, vs 1m, vs 1y)          │
│   4 curves overlaid      │                                  │
│   CAD / USD / EUR / GBP  │   Small multiples: 4 curves ×   │
│                          │   change horizon                 │
│   Tenors on x-axis       │                                  │
│   Color-coded by country │                                  │
│                          │                                  │
├──────────────┬───────────┴──────────────────────────────────┤
│              │                                              │
│  KEY RATES   │   SLOPES & SPREADS                           │
│              │                                              │
│  CORRA  4.25 │   2s10s:  CAD +45  USD +32  EUR +18  GBP +62│
│  SOFR   4.30 │   GoC-UST 10yr spread: −28bp                │
│  €STR   2.50 │   Bund-Gilt 10yr spread: −112bp             │
│  SONIA  4.50 │   USD 10yr swap spread: +8bp                │
│              │                                              │
│  ▲▼ arrows  │   Regime: NORMAL STEEP (CAD), FLAT (EUR)     │
│  for daily Δ │                                              │
└──────────────┴──────────────────────────────────────────────┘
```

Color palette (dark mode):

- Background: `#0a0a0f` (near black with slight blue)
- CAD: `#e74c3c` (warm red)
- USD: `#3498db` (blue)
- EUR: `#f1c40f` (gold)
- GBP: `#2ecc71` (green)
- Grid lines: `#1a1a2e`
- Text: `#e0e0e0`
- Positive change: `#2ecc71`
- Negative change: `#e74c3c`

### Analytical Views (Desktop Interactive)

#### A. Curve Explorer

- Select any date, overlay multiple dates
- Drag-slider to scrub through time (curve animates)
- Toggle between zero-coupon, par, forward interpretations (where data permits)
- Click a tenor point to see its full history as a time series

#### B. Spread Dashboard

- Time series of key spreads with regime shading in background
- Cross-market spread heatmap (tenor × currency pair)

#### C. Shock Fans

- Current curve with fan bands at 50/75/90/95/99/99.5% confidence
- Color gradient from transparent center to saturated edges
- Horizon selector: 1m, 3m, 6m, 12m
- The fan is NOT symmetric — it reflects actual historical skew

#### D. Conditional Forecast

- "If today's curve shape persists, here's the distribution of outcomes in X months"
- Shown as a spaghetti plot of historical analogs overlaid with density coloring
- Median path highlighted

### Beautiful / Topographic Views

These are the signature visuals — what makes Thalweg distinctive.

#### E. Yield Surface (3D Topography)

- X-axis: tenor (0.25–30yr)
- Y-axis: time (scrolling back months/years)
- Z-axis / color: yield level
- Renders as a 3D terrain or contour map
- Inversions appear as valleys/ridges
- Rate hiking cycles appear as rising plateaus
- Think: geological strata of monetary policy

Visualization approach: WebGL (Three.js) for 3D, or D3 contour plot for 2D topographic.

#### F. Curve Velocity Field

- Plot the *change* in the curve as a vector field
- Each tenor gets an arrow showing direction and magnitude of recent movement
- Reveals whether the curve is steepening from the front end or back end
- Animated over time → shows the "flow" of rate changes

#### G. Phase Space Portrait

- Plot PC1 (level) vs PC2 (slope) as a 2D phase diagram
- Each day is a point; connect them to trace the curve's path through regime space
- Color-code by time (old = faded, recent = bright)
- Overlay regime boundaries
- Current position marked with a pulsing dot
- This is the "map of all possible curve shapes" — and you can see where you are

#### H. Probability Landscape

- For the shock confidence intervals, render as a topographic "probability terrain"
- Current curve at the peak of a hill
- Contour lines show probability density of future curve positions
- Darker/warmer colors = higher probability
- The 99.5th percentile shock is the edge of the visible landscape
- Essentially a 2D density plot in (level, slope) space with the curve reconstructed at each point

-----

## Tech Stack

### Backend

```
thalweg/
├── pyproject.toml              # uv-managed, Python 3.12+
├── src/
│   └── thalweg/
│       ├── __init__.py
│       ├── config.py           # Settings, paths, data source configs
│       ├── fetchers/           # One module per data source
│       │   ├── base.py         # Abstract fetcher with retry, logging
│       │   ├── boc.py          # Bank of Canada Valet
│       │   ├── ust.py          # US Treasury XML
│       │   ├── ecb.py          # ECB SDMX
│       │   ├── boe.py          # Bank of England spreadsheet
│       │   └── fed_h15.py      # H.15 swap rates
│       ├── storage.py          # Parquet read/write, append, query
│       ├── analytics/
│       │   ├── pca.py          # PCA decomposition of curves
│       │   ├── regimes.py      # Regime classification
│       │   ├── spreads.py      # Spread/slope/curvature calcs
│       │   ├── shocks.py       # Empirical shock distributions
│       │   └── forecast.py     # Conditional analog forecasting
│       ├── web/
│       │   ├── app.py          # FastAPI application
│       │   ├── api.py          # JSON endpoints for frontend
│       │   └── static/         # Frontend build output
│       └── cli.py              # CLI for fetch, backfill, status
├── frontend/                   # Phase 3+ only (React + Three.js)
│   ├── src/
│   │   ├── App.jsx
│   │   ├── components/
│   │   │   ├── Dashboard.jsx       # Glanceable wall display
│   │   │   ├── CurveExplorer.jsx   # Interactive curve browser
│   │   │   ├── ShockFan.jsx        # Confidence interval fans
│   │   │   ├── YieldSurface.jsx    # 3D topography (Three.js)
│   │   │   ├── PhaseSpace.jsx      # PC1 vs PC2 regime map
│   │   │   └── SpreadDash.jsx      # Spread time series
│   │   └── lib/
│   │       ├── colors.js           # Palette constants
│   │       └── curves.js           # Interpolation, formatting
│   └── package.json
└── tests/
```

**Key dependencies:**

- `httpx` — async HTTP for fetchers
- `polars` — dataframe ops
- `pyarrow` — parquet I/O
- `scikit-learn` — PCA, clustering
- `scipy` — statistical distributions, interpolation
- `fastapi` + `uvicorn` — web server
- `click` — CLI framework
- `openpyxl` — BoE spreadsheet parsing

### Frontend (MVP — Phases 0–2): HTMX + D3

- FastAPI serves HTML pages with HTMX for interactivity
- D3.js for all 2D charts (yield curves, time series, phase diagrams, contour plots)
- Charts rendered client-side in D3, data served as JSON from FastAPI endpoints
- Dashboard is a single HTML page with auto-refresh (15 min interval)
- Tailwind for utility styling
- Dark mode as default (office display primary use case)

### Frontend (Phase 3+): React + Three.js

- Migrate interactive views to React when building 3D topographic visualizations
- Three.js for yield surface rendering
- D3 charts wrapped in React components
- Only introduced when HTMX hits its limits (complex state, 3D interaction)

-----

## Deployment

### Primary: Hetzner VPS (alongside ALM Sidecar)

```
Hetzner CPX22 (existing)
├── ALM Sidecar (port 8000)
├── Winnow (periodic job)
└── Thalweg
    ├── Data fetcher (cron, 18:00 ET daily)
    ├── Analytics recompute (after fetch)
    └── Web server (port 8001)
         └── Accessible via Tailscale
```

Cron schedule:

```
# Fetch all sources daily at 18:00 ET (23:00 UTC in winter, 22:00 UTC in summer)
0 23 * * 1-5 cd /path/to/thalweg && uv run python -m thalweg.cli fetch --all
# Recompute analytics after fetch
5 23 * * 1-5 cd /path/to/thalweg && uv run python -m thalweg.cli analyze
```

### Display: Raspberry Pi in Office

- Pi 4 or Pi 5 running Chromium in kiosk mode
- Connects to Thalweg web server over Tailscale
- Fullscreen dark-mode dashboard
- Auto-refreshes every 15 minutes during market hours
- Could rotate between dashboard and topographic views

```bash
# Pi kiosk setup
chromium-browser --kiosk --noerrdialogs \
  --disable-infobars --disable-session-crashed-bubble \
  "http://thalweg.tailnet:8001/dashboard"
```

### Optional: Public Website

If you want to put it on the internet:

- Caddy reverse proxy with automatic HTTPS on the VPS
- Subdomain: `curves.yourdomain.com`
- Attribution footer for all data sources
- Non-commercial framing (no ads, no paywalls)
- Open-source the repo on GitHub

-----

## MVP Phases

### Phase 0: Data Foundation (1–2 sessions with Claude Code)

- [ ] Project scaffold with uv
- [ ] BoC fetcher + UST fetcher (the two easiest APIs)
- [ ] Parquet storage layer
- [ ] Backfill 10 years of GoC + UST data
- [ ] CLI: `thalweg fetch`, `thalweg status`
- **Exit criteria:** 10 years of daily CAD + USD curves in parquet files

### Phase 1: Dashboard MVP (2–3 sessions)

- [ ] ECB fetcher + BoE fetcher
- [ ] H.15 swap rate fetcher
- [ ] Overnight rate fetchers (CORRA, SOFR, SONIA, €STR)
- [ ] Spread/slope/curvature calculations
- [ ] FastAPI server with JSON API
- [ ] Dashboard page: 4 curves overlaid, key rates, slopes, spreads
- [ ] Dark mode, designed for wall display
- [ ] Pi kiosk deployment
- **Exit criteria:** Working wall display showing today's curves and key metrics

### Phase 2: History & Context (2–3 sessions)

- [ ] Curve Explorer: date slider, overlay comparison
- [ ] Spread time series charts
- [ ] Regime classification (rule-based)
- [ ] Regime timeline on dashboard
- [ ] "Curve change" small multiples (vs 1d/1w/1m/1y)
- **Exit criteria:** Can explore any historical date and see regime context

### Phase 3: Analytics (3–4 sessions)

- [ ] PCA decomposition module
- [ ] Phase space portrait visualization (PC1 vs PC2)
- [ ] Empirical shock distributions
- [ ] Shock fan charts with confidence bands
- [ ] Conditional analog forecasting
- [ ] Probability landscape visualization
- **Exit criteria:** Can see shock fans and forecast distributions for any curve

### Phase 4: Beautiful (2–3 sessions)

- [ ] 3D yield surface (Three.js)
- [ ] Curve velocity field
- [ ] Topographic contour renderings
- [ ] Animation: scrub through time on the yield surface
- [ ] Polish all visualizations for "art on the wall" quality
- **Exit criteria:** Would hang a screenshot on the wall

### Phase 5: Public Release (1–2 sessions)

- [ ] Attribution footer on all pages
- [ ] About page with data source documentation
- [ ] Caddy HTTPS setup
- [ ] Open-source repo with README
- [ ] Share on r/fixedincome, LinkedIn, etc.

-----

## Design Decisions

1. **Polars over Pandas.** Polars for all storage and query. Convert to numpy at sklearn/scipy boundaries only.
1. **HTMX + D3 for MVP, React later.** Ship the dashboard and basic curve views with HTMX and server-rendered pages. Introduce React + Three.js only in Phase 3–4 for interactive 3D visualizations.
1. **Backfill all available history.** BoC back to ~1986, UST back to 1960s, ECB from 2004, BoE from 1979. Analytics use 2004+ as the common start across all four markets.
1. **Forward curves in Phase 2.** Derive instantaneous forward rates from zero-coupon curves (BoC, ECB). Bootstrap zeros from UST par yields. The forward curve reveals expectations more directly than the spot curve.
1. **No stored interpolation.** Store raw tenors only. Interpolate at query time via cubic spline for overlay charts. Never persist interpolated data as if observed.
1. **BoE via spreadsheet download.** Daily cron downloads the Excel file, parses with openpyxl. More robust than scraping individual series codes.

-----

## What Makes This Different

There are yield curve websites. There are bond data APIs. There isn't a tool that:

- Focuses specifically on the four currencies that matter for Canadian insurance ALM
- Derives cross-market spreads and presents them as first-class metrics
- Runs PCA-based analytics to decompose curve movements into interpretable factors
- Shows historically-conditioned probabilistic fans (not parametric assumptions)
- Renders the yield curve as a *landscape* — topographic, geological, beautiful
- Runs on your own infrastructure, under your own control
- Is open-source and built with transparent methodology

This is a professional tool that happens to produce art. Or art that happens to be a professional tool.

-----

## Backlog (Future Phases)

Items to incorporate beyond the initial MVP phases:

1. **Real rates and inflation curves.** The BoC publishes Real Return Bond data. The US Treasury publishes TIPS-derived real yield curves (available via the same XML feed and FRED). The ECB publishes inflation-linked yield curves. The BoE publishes real gilt curves alongside nominal. Thalweg should eventually show nominal curves, real curves, and the implied breakeven inflation term structure (nominal − real) for each market. This is critical for ALM — liability discounting under IFRS 17 and LICAT depends on whether you're looking at nominal or real obligations. Breakeven inflation surfaces are also visually striking and would fit naturally into the topographic visualization framework.
1. **Provincial / sub-sovereign spreads (Canada).** BoC publishes some provincial benchmark yields. Display GoC-provincial spreads by tenor — directly relevant for Canadian insurance investment portfolios.
1. **Credit spreads.** IG corporate bond index spreads (if a free data source can be found). OAS by rating tier.
1. **Forward rate curves.** Derive instantaneous forward rates from zero-coupon curves. The forward curve reveals expectations about future rate paths more directly than the spot curve.
1. **Swap spread term structure.** Currently MVP only has USD swap spreads. Extend to CAD (CORRA-based), EUR (€STR-based), GBP (SONIA-based) as data becomes available.
1. **Mobile / responsive layout.** The dashboard is designed for a wall-mounted screen. A phone-friendly view for checking curves on the go would be useful.
1. **Alerts / notifications.** Configurable alerts when spreads, slopes, or levels breach thresholds (e.g., "GoC 2s10s inverted", "UST-GoC 10yr spread > 50bp").
