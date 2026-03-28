"""Tests for the JSON API."""

from __future__ import annotations

from datetime import date

import polars as pl
import pytest
from httpx import ASGITransport, AsyncClient

from thalweg import storage
from thalweg.web.app import create_app


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture()
def tmp_data_dir(tmp_path, monkeypatch):
    """Point storage at temporary directories."""
    curves_dir = tmp_path / "curves"
    curves_dir.mkdir()
    rates_dir = tmp_path / "rates"
    rates_dir.mkdir()
    derived_dir = tmp_path / "derived"
    derived_dir.mkdir()
    monkeypatch.setattr(storage, "CURVES_DIR", curves_dir)
    monkeypatch.setattr(storage, "RATES_DIR", rates_dir)
    monkeypatch.setattr(storage, "DERIVED_DIR", derived_dir)
    return tmp_path


@pytest.fixture()
def app():
    """Create a fresh FastAPI application."""
    return create_app()


@pytest.fixture()
async def client(app):
    """Async test client wired to the ASGI app."""
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as c:
        yield c


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_curve(
    obs_date: date,
    currency: str = "CAD",
    curve_type: str = "govt_par",
    tenors: list[float] | None = None,
    base_yield: float = 3.0,
) -> pl.DataFrame:
    """Build a small curve DataFrame for testing."""
    tenors = tenors or [2.0, 5.0, 10.0, 30.0]
    return pl.DataFrame(
        {
            "date": [obs_date] * len(tenors),
            "currency": [currency] * len(tenors),
            "curve_type": [curve_type] * len(tenors),
            "tenor_years": tenors,
            "yield_pct": [base_yield + i * 0.25 for i in range(len(tenors))],
        }
    ).cast({"date": pl.Date, "tenor_years": pl.Float64, "yield_pct": pl.Float64})


def _make_rates(obs_date: date, rate_name: str = "CORRA", value: float = 4.5) -> pl.DataFrame:
    """Build a small overnight-rate DataFrame for testing."""
    return pl.DataFrame(
        {
            "date": [obs_date],
            "rate_name": [rate_name],
            "value_pct": [value],
        }
    ).cast({"date": pl.Date, "value_pct": pl.Float64})


# ---------------------------------------------------------------------------
# Root / dashboard
# ---------------------------------------------------------------------------


async def test_root_redirects_to_dashboard(client: AsyncClient) -> None:
    """GET / should 307-redirect to /dashboard."""
    resp = await client.get("/", follow_redirects=False)
    assert resp.status_code == 307
    assert resp.headers["location"] == "/dashboard"


async def test_dashboard_renders(client: AsyncClient) -> None:
    """GET /dashboard should return 200 with HTML."""
    resp = await client.get("/dashboard")
    assert resp.status_code == 200
    assert "Thalweg" in resp.text


async def test_explorer_renders(client: AsyncClient) -> None:
    """GET /explorer should return 200 with HTML."""
    resp = await client.get("/explorer")
    assert resp.status_code == 200
    assert "Curve Explorer" in resp.text


async def test_analytics_renders(client: AsyncClient) -> None:
    """GET /analytics should return 200 with HTML."""
    resp = await client.get("/analytics")
    assert resp.status_code == 200
    assert "Analytics" in resp.text


# ---------------------------------------------------------------------------
# /api/curves/latest
# ---------------------------------------------------------------------------


async def test_curves_latest_empty(tmp_data_dir, client: AsyncClient) -> None:
    """No data on disk returns an empty list."""
    resp = await client.get("/api/curves/latest")
    assert resp.status_code == 200
    assert resp.json() == {"curves": []}


async def test_curves_latest_with_data(tmp_data_dir, client: AsyncClient) -> None:
    """Seeded curves should appear in the latest endpoint."""
    storage.append_curves(_make_curve(date(2024, 1, 15), currency="CAD"))
    storage.append_curves(_make_curve(date(2024, 1, 16), currency="CAD"))

    resp = await client.get("/api/curves/latest")
    assert resp.status_code == 200
    body = resp.json()
    assert len(body["curves"]) == 4  # 4 tenors, only latest date
    assert all(r["date"] == "2024-01-16" for r in body["curves"])


async def test_curves_latest_multi_currency(tmp_data_dir, client: AsyncClient) -> None:
    """Each currency returns its own latest date."""
    storage.append_curves(_make_curve(date(2024, 1, 15), currency="CAD"))
    storage.append_curves(_make_curve(date(2024, 1, 20), currency="USD"))

    resp = await client.get("/api/curves/latest")
    body = resp.json()
    dates_by_ccy = {}
    for r in body["curves"]:
        dates_by_ccy.setdefault(r["currency"], set()).add(r["date"])
    assert dates_by_ccy["CAD"] == {"2024-01-15"}
    assert dates_by_ccy["USD"] == {"2024-01-20"}


# ---------------------------------------------------------------------------
# /api/curves
# ---------------------------------------------------------------------------


async def test_curves_query_filters(tmp_data_dir, client: AsyncClient) -> None:
    """Currency filter should restrict results."""
    storage.append_curves(_make_curve(date(2024, 1, 15), currency="CAD"))
    storage.append_curves(_make_curve(date(2024, 1, 15), currency="USD"))

    resp = await client.get("/api/curves", params={"currency": "CAD"})
    assert resp.status_code == 200
    body = resp.json()
    assert len(body["curves"]) == 4
    assert all(r["currency"] == "CAD" for r in body["curves"])


async def test_curves_date_range_filter(tmp_data_dir, client: AsyncClient) -> None:
    """Date range filters should work."""
    storage.append_curves(_make_curve(date(2024, 1, 10), currency="CAD"))
    storage.append_curves(_make_curve(date(2024, 3, 10), currency="CAD"))

    resp = await client.get(
        "/api/curves",
        params={"currency": "CAD", "start_date": "2024-02-01", "end_date": "2024-12-31"},
    )
    body = resp.json()
    assert len(body["curves"]) == 4
    assert all(r["date"] == "2024-03-10" for r in body["curves"])


# ---------------------------------------------------------------------------
# /api/rates/overnight
# ---------------------------------------------------------------------------


async def test_rates_overnight_empty(tmp_data_dir, client: AsyncClient) -> None:
    """No rate data returns an empty list."""
    resp = await client.get("/api/rates/overnight")
    assert resp.status_code == 200
    assert resp.json() == {"rates": []}


async def test_rates_overnight_with_data(tmp_data_dir, client: AsyncClient) -> None:
    """Seeded rates appear and only latest per rate_name is returned."""
    storage.append_rates(_make_rates(date(2024, 1, 15), "CORRA", 4.5))
    storage.append_rates(_make_rates(date(2024, 1, 16), "CORRA", 4.6))
    storage.append_rates(_make_rates(date(2024, 1, 16), "SOFR", 5.3))

    resp = await client.get("/api/rates/overnight")
    body = resp.json()
    assert len(body["rates"]) == 2  # CORRA latest + SOFR latest
    rates_map = {r["rate_name"]: r for r in body["rates"]}
    assert rates_map["CORRA"]["date"] == "2024-01-16"
    assert rates_map["CORRA"]["value_pct"] == pytest.approx(4.6)
    assert rates_map["SOFR"]["value_pct"] == pytest.approx(5.3)
    # CORRA has a prior day so change_bp should be present
    assert rates_map["CORRA"]["change_bp"] == pytest.approx((4.6 - 4.5) * 100)
    # SOFR has only one day so change_bp should be null
    assert rates_map["SOFR"]["change_bp"] is None


# ---------------------------------------------------------------------------
# /api/analytics/slopes
# ---------------------------------------------------------------------------


async def test_analytics_slopes_empty(tmp_data_dir, client: AsyncClient) -> None:
    """No data returns empty slopes."""
    resp = await client.get("/api/analytics/slopes")
    assert resp.status_code == 200
    assert resp.json() == {"slopes": []}


async def test_analytics_slopes(tmp_data_dir, client: AsyncClient) -> None:
    """Seeded curves should produce 2s10s, 2s30s, 5s30s slopes."""
    storage.append_curves(_make_curve(date(2024, 1, 15), currency="CAD"))

    resp = await client.get("/api/analytics/slopes")
    body = resp.json()
    slope_names = {s["slope_name"] for s in body["slopes"]}
    assert slope_names == {"2s10s", "2s30s", "5s30s", "5s10s"}
    # Verify a specific slope value: 2s10s = (10yr - 2yr) * 100 bp
    slopes_map = {s["slope_name"]: s["value_bp"] for s in body["slopes"]}
    # base_yield=3.0, 2yr=3.0, 10yr=3.5 -> 50bp
    assert slopes_map["2s10s"] == pytest.approx(50.0)


# ---------------------------------------------------------------------------
# /api/analytics/curvature
# ---------------------------------------------------------------------------


async def test_analytics_curvature_empty(tmp_data_dir, client: AsyncClient) -> None:
    """No data returns empty curvature."""
    resp = await client.get("/api/analytics/curvature")
    assert resp.status_code == 200
    assert resp.json() == {"curvature": []}


async def test_analytics_curvature(tmp_data_dir, client: AsyncClient) -> None:
    """Seeded curves should produce butterfly spreads."""
    storage.append_curves(_make_curve(date(2024, 1, 15), currency="CAD"))

    resp = await client.get("/api/analytics/curvature")
    body = resp.json()
    bfly_names = {b["butterfly_name"] for b in body["curvature"]}
    assert bfly_names == {"2s5s10s", "5s10s30s"}


# ---------------------------------------------------------------------------
# /api/analytics/spreads
# ---------------------------------------------------------------------------


async def test_analytics_spreads_empty(tmp_data_dir, client: AsyncClient) -> None:
    """No data returns empty spreads."""
    resp = await client.get("/api/analytics/spreads")
    assert resp.status_code == 200
    assert resp.json() == {"spreads": []}


async def test_analytics_spreads(tmp_data_dir, client: AsyncClient) -> None:
    """Two currencies on the same date should produce cross-market spreads."""
    storage.append_curves(_make_curve(date(2024, 1, 15), currency="USD", base_yield=4.0))
    storage.append_curves(_make_curve(date(2024, 1, 15), currency="CAD", base_yield=3.0))

    resp = await client.get("/api/analytics/spreads")
    body = resp.json()
    assert len(body["spreads"]) > 0
    # All spreads should be for the USD-CAD pair
    assert all(s["pair"] == "USD-CAD" for s in body["spreads"])
    # At matched tenors, spread should be ~100bp (4.0 - 3.0 = 1.0pct = 100bp)
    two_yr = [s for s in body["spreads"] if s["tenor_years"] == 2.0]
    assert len(two_yr) == 1
    assert two_yr[0]["spread_bp"] == pytest.approx(100.0)


# ---------------------------------------------------------------------------
# /api/curves/changes
# ---------------------------------------------------------------------------


async def test_curves_changes_empty(tmp_data_dir, client: AsyncClient) -> None:
    """No data returns empty changes."""
    resp = await client.get("/api/curves/changes")
    assert resp.status_code == 200
    assert resp.json() == {"changes": []}


async def test_curves_changes_with_data(tmp_data_dir, client: AsyncClient) -> None:
    """Curves at two dates should produce at least the 1d change."""
    storage.append_curves(_make_curve(date(2024, 1, 14), currency="CAD", base_yield=3.0))
    storage.append_curves(_make_curve(date(2024, 1, 15), currency="CAD", base_yield=3.1))

    resp = await client.get("/api/curves/changes")
    body = resp.json()
    assert len(body["changes"]) > 0
    # 1d horizon should show up
    horizons = {c["horizon"] for c in body["changes"]}
    assert "1d" in horizons
    # Change at 2yr tenor: 3.1 - 3.0 = 0.1
    one_d = [c for c in body["changes"] if c["horizon"] == "1d" and c["tenor_years"] == 2.0]
    assert len(one_d) == 1
    assert one_d[0]["change_pct"] == pytest.approx(0.1)


# ---------------------------------------------------------------------------
# /api/curves/dates (Phase 2)
# ---------------------------------------------------------------------------


async def test_curves_dates_empty(tmp_data_dir, client: AsyncClient) -> None:
    """No data returns empty dates list."""
    resp = await client.get("/api/curves/dates")
    assert resp.status_code == 200
    assert resp.json() == {"dates": []}


async def test_curves_dates_with_data(tmp_data_dir, client: AsyncClient) -> None:
    """Seeded curves return sorted unique dates."""
    storage.append_curves(_make_curve(date(2024, 1, 15), currency="CAD"))
    storage.append_curves(_make_curve(date(2024, 3, 10), currency="CAD"))

    resp = await client.get("/api/curves/dates")
    body = resp.json()
    assert body["dates"] == ["2024-01-15", "2024-03-10"]


async def test_curves_dates_filtered(tmp_data_dir, client: AsyncClient) -> None:
    """Currency filter restricts dates to that currency."""
    storage.append_curves(_make_curve(date(2024, 1, 15), currency="CAD"))
    storage.append_curves(_make_curve(date(2024, 1, 20), currency="USD"))

    resp = await client.get("/api/curves/dates", params={"currency": "CAD"})
    body = resp.json()
    assert body["dates"] == ["2024-01-15"]


# ---------------------------------------------------------------------------
# /api/regimes (Phase 2)
# ---------------------------------------------------------------------------


def _make_regime_data(
    obs_date: date, currency: str = "USD", regime: str = "normal",
) -> pl.DataFrame:
    return pl.DataFrame({
        "date": [obs_date],
        "currency": [currency],
        "curve_type": ["govt_par"],
        "regime": [regime],
        "slope_2s10s_bp": [50.0],
        "level_10y": [4.0],
    }).cast({"date": pl.Date, "slope_2s10s_bp": pl.Float64, "level_10y": pl.Float64})


async def test_regimes_latest_empty(tmp_data_dir, client: AsyncClient) -> None:
    """No regime data returns empty list."""
    resp = await client.get("/api/regimes/latest")
    assert resp.status_code == 200
    assert resp.json() == {"regimes": []}


async def test_regimes_latest_with_data(tmp_data_dir, client: AsyncClient) -> None:
    """Returns latest regime per currency."""
    df = pl.concat([
        _make_regime_data(date(2024, 1, 15), "USD", "normal"),
        _make_regime_data(date(2024, 1, 20), "USD", "inverted"),
        _make_regime_data(date(2024, 1, 18), "CAD", "flat"),
    ])
    storage.append_regimes(df)

    resp = await client.get("/api/regimes/latest")
    body = resp.json()
    assert len(body["regimes"]) == 2
    regimes_map = {r["currency"]: r["regime"] for r in body["regimes"]}
    assert regimes_map["USD"] == "inverted"
    assert regimes_map["CAD"] == "flat"


async def test_regimes_query_empty(tmp_data_dir, client: AsyncClient) -> None:
    """No regime data returns empty list."""
    resp = await client.get("/api/regimes")
    assert resp.status_code == 200
    assert resp.json() == {"regimes": []}


async def test_regimes_query_filtered(tmp_data_dir, client: AsyncClient) -> None:
    """Currency filter restricts results."""
    df = pl.concat([
        _make_regime_data(date(2024, 1, 15), "USD", "normal"),
        _make_regime_data(date(2024, 1, 15), "CAD", "flat"),
    ])
    storage.append_regimes(df)

    resp = await client.get("/api/regimes", params={"currency": "USD"})
    body = resp.json()
    assert len(body["regimes"]) == 1
    assert body["regimes"][0]["currency"] == "USD"


# ---------------------------------------------------------------------------
# /api/analytics/slopes/history (Phase 2)
# ---------------------------------------------------------------------------


async def test_slopes_history_empty(tmp_data_dir, client: AsyncClient) -> None:
    """No derived data returns empty list."""
    resp = await client.get("/api/analytics/slopes/history")
    assert resp.status_code == 200
    assert resp.json() == {"slopes": []}


async def test_slopes_history_with_data(tmp_data_dir, client: AsyncClient) -> None:
    """Seeded slopes parquet returns filtered results."""
    slopes_df = pl.DataFrame({
        "date": [date(2024, 1, 15), date(2024, 1, 15)],
        "currency": ["USD", "CAD"],
        "curve_type": ["govt_par", "govt_par"],
        "slope_name": ["2s10s", "2s10s"],
        "value_bp": [50.0, 80.0],
    }).cast({"date": pl.Date, "value_bp": pl.Float64})
    slopes_df.write_parquet(storage.DERIVED_DIR / "slopes.parquet")

    resp = await client.get(
        "/api/analytics/slopes/history", params={"currency": "USD"}
    )
    body = resp.json()
    assert len(body["slopes"]) == 1
    assert body["slopes"][0]["currency"] == "USD"


# ---------------------------------------------------------------------------
# /api/analytics/spreads/history (Phase 2)
# ---------------------------------------------------------------------------


async def test_spreads_history_empty(tmp_data_dir, client: AsyncClient) -> None:
    """No derived data returns empty list."""
    resp = await client.get("/api/analytics/spreads/history")
    assert resp.status_code == 200
    assert resp.json() == {"spreads": []}


async def test_spreads_history_with_data(tmp_data_dir, client: AsyncClient) -> None:
    """Seeded spreads parquet returns filtered results."""
    spreads_df = pl.DataFrame({
        "date": [date(2024, 1, 15), date(2024, 1, 15)],
        "pair": ["USD-CAD", "USD-EUR"],
        "tenor_years": [10.0, 10.0],
        "spread_bp": [45.0, -30.0],
    }).cast({"date": pl.Date, "tenor_years": pl.Float64, "spread_bp": pl.Float64})
    spreads_df.write_parquet(storage.DERIVED_DIR / "spreads.parquet")

    resp = await client.get(
        "/api/analytics/spreads/history", params={"pair": "USD-CAD"}
    )
    body = resp.json()
    assert len(body["spreads"]) == 1
    assert body["spreads"][0]["pair"] == "USD-CAD"


# ---------------------------------------------------------------------------
# /api/analytics/pca/scores (Phase 3)
# ---------------------------------------------------------------------------


async def test_pca_scores_empty(tmp_data_dir, client: AsyncClient) -> None:
    """No derived data returns empty scores."""
    resp = await client.get("/api/analytics/pca/scores")
    assert resp.status_code == 200
    assert resp.json() == {"scores": []}


async def test_pca_scores_with_data(tmp_data_dir, client: AsyncClient) -> None:
    """Seeded pca_scores parquet returns filtered records."""
    scores_df = pl.DataFrame({
        "date": [date(2024, 1, 15), date(2024, 1, 16), date(2024, 1, 15)],
        "currency": ["USD", "USD", "CAD"],
        "curve_type": ["govt_par", "govt_par", "govt_par"],
        "pc1": [1.0, 1.1, 0.5],
        "pc2": [0.2, 0.3, 0.1],
        "pc3": [-0.1, -0.2, 0.0],
    }).cast({"date": pl.Date, "pc1": pl.Float64, "pc2": pl.Float64, "pc3": pl.Float64})
    scores_df.write_parquet(storage.DERIVED_DIR / "pca_scores.parquet")

    # All scores
    resp = await client.get("/api/analytics/pca/scores")
    body = resp.json()
    assert len(body["scores"]) == 3

    # Filter by currency
    resp = await client.get("/api/analytics/pca/scores", params={"currency": "USD"})
    body = resp.json()
    assert len(body["scores"]) == 2
    assert all(r["currency"] == "USD" for r in body["scores"])


# ---------------------------------------------------------------------------
# /api/analytics/pca/loadings (Phase 3)
# ---------------------------------------------------------------------------


async def test_pca_loadings_empty(tmp_data_dir, client: AsyncClient) -> None:
    """No derived data returns empty loadings and explained_variance."""
    resp = await client.get("/api/analytics/pca/loadings")
    assert resp.status_code == 200
    assert resp.json() == {"loadings": [], "explained_variance": []}


async def test_pca_loadings_with_data(tmp_data_dir, client: AsyncClient) -> None:
    """Seeded pca_loadings parquet returns loadings and explained_variance."""
    loadings_df = pl.DataFrame({
        "currency": ["USD"] * 6,
        "curve_type": ["govt_par"] * 6,
        "component": ["pc1", "pc1", "pc2", "pc2", "pc3", "pc3"],
        "tenor_years": [2.0, 10.0, 2.0, 10.0, 2.0, 10.0],
        "loading": [0.5, 0.5, -0.7, 0.7, 0.5, -0.5],
        "explained_variance_ratio": [0.85, 0.85, 0.10, 0.10, 0.03, 0.03],
    }).cast({
        "tenor_years": pl.Float64,
        "loading": pl.Float64,
        "explained_variance_ratio": pl.Float64,
    })
    loadings_df.write_parquet(storage.DERIVED_DIR / "pca_loadings.parquet")

    resp = await client.get("/api/analytics/pca/loadings", params={"currency": "USD"})
    body = resp.json()

    # Loadings should not contain explained_variance_ratio
    assert len(body["loadings"]) == 6
    assert "explained_variance_ratio" not in body["loadings"][0]
    assert all(r["currency"] == "USD" for r in body["loadings"])

    # Explained variance should be unique per component
    assert len(body["explained_variance"]) == 3
    ev_map = {r["component"]: r["explained_variance_ratio"] for r in body["explained_variance"]}
    assert ev_map["pc1"] == pytest.approx(0.85)
    assert ev_map["pc2"] == pytest.approx(0.10)
    assert ev_map["pc3"] == pytest.approx(0.03)


# ---------------------------------------------------------------------------
# /api/analytics/fan (Phase 3)
# ---------------------------------------------------------------------------


def _seed_many_curves(
    n_days: int = 60,
    currency: str = "CAD",
    base_date: date = date(2024, 1, 1),
) -> None:
    """Seed n_days of curve data so PCA has enough observations."""
    import random

    random.seed(42)
    for i in range(n_days):
        obs_date = base_date + __import__("datetime").timedelta(days=i)
        # Small random variation to give PCA non-trivial components
        base_yield = 3.0 + random.gauss(0, 0.1)
        storage.append_curves(_make_curve(obs_date, currency=currency, base_yield=base_yield))


async def test_fan_empty(tmp_data_dir, client: AsyncClient) -> None:
    """No curve data returns empty fan and current."""
    resp = await client.get("/api/analytics/fan", params={"currency": "CAD"})
    assert resp.status_code == 200
    assert resp.json() == {"fan": [], "current": []}


async def test_fan_with_data(tmp_data_dir, client: AsyncClient) -> None:
    """Seeded curves produce fan chart bands and current curve."""
    _seed_many_curves(n_days=60, currency="CAD")

    resp = await client.get(
        "/api/analytics/fan", params={"currency": "CAD", "horizon": 21}
    )
    body = resp.json()

    # Fan should have records
    assert len(body["fan"]) > 0
    assert all(r["currency"] == "CAD" for r in body["fan"])
    assert all("quantile" in r for r in body["fan"])
    assert all("tenor_years" in r for r in body["fan"])

    # Current curve should have the latest date's data
    assert len(body["current"]) > 0
    assert all(r["currency"] == "CAD" for r in body["current"])


async def test_fan_requires_currency(client: AsyncClient) -> None:
    """Fan endpoint requires currency parameter."""
    resp = await client.get("/api/analytics/fan")
    assert resp.status_code == 422  # FastAPI validation error


# ---------------------------------------------------------------------------
# /api/analytics/analogs (Phase 3)
# ---------------------------------------------------------------------------


async def test_analogs_empty(tmp_data_dir, client: AsyncClient) -> None:
    """No curve data returns empty analogs and forecasts."""
    resp = await client.get("/api/analytics/analogs", params={"currency": "CAD"})
    assert resp.status_code == 200
    assert resp.json() == {"analogs": [], "forecasts": []}


async def test_analogs_with_data(tmp_data_dir, client: AsyncClient) -> None:
    """Seeded curves produce analog dates and forecast paths."""
    # Need enough data for PCA + analog search + forecast horizon
    _seed_many_curves(n_days=150, currency="CAD")

    resp = await client.get(
        "/api/analytics/analogs",
        params={"currency": "CAD", "k": 5, "horizon": 21},
    )
    body = resp.json()

    # Should have analog dates
    assert len(body["analogs"]) > 0
    assert all(r["currency"] == "CAD" for r in body["analogs"])
    assert all("analog_date" in r for r in body["analogs"])
    assert all("distance" in r for r in body["analogs"])

    # Should have forecast paths
    assert len(body["forecasts"]) > 0
    assert all(r["currency"] == "CAD" for r in body["forecasts"])


async def test_analogs_requires_currency(client: AsyncClient) -> None:
    """Analogs endpoint requires currency parameter."""
    resp = await client.get("/api/analytics/analogs")
    assert resp.status_code == 422  # FastAPI validation error
