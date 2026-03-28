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
    monkeypatch.setattr(storage, "CURVES_DIR", curves_dir)
    monkeypatch.setattr(storage, "RATES_DIR", rates_dir)
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
    assert slope_names == {"2s10s", "2s30s", "5s30s"}
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
