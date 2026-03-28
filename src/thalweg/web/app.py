"""FastAPI application factory for Thalweg."""

from __future__ import annotations

from pathlib import Path

from fastapi import FastAPI, Request
from fastapi.responses import RedirectResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

_TEMPLATES_DIR = Path(__file__).parent / "templates"
_STATIC_DIR = Path(__file__).parent / "static"


def create_app() -> FastAPI:
    """Create and configure the FastAPI application.

    Returns:
        Configured FastAPI instance with API routes, templates, and static files.
    """
    app = FastAPI(title="Thalweg", description="Yield curve observatory")

    from thalweg.web.api import router

    app.include_router(router, prefix="/api")

    templates = Jinja2Templates(directory=str(_TEMPLATES_DIR))

    if _STATIC_DIR.exists():
        app.mount("/static", StaticFiles(directory=str(_STATIC_DIR)), name="static")

    @app.get("/")
    async def root() -> RedirectResponse:
        """Redirect root to dashboard."""
        return RedirectResponse(url="/dashboard")

    @app.get("/dashboard")
    async def dashboard(request: Request):  # type: ignore[no-untyped-def]
        """Render the dashboard template."""
        return templates.TemplateResponse(request, "dashboard.html")

    return app
