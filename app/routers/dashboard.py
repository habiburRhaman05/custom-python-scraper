"""
Admin analytics dashboard — a self-contained HTML page over the existing JSON API.

No server-side templating and no new secret surface: the page is static
HTML/JS that calls the same /api/* endpoints documented at /docs. The admin
API key is entered once in the browser and kept in sessionStorage — it is
never embedded in the HTML the server returns.
"""

from __future__ import annotations

from pathlib import Path

from fastapi import APIRouter
from fastapi.responses import HTMLResponse

router = APIRouter(tags=["dashboard"])

_TEMPLATE_PATH = Path(__file__).resolve().parent.parent / "templates" / "dashboard.html"


@router.get("/admin/dashboard", response_class=HTMLResponse, include_in_schema=False)
def dashboard() -> str:
    return _TEMPLATE_PATH.read_text(encoding="utf-8")
