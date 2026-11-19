"""FastAPI application entrypoint."""

from __future__ import annotations

from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app import scheduler
from app.config import settings
from app.jobs import reconcile_orphaned_runs
from app.media_jobs import reconcile_orphaned_media_runs
from app.routers import admin, catalog, dashboard, meta, projects

DESCRIPTION = """
Read API over scraped Dubai off-plan real estate data.

**Data**: ~2,021 projects, 450 developers, 134 areas, 18 brokers, 27 articles.
Sourced from public listing pages and sitemaps; `robots.txt` is respected.

**Refreshing**: `POST /api/admin/scrape` with an `X-API-Key` header starts a
run in the background and returns a `run_id` to poll. Only records whose
content hash changed are written, so a run that finds nothing new writes
nothing. Field-level changes (price moves, status flips) are queryable via
`/api/changes/recent`.
"""


@asynccontextmanager
async def lifespan(app: FastAPI):
    if not settings.admin_api_key:
        print(
            "WARNING: ADMIN_API_KEY is unset — admin routes will return 503. "
            "Set it in .env before deploying."
        )
    # A process that dies mid-run (crash, deploy restart, OOM) leaves its
    # ScrapeRun/MediaRun row stuck in an in-progress status forever -- no
    # thread in THIS process holds the lock for it, so it can never finish,
    # be paused, or be superseded. Close those rows out before anything else
    # starts, or every future trigger/pause/schedule silently 409s against a
    # run that's been dead since the last restart.
    n1 = reconcile_orphaned_runs()
    n2 = reconcile_orphaned_media_runs()
    if n1 or n2:
        print(f"startup: recovered {n1} orphaned scrape run(s), {n2} orphaned media run(s)")
    scheduler.start()
    try:
        yield
    finally:
        scheduler.stop()


app = FastAPI(
    title=settings.api_title,
    description=DESCRIPTION,
    version="1.0.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.cors_origin_list,
    allow_credentials=True,
    allow_methods=["GET", "POST", "OPTIONS"],
    allow_headers=["*"],
)

app.include_router(meta.router)
app.include_router(projects.router)
app.include_router(catalog.router)
app.include_router(admin.router)
app.include_router(dashboard.router)


@app.get("/", include_in_schema=False)
def root():
    return {
        "name": settings.api_title,
        "docs": "/docs",
        "dashboard": "/admin/dashboard",
        "health": "/health",
        "projects": "/api/projects",
        "stats": "/api/stats",
    }
