"""
Admin routes — the HTTP scrape trigger and run history.

POST /api/admin/scrape is the endpoint you (or cron, or the frontend) hit to
start a scrape. It returns 202 immediately with a run_id rather than holding
the connection open for the ~21 minutes a full run takes; poll
GET /api/admin/runs/{id} for progress.

All routes require the X-API-Key header. An anonymous trigger would let anyone
launch a 2,662-page crawl at will, which also hammers maisondubai.com and risks
the VPS IP being blocked.
"""

from __future__ import annotations

from fastapi import APIRouter, BackgroundTasks, Depends, HTTPException, Query, Response, status
from sqlalchemy import desc, select
from sqlalchemy.orm import Session

from app.db import get_db
from app.jobs import release_lock, run_in_background, start_scrape_job
from app.models import ENTITY_MODELS, ScrapeRun
from app.routers.common import require_admin
from app.schemas import RunOut, ScrapeAccepted

router = APIRouter(
    prefix="/api/admin", tags=["admin"], dependencies=[Depends(require_admin)]
)


@router.post(
    "/scrape",
    response_model=ScrapeAccepted,
    status_code=status.HTTP_202_ACCEPTED,
    summary="Trigger a scrape (returns immediately)",
)
def trigger_scrape(
    background: BackgroundTasks,
    response: Response,
    only: list[str] | None = Query(
        None,
        description="limit to these entity types, e.g. only=projects&only=developers. "
                    "Partial runs never delist.",
    ),
    limit: int | None = Query(
        None, ge=1, description="max pages per entity type — for smoke tests"
    ),
):
    if only:
        unknown = set(only) - set(ENTITY_MODELS) - {"listings"}
        if unknown:
            raise HTTPException(
                status.HTTP_422_UNPROCESSABLE_ENTITY,
                f"unknown entity type(s): {', '.join(sorted(unknown))}",
            )

    started, run_id, message = start_scrape_job("api", only=only, limit=limit)
    if not started:
        # already running — tell the caller which run to watch instead of
        # kicking off a second concurrent crawl
        raise HTTPException(
            status.HTTP_409_CONFLICT,
            {"message": message, "run_id": run_id,
             "poll": f"/api/admin/runs/{run_id}" if run_id else None},
        )

    try:
        background.add_task(run_in_background, run_id, "api", only, limit)
    except Exception:
        release_lock()
        raise

    response.headers["Location"] = f"/api/admin/runs/{run_id}"
    return ScrapeAccepted(
        run_id=run_id,
        status="running",
        message=message,
        poll=f"/api/admin/runs/{run_id}",
    )


@router.get("/runs", response_model=list[RunOut], summary="Run history")
def list_runs(
    db: Session = Depends(get_db),
    status_filter: str | None = Query(
        None, alias="status", description="running | success | failed | aborted"
    ),
    limit: int = Query(25, ge=1, le=200),
):
    stmt = select(ScrapeRun).order_by(desc(ScrapeRun.started_at)).limit(limit)
    if status_filter:
        stmt = stmt.where(ScrapeRun.status == status_filter)
    return [RunOut.model_validate(r) for r in db.execute(stmt).scalars()]


@router.get("/runs/{run_id}", response_model=RunOut, summary="One run — poll this")
def get_run(run_id: int, db: Session = Depends(get_db)):
    run = db.get(ScrapeRun, run_id)
    if run is None:
        raise HTTPException(status.HTTP_404_NOT_FOUND, f"run {run_id} not found")
    return RunOut.model_validate(run)
