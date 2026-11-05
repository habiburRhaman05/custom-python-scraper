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

from datetime import datetime, timezone

from fastapi import APIRouter, BackgroundTasks, Depends, HTTPException, Query, Response, status
from sqlalchemy import desc, select
from sqlalchemy.orm import Session

from app.db import get_db
from app.jobs import pause_scrape, release_lock, run_in_background, start_scrape_job
from app.media_jobs import (
    pause_media,
    release_media_lock,
    run_scan,
    run_upload,
    start_media_scan,
    start_media_upload,
)
from app.models import ENTITY_MODELS, MediaRun, ScheduledJob, ScrapeRun
from app.routers.common import require_admin
from app.scheduler import to_utc
from app.schemas import MediaRunOut, RunOut, ScheduledJobOut, ScheduleRequest, ScrapeAccepted

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


@router.post("/scrape/pause", summary="Pause the running scrape (not a reset)")
def pause_scrape_route():
    """
    Stops cleanly after the current in-flight batch and marks the run
    'paused'. Nothing already fetched is discarded. Resuming is just
    triggering a scrape again -- already-fetched pages are fast via the
    HTML cache (or return 304 with revalidation), so it picks up quickly.
    """
    if not pause_scrape():
        raise HTTPException(status.HTTP_409_CONFLICT, "no scrape is currently running")
    return {"message": "pause requested — the run will stop within the current batch"}


# ---------------------------------------------------------------------------
# Media migration — two-phase: scan (read-only) then explicit approval.
# ---------------------------------------------------------------------------
@router.post(
    "/media/scan",
    response_model=MediaRunOut,
    status_code=status.HTTP_202_ACCEPTED,
    summary="Scan for media to migrate (read-only, nothing uploaded)",
)
def media_scan(background: BackgroundTasks, db: Session = Depends(get_db)):
    started, run_id, message = start_media_scan("api")
    if not started:
        raise HTTPException(
            status.HTTP_409_CONFLICT,
            {"message": message, "run_id": run_id,
             "poll": f"/api/admin/media/runs/{run_id}" if run_id else None},
        )
    try:
        background.add_task(run_scan, run_id)
    except Exception:
        release_media_lock()
        raise
    return MediaRunOut.model_validate(db.get(MediaRun, run_id))


@router.post(
    "/media/runs/{run_id}/approve",
    response_model=MediaRunOut,
    status_code=status.HTTP_202_ACCEPTED,
    summary="Approve a scanned run — starts the actual upload",
)
def media_approve(run_id: int, background: BackgroundTasks,
                   db: Session = Depends(get_db),
                   limit: int | None = Query(
                       None, ge=1,
                       description="upload at most N files this run — for a trial "
                                   "batch; re-approving resumes where it left off")):
    run = db.get(MediaRun, run_id)
    if run is None:
        raise HTTPException(status.HTTP_404_NOT_FOUND, f"media run {run_id} not found")

    started, message = start_media_upload(run_id)
    if not started:
        raise HTTPException(status.HTTP_409_CONFLICT, {"message": message,
                                                        "run_id": run_id})
    try:
        background.add_task(run_upload, run_id, limit)
    except Exception:
        release_media_lock()
        raise
    db.refresh(run)
    return MediaRunOut.model_validate(run)


@router.get("/media/runs", response_model=list[MediaRunOut],
             summary="Media migration history")
def media_runs(db: Session = Depends(get_db), limit: int = Query(25, ge=1, le=200)):
    stmt = select(MediaRun).order_by(desc(MediaRun.started_at)).limit(limit)
    return [MediaRunOut.model_validate(r) for r in db.execute(stmt).scalars()]


@router.get("/media/runs/{run_id}", response_model=MediaRunOut,
             summary="One media run — poll this for progress")
def media_run(run_id: int, db: Session = Depends(get_db)):
    run = db.get(MediaRun, run_id)
    if run is None:
        raise HTTPException(status.HTTP_404_NOT_FOUND, f"media run {run_id} not found")
    return MediaRunOut.model_validate(run)


@router.post("/media/pause", summary="Pause the running upload (not a reset)")
def pause_media_route():
    """
    Stops cleanly after the current in-flight batch. Every file already
    uploaded this run is already committed to the ledger, so nothing is
    lost. Resuming is just scanning and approving again -- already-uploaded
    files are skipped.
    """
    if not pause_media():
        raise HTTPException(status.HTTP_409_CONFLICT, "no media upload is currently running")
    return {"message": "pause requested — the upload will stop within the current batch"}


# ---------------------------------------------------------------------------
# Scheduling — one-time future triggers, fired by the background poll loop
# in app/scheduler.py. A scheduled media_migrate runs unattended (scan then
# straight to upload, no approval step, since nobody's there to click it).
# ---------------------------------------------------------------------------
def _schedule(job_type: str, body: ScheduleRequest, db: Session) -> ScheduledJob:
    try:
        local_naive = datetime.strptime(body.scheduled_at, "%Y-%m-%dT%H:%M")
        scheduled_utc = to_utc(local_naive, body.timezone)
    except (ValueError, KeyError) as exc:
        raise HTTPException(
            status.HTTP_422_UNPROCESSABLE_ENTITY,
            f"invalid scheduled_at/timezone: {exc}",
        )
    if scheduled_utc <= datetime.now(timezone.utc):
        raise HTTPException(status.HTTP_422_UNPROCESSABLE_ENTITY,
                            "scheduled_at must be in the future")

    job = ScheduledJob(
        job_type=job_type, scheduled_at_utc=scheduled_utc, timezone_name=body.timezone,
        status="pending",
        params={"only": body.only, "limit": body.limit} if job_type == "scrape" else None,
    )
    db.add(job)
    db.commit()
    return job


@router.post("/scrape/schedule", response_model=ScheduledJobOut, status_code=status.HTTP_201_CREATED,
             summary="Schedule a one-time future scrape")
def schedule_scrape(body: ScheduleRequest, db: Session = Depends(get_db)):
    return ScheduledJobOut.model_validate(_schedule("scrape", body, db))


@router.post("/media/schedule", response_model=ScheduledJobOut, status_code=status.HTTP_201_CREATED,
             summary="Schedule a one-time future media migration (runs unattended)")
def schedule_media(body: ScheduleRequest, db: Session = Depends(get_db)):
    return ScheduledJobOut.model_validate(_schedule("media_migrate", body, db))


@router.get("/scheduled-jobs", response_model=list[ScheduledJobOut], summary="Upcoming + past scheduled jobs")
def list_scheduled_jobs(db: Session = Depends(get_db),
                        job_type: str | None = Query(None, description="scrape | media_migrate"),
                        limit: int = Query(50, ge=1, le=200)):
    stmt = select(ScheduledJob).order_by(desc(ScheduledJob.scheduled_at_utc)).limit(limit)
    if job_type:
        stmt = stmt.where(ScheduledJob.job_type == job_type)
    return [ScheduledJobOut.model_validate(j) for j in db.execute(stmt).scalars()]


@router.post("/scheduled-jobs/{job_id}/cancel", response_model=ScheduledJobOut,
             summary="Cancel a scheduled job before it fires")
def cancel_scheduled_job(job_id: int, db: Session = Depends(get_db)):
    job = db.get(ScheduledJob, job_id)
    if job is None:
        raise HTTPException(status.HTTP_404_NOT_FOUND, f"scheduled job {job_id} not found")
    if job.status != "pending":
        raise HTTPException(status.HTTP_409_CONFLICT, f"job is '{job.status}', not pending")
    job.status = "cancelled"
    db.commit()
    return ScheduledJobOut.model_validate(job)
