"""
One-time scheduled triggers for 'scrape' and 'media_migrate', created from
the dashboard's Schedule button.

A background thread polls for jobs whose scheduled_at_utc has arrived and
fires them. Each firing runs in its own thread so a multi-hour media
migration doesn't block the poll loop from noticing other due jobs.

Firing a 'scrape' job reuses the exact same start_scrape_job/run_in_background
path the manual Trigger button uses. Firing a 'media_migrate' job is
different: there's no human present to click Approve at 3am, so a scheduled
media run scans and then immediately proceeds to upload, unattended.
"""

from __future__ import annotations

import threading
import time
import traceback
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

from sqlalchemy import select
from sqlalchemy.orm import Session

from app.db import SessionLocal
from app.jobs import run_in_background, start_scrape_job
from app.media_jobs import run_scan, run_upload, start_media_scan, start_media_upload
from app.models import ScheduledJob

POLL_SECONDS = 20
_stop_event = threading.Event()
_thread: threading.Thread | None = None


def to_utc(local_naive: datetime, timezone_name: str) -> datetime:
    """A naive local datetime + IANA zone name -> an aware UTC datetime."""
    aware_local = local_naive.replace(tzinfo=ZoneInfo(timezone_name))
    return aware_local.astimezone(timezone.utc)


def _due_jobs(db: Session) -> list[ScheduledJob]:
    now = datetime.now(timezone.utc)
    return list(db.execute(
        select(ScheduledJob)
        .where(ScheduledJob.status == "pending", ScheduledJob.scheduled_at_utc <= now)
    ).scalars())


def _fire_scrape(job_id: int) -> None:
    db = SessionLocal()
    try:
        job = db.get(ScheduledJob, job_id)
        if job is None:
            return
        params = job.params or {}
        started, run_id, message = start_scrape_job(
            "schedule", only=params.get("only"), limit=params.get("limit")
        )
        if not started:
            # Another job is holding the lock -- leave 'pending' and let the
            # next poll tick try again rather than dropping the schedule.
            return
        job.status = "fired"
        job.fired_at = datetime.now(timezone.utc)
        job.result_run_id = run_id
        db.commit()
    finally:
        db.close()
    run_in_background(run_id, "schedule", params.get("only"), params.get("limit"))


def _fire_media_migrate(job_id: int) -> None:
    db = SessionLocal()
    try:
        job = db.get(ScheduledJob, job_id)
        if job is None:
            return
        started, run_id, message = start_media_scan("schedule")
        if not started:
            return  # lock held elsewhere; retry next poll tick
        job.status = "fired"
        job.fired_at = datetime.now(timezone.utc)
        job.result_run_id = run_id
        db.commit()
    finally:
        db.close()

    # Scan first (read-only), then -- unlike the dashboard button -- proceed
    # straight to upload with no human approval step, since this is meant to
    # run unattended.
    run_scan(run_id)
    approved, _ = start_media_upload(run_id)
    if approved:
        run_upload(run_id)


_FIRE = {"scrape": _fire_scrape, "media_migrate": _fire_media_migrate}


def _poll_loop() -> None:
    while not _stop_event.is_set():
        try:
            db = SessionLocal()
            try:
                due = _due_jobs(db)
            finally:
                db.close()

            for job in due:
                fire = _FIRE.get(job.job_type)
                if fire is None:
                    continue
                threading.Thread(target=fire, args=(job.id,), daemon=True).start()
        except Exception:
            print("scheduler poll error:\n" + traceback.format_exc())

        _stop_event.wait(POLL_SECONDS)


def start() -> None:
    """Call once at app startup (see main.py lifespan)."""
    global _thread
    if _thread is not None:
        return
    _stop_event.clear()
    _thread = threading.Thread(target=_poll_loop, daemon=True)
    _thread.start()


def stop() -> None:
    _stop_event.set()
