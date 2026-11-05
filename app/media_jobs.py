"""
Two-phase media migration job, driven from the dashboard.

Phase 1 (scan)     -- read-only. Counts what would change and stores the
                      result on a MediaRun row with status='awaiting_approval'.
                      Nothing is uploaded and nothing is written to content
                      tables.
Phase 2 (approve)  -- only runs when a human explicitly approves that run.
                      Downloads each file, uploads it to GoHighLevel, then
                      rewrites the matching URLs across every table.

Split this way so a multi-hour, ~38k-file operation against a live GHL
account can never start by accident.
"""

from __future__ import annotations

import threading
import time
import traceback
from typing import Any

from sqlalchemy import select
from sqlalchemy.orm import Session

from app.config import settings
from app.db import SessionLocal
from app.media_migration import (
    build_client,
    pending_urls,
    replace_everywhere,
    scan,
    upload_pending,
)
from app.models import MediaRun, utcnow

# Single-flight, same reasoning as the scrape lock: one long job at a time.
_media_lock = threading.Lock()

ACTIVE_STATUSES = ("scanning", "uploading")


def active_media_run(db: Session) -> MediaRun | None:
    return db.execute(
        select(MediaRun)
        .where(MediaRun.status.in_(ACTIVE_STATUSES))
        .order_by(MediaRun.started_at.desc())
    ).scalars().first()


def start_media_scan(trigger: str = "api") -> tuple[bool, int | None, str]:
    """Claim the lock and open a MediaRun in 'scanning'."""
    if not _media_lock.acquire(blocking=False):
        db = SessionLocal()
        try:
            cur = active_media_run(db)
            return False, cur.id if cur else None, "a media job is already running"
        finally:
            db.close()

    db = SessionLocal()
    try:
        cur = active_media_run(db)
        if cur is not None:
            _media_lock.release()
            return False, cur.id, "a media job is already running"
        run = MediaRun(trigger=trigger, status="scanning", started_at=utcnow())
        db.add(run)
        db.commit()
        return True, run.id, "scan started"
    except Exception:
        _media_lock.release()
        raise
    finally:
        db.close()


def release_media_lock() -> None:
    try:
        _media_lock.release()
    except RuntimeError:
        pass


def run_scan(run_id: int) -> None:
    """Phase 1. Read-only; fills in the preview the user approves."""
    db = SessionLocal()
    try:
        run = db.get(MediaRun, run_id)
        if run is None:
            return
        result = scan(db)
        pending, already = pending_urls(db, result.unique_urls)

        run.scan_total_urls = len(result.unique_urls)
        run.scan_occurrences = result.occurrences
        run.scan_by_table = result.by_table
        run.scan_already_migrated = already
        run.scan_pending = len(pending)
        run.scan_samples = result.samples()
        run.progress = None
        run.status = "awaiting_approval"
        db.commit()
    except Exception as exc:
        db.rollback()
        run = db.get(MediaRun, run_id)
        if run is not None:
            run.status = "failed"
            run.error = f"{type(exc).__name__}: {exc}\n{traceback.format_exc()[-1500:]}"
            run.finished_at = utcnow()
            db.commit()
    finally:
        db.close()
        release_media_lock()


def start_media_upload(run_id: int) -> tuple[bool, str]:
    """Approve a scanned run and claim the lock for the upload phase."""
    db = SessionLocal()
    try:
        run = db.get(MediaRun, run_id)
        if run is None:
            return False, "run not found"
        if run.status != "awaiting_approval":
            return False, f"run is '{run.status}', not awaiting approval"
    finally:
        db.close()

    if not _media_lock.acquire(blocking=False):
        return False, "a media job is already running"

    db = SessionLocal()
    try:
        run = db.get(MediaRun, run_id)
        run.status = "uploading"
        run.approved_at = utcnow()
        db.commit()
        return True, "upload started"
    except Exception:
        _media_lock.release()
        raise
    finally:
        db.close()


def run_upload(run_id: int, limit: int | None = None) -> None:
    """
    Phase 2. Downloads, uploads to GHL, then rewrites URLs.

    `limit` caps how many files this run uploads -- useful for a small trial
    batch before committing to the full set. Re-running picks up where it
    left off, since completed uploads are skipped via the ledger.
    """
    db = SessionLocal()
    started = time.time()
    try:
        run = db.get(MediaRun, run_id)
        if run is None:
            return

        result = scan(db)  # re-scan: data may have changed since approval
        client = build_client()

        def _upload_progress(done: int, total: int, uploaded: int, failed: int) -> None:
            run.progress = {
                "phase": "uploading", "done": done, "total": total,
                "uploaded": uploaded, "failed": failed,
            }
            db.commit()

        counts = upload_pending(db, result.unique_urls, client, limit=limit,
                                 on_progress=_upload_progress,
                                 workers=settings.ghl_workers,
                                 names=result.names)

        def _replace_progress(done: int, total: int) -> None:
            run.progress = {"phase": "replacing", "done": done, "total": total}
            db.commit()

        replaced = replace_everywhere(db, on_progress=_replace_progress)

        run.uploaded = counts["uploaded"]
        run.failed = counts["failed"]
        run.rows_updated = replaced["rows_updated"]
        run.fields_updated = replaced["fields_updated"]
        run.progress = None
        run.status = "success"
        run.finished_at = utcnow()
        run.duration_seconds = round(time.time() - started, 1)
        db.commit()
    except Exception as exc:
        db.rollback()
        run = db.get(MediaRun, run_id)
        if run is not None:
            run.status = "failed"
            run.error = f"{type(exc).__name__}: {exc}\n{traceback.format_exc()[-1500:]}"
            run.finished_at = utcnow()
            run.duration_seconds = round(time.time() - started, 1)
            db.commit()
    finally:
        db.close()
        release_media_lock()
