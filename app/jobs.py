"""
The scrape + ingest job, with the safety rails that make an unattended weekly
run trustworthy.

The rails exist because the parsers depend on maisondubai.com's HTML. When that
changes, selectors stop matching and the scrape "succeeds" while returning
mostly empty records. Writing that to the database would quietly destroy the
dataset. So before any write we compare the fresh scrape against the last good
run and refuse to proceed if it looks wrong — the run is marked `aborted`, the
reason is stored, and the data is left exactly as it was.

No model is in this path. It is threshold arithmetic over counts.
"""

from __future__ import annotations

import threading
import time
import traceback
from datetime import datetime, timezone
from typing import Any

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from app.config import settings
from app.db import SessionLocal
from app.ingest import ingest_bundle
from app.models import ENTITY_MODELS, Project, ScrapeRun, utcnow
from scraper.maisondubai import run_scrape

# Single-flight: one scrape at a time per process. Paired with the DB check in
# start_scrape_job() so a second trigger can't stack a parallel 2,662-page run.
_run_lock = threading.Lock()

# Set by pause_scrape(); checked by the scraper's inner loop every page.
# Cleared at the start of every new run so a stale pause can't abort the
# next one before it even starts.
_scrape_pause = threading.Event()


def pause_scrape() -> bool:
    """Request the running scrape to stop cleanly. Returns False if none is running."""
    db = SessionLocal()
    try:
        if active_run(db) is None:
            return False
    finally:
        db.close()
    _scrape_pause.set()
    return True


class GuardTripped(Exception):
    """Raised when the fresh scrape fails a sanity check; nothing is written."""

    def __init__(self, checks: list[dict]):
        self.checks = checks
        failed = [c["check"] for c in checks if not c["passed"]]
        super().__init__("safety rails tripped: " + ", ".join(failed))


# ---------------------------------------------------------------------------
# safety rails
# ---------------------------------------------------------------------------
def _price_coverage(projects: list[dict]) -> float:
    """Percent of projects carrying a real numeric price."""
    if not projects:
        return 0.0
    with_price = sum(
        1 for p in projects
        if isinstance(p.get("price_aed"), dict) and p["price_aed"].get("min") is not None
    )
    return round(with_price * 100 / len(projects), 2)


def validate_bundle(db: Session, bundle: dict, *, full_run: bool) -> list[dict]:
    """
    Compare the scrape against current DB state. Returns one dict per check:
    {"check": name, "passed": bool, "detail": str, ...}.
    """
    projects = bundle.get("projects") or []
    checks: list[dict] = []

    live_count = db.scalar(
        select(func.count()).select_from(Project).where(Project.is_active.is_(True))
    ) or 0

    # Rail 1 — did the project count collapse?
    if full_run and live_count:
        ratio = len(projects) / live_count
        checks.append({
            "check": "project_count_ratio",
            "passed": ratio >= settings.guard_min_count_ratio,
            "scraped": len(projects),
            "in_db": live_count,
            "ratio": round(ratio, 3),
            "threshold": settings.guard_min_count_ratio,
            "detail": f"scraped {len(projects)} vs {live_count} active in DB",
        })

    # Rail 2 — did price parsing silently break?
    coverage = _price_coverage(projects)
    if full_run and live_count:
        db_with_price = db.scalar(
            select(func.count()).select_from(Project).where(
                Project.is_active.is_(True), Project.price_min_aed.isnot(None)
            )
        ) or 0
        prev_coverage = round(db_with_price * 100 / live_count, 2)
        drop = prev_coverage - coverage
        checks.append({
            "check": "price_coverage_drop",
            "passed": drop <= settings.guard_max_coverage_drop,
            "coverage_now": coverage,
            "coverage_before": prev_coverage,
            "drop_pct_points": round(drop, 2),
            "threshold": settings.guard_max_coverage_drop,
            "detail": f"price coverage {prev_coverage}% -> {coverage}%",
        })
    else:
        checks.append({
            "check": "price_coverage",
            "passed": True,
            "coverage_now": coverage,
            "detail": f"price coverage {coverage}% (no baseline to compare)",
        })

    # Rail 3 — would this run mass-delist?
    if full_run and live_count:
        scraped_slugs = {p["slug"] for p in projects if p.get("slug")}
        db_slugs = set(
            db.execute(
                select(Project.slug).where(Project.is_active.is_(True))
            ).scalars()
        )
        would_delist = len(db_slugs - scraped_slugs)
        ratio = would_delist / live_count
        checks.append({
            "check": "delist_ratio",
            "passed": ratio <= settings.guard_max_delist_ratio,
            "would_delist": would_delist,
            "in_db": live_count,
            "ratio": round(ratio, 3),
            "threshold": settings.guard_max_delist_ratio,
            "detail": f"{would_delist} of {live_count} active projects missing",
        })

    # Rail 4 — parse errors are always worth surfacing
    parse_errors = sum(
        1 for recs in bundle.values()
        if isinstance(recs, list)
        for r in recs
        if isinstance(r, dict) and r.get("parse_error")
    )
    checks.append({
        "check": "parse_errors",
        "passed": parse_errors == 0,
        "count": parse_errors,
        "detail": f"{parse_errors} record(s) failed to parse",
    })

    return checks


# ---------------------------------------------------------------------------
# the job
# ---------------------------------------------------------------------------
def run_scrape_and_ingest(
    trigger: str = "api",
    *,
    only: list[str] | None = None,
    limit: int | None = None,
    run_id: int | None = None,
) -> dict[str, Any]:
    """
    Scrape, validate, then ingest. Returns a summary dict.

    `only`/`limit` make it a partial run: delisting is disabled and the
    count/delist rails are skipped, so a smoke test can't damage real data.
    """
    full_run = not only and not limit
    db: Session = SessionLocal()
    started = time.time()
    _scrape_pause.clear()  # a stale pause from a prior run must not fire instantly

    try:
        if run_id is None:
            run = ScrapeRun(trigger=trigger, status="running", started_at=utcnow())
            db.add(run)
            db.commit()
        else:
            run = db.get(ScrapeRun, run_id)
            if run is None:
                raise RuntimeError(f"run {run_id} not found")
        rid = run.id

        def _on_progress(p: dict) -> None:
            # Fires on the same thread running this function (ex.map() is
            # consumed synchronously), so reusing `db`/`run` here is safe --
            # nothing else touches this session concurrently.
            run.progress = p
            db.commit()

        bundle = run_scrape(
            out=settings.scrape_output_dir,
            only=only,
            limit=limit,
            workers=settings.scrape_workers,
            delay=settings.scrape_delay,
            cache_dir=settings.scrape_cache_dir,
            revalidate=settings.scrape_revalidate,
            write_json=True,
            on_progress=_on_progress,
            should_pause=_scrape_pause.is_set,
        )
        run.progress = None  # scraping finished; nothing left to poll for

        if (bundle.get("meta") or {}).get("paused"):
            # Nothing is ingested for a paused run -- the partial bundle may
            # be missing whole entity types, and ingesting it with
            # delist_missing could wipe out everything not yet re-fetched.
            # Resuming is just running again; already-fetched pages are
            # fast via the HTML cache, so nothing here needs to be resumable
            # at a finer grain than "run it again."
            run.status = "paused"
            run.finished_at = utcnow()
            run.duration_seconds = round(time.time() - started, 1)
            db.commit()
            return {"run_id": rid, "status": "paused",
                    "duration_seconds": run.duration_seconds}

        http = (bundle.get("meta") or {}).get("http") or {}
        run.pages_fetched = http.get("net", 0)
        run.pages_not_modified = http.get("not_modified", 0)
        run.pages_from_cache = http.get("cache", 0)
        run.pages_failed = http.get("fail", 0)

        checks = validate_bundle(db, bundle, full_run=full_run)
        run.validation = {"full_run": full_run, "checks": checks}

        failed = [c for c in checks if not c["passed"]]
        if failed and settings.guard_enabled:
            run.status = "aborted"
            run.error = "; ".join(c["detail"] for c in failed)
            run.finished_at = utcnow()
            run.duration_seconds = round(time.time() - started, 1)
            db.commit()
            return {
                "run_id": rid,
                "status": "aborted",
                "reason": run.error,
                "validation": run.validation,
            }

        counts = ingest_bundle(db, bundle, rid, delist_missing=full_run)

        run.counts = counts
        run.status = "success"
        run.finished_at = utcnow()
        run.duration_seconds = round(time.time() - started, 1)
        db.commit()

        return {
            "run_id": rid,
            "status": "success",
            "duration_seconds": run.duration_seconds,
            "counts": counts,
            "validation": run.validation,
        }

    except Exception as exc:
        db.rollback()
        try:
            run = db.get(ScrapeRun, run_id) if run_id else None
            if run is None:
                run = ScrapeRun(trigger=trigger, started_at=utcnow())
                db.add(run)
            run.status = "failed"
            run.error = f"{type(exc).__name__}: {exc}\n{traceback.format_exc()[-2000:]}"
            run.finished_at = utcnow()
            run.duration_seconds = round(time.time() - started, 1)
            db.commit()
            rid = run.id
        except Exception:
            db.rollback()
            rid = run_id
        return {"run_id": rid, "status": "failed", "error": str(exc)}
    finally:
        db.close()


# ---------------------------------------------------------------------------
# trigger plumbing used by the API
# ---------------------------------------------------------------------------
def active_run(db: Session) -> ScrapeRun | None:
    return db.execute(
        select(ScrapeRun)
        .where(ScrapeRun.status == "running")
        .order_by(ScrapeRun.started_at.desc())
    ).scalars().first()


def start_scrape_job(
    trigger: str = "api",
    *,
    only: list[str] | None = None,
    limit: int | None = None,
) -> tuple[bool, int | None, str]:
    """
    Claim the lock and open a run row. Returns (started, run_id, message).

    The caller is responsible for actually running the work in the background
    (see routers/admin.py) and must call release_lock() when it finishes.
    """
    if not _run_lock.acquire(blocking=False):
        db = SessionLocal()
        try:
            cur = active_run(db)
            return False, cur.id if cur else None, "a scrape is already running"
        finally:
            db.close()

    db = SessionLocal()
    try:
        cur = active_run(db)
        if cur is not None:
            _run_lock.release()
            return False, cur.id, "a scrape is already running"
        run = ScrapeRun(trigger=trigger, status="running", started_at=utcnow())
        db.add(run)
        db.commit()
        return True, run.id, "scrape started"
    except Exception:
        _run_lock.release()
        raise
    finally:
        db.close()


def release_lock() -> None:
    try:
        _run_lock.release()
    except RuntimeError:
        pass  # not held — nothing to do


def run_in_background(
    run_id: int, trigger: str, only: list[str] | None, limit: int | None
) -> None:
    """Target for BackgroundTasks; always releases the lock."""
    try:
        run_scrape_and_ingest(trigger, only=only, limit=limit, run_id=run_id)
    finally:
        release_lock()
