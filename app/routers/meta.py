"""Health, aggregate stats, and the cross-dataset change feed."""

from __future__ import annotations

from fastapi import APIRouter, Depends, Query
from sqlalchemy import desc, func, select, text
from sqlalchemy.orm import Session

from app.db import get_db
from app.models import ENTITY_MODELS, Project, RecordChange, ScrapeRun
from app.schemas import ChangeOut, Health, RunOut, Stats

router = APIRouter(tags=["meta"])


def _last_successful(db: Session) -> ScrapeRun | None:
    return db.execute(
        select(ScrapeRun)
        .where(ScrapeRun.status == "success")
        .order_by(desc(ScrapeRun.started_at))
        .limit(1)
    ).scalars().first()


@router.get("/health", response_model=Health, summary="Liveness + DB + last run")
def health(db: Session = Depends(get_db)):
    try:
        db.execute(text("SELECT 1"))
        database = "ok"
    except Exception as exc:  # pragma: no cover - only on a broken DB
        return Health(status="degraded", database=f"error: {exc}")

    active = db.scalar(
        select(func.count()).select_from(Project).where(Project.is_active.is_(True))
    )
    last = _last_successful(db)
    return Health(
        status="ok",
        database=database,
        projects_active=active or 0,
        last_successful_run=RunOut.model_validate(last) if last else None,
    )


@router.get("/api/stats", response_model=Stats, summary="Dataset overview")
def stats(db: Session = Depends(get_db)):
    counts = {
        name: db.scalar(
            select(func.count()).select_from(model).where(model.is_active.is_(True))
        ) or 0
        for name, model in ENTITY_MODELS.items()
    }

    total = counts.get("projects", 0)
    with_price = db.scalar(
        select(func.count()).select_from(Project).where(
            Project.is_active.is_(True), Project.price_min_aed.isnot(None)
        )
    ) or 0

    lo, hi, avg = db.execute(
        select(
            func.min(Project.price_min_aed),
            func.max(Project.price_max_aed),
            func.avg(Project.price_min_aed),
        ).where(Project.is_active.is_(True), Project.price_min_aed.isnot(None))
    ).one()

    by_status = {
        (s or "unknown"): n
        for s, n in db.execute(
            select(Project.status, func.count())
            .where(Project.is_active.is_(True))
            .group_by(Project.status)
            .order_by(desc(func.count()))
        ).all()
    }

    top_devs = [
        {"slug": slug, "name": name, "projects": n}
        for slug, name, n in db.execute(
            select(Project.developer_slug, Project.developer_name, func.count())
            .where(Project.is_active.is_(True), Project.developer_slug.isnot(None))
            .group_by(Project.developer_slug, Project.developer_name)
            .order_by(desc(func.count()))
            .limit(15)
        ).all()
    ]

    top_areas = [
        {"slug": slug, "name": name, "projects": n}
        for slug, name, n in db.execute(
            select(Project.area_slug, Project.area_name, func.count())
            .where(Project.is_active.is_(True), Project.area_slug.isnot(None))
            .group_by(Project.area_slug, Project.area_name)
            .order_by(desc(func.count()))
            .limit(15)
        ).all()
    ]

    years = {
        str(y): n
        for y, n in db.execute(
            select(Project.handover_year, func.count())
            .where(Project.is_active.is_(True), Project.handover_year.isnot(None))
            .group_by(Project.handover_year)
            .order_by(Project.handover_year)
        ).all()
    }

    last = db.execute(
        select(ScrapeRun).order_by(desc(ScrapeRun.started_at)).limit(1)
    ).scalars().first()

    return Stats(
        counts=counts,
        projects_with_price=with_price,
        price_coverage_pct=round(with_price * 100 / total, 2) if total else 0.0,
        price_aed={
            "min": lo,
            "max": hi,
            "avg": round(float(avg)) if avg is not None else None,
        },
        by_status=by_status,
        top_developers=top_devs,
        top_areas=top_areas,
        handover_years=years,
        last_run=RunOut.model_validate(last) if last else None,
    )


@router.get(
    "/api/changes/recent",
    response_model=list[ChangeOut],
    summary="What moved across the whole dataset",
)
def recent_changes(
    db: Session = Depends(get_db),
    entity: str | None = Query(None, description="projects | developers | areas | ..."),
    field: str | None = Query(None, description='e.g. "price_min_aed"'),
    run_id: int | None = None,
    limit: int = Query(100, ge=1, le=500),
):
    stmt = (
        select(RecordChange).order_by(desc(RecordChange.changed_at)).limit(limit)
    )
    if entity:
        stmt = stmt.where(RecordChange.entity == entity)
    if field:
        stmt = stmt.where(RecordChange.field == field)
    if run_id:
        stmt = stmt.where(RecordChange.run_id == run_id)
    return [ChangeOut.model_validate(c) for c in db.execute(stmt).scalars()]
