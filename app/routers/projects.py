"""Public project endpoints — the main surface the frontend consumes."""

from __future__ import annotations

from typing import Literal

from fastapi import APIRouter, Depends, Query
from sqlalchemy import asc, desc, func, or_, select
from sqlalchemy.orm import Session

from app.db import get_db
from app.models import Project, RecordChange
from app.routers.common import active_filter, get_or_404, page_params, paginate
from app.schemas import ChangeOut, Page, ProjectDetail, ProjectSummary

router = APIRouter(prefix="/api/projects", tags=["projects"])

SORTS = {
    "name": Project.name,
    "price": Project.price_min_aed,
    "price_desc": Project.price_min_aed,
    "handover": Project.handover_year,
    "updated": Project.last_changed_at,
    "sqft": Project.aed_per_sqft_min,
}


@router.get("", response_model=Page[ProjectSummary], summary="List / filter projects")
def list_projects(
    db: Session = Depends(get_db),
    paging: tuple[int, int] = Depends(page_params),
    q: str | None = Query(None, description="substring match on name or area"),
    developer: str | None = Query(None, description="developer slug"),
    area: str | None = Query(None, description="area slug"),
    status: str | None = Query(None, description="on sale | presale | out of stock"),
    construction_status: str | None = None,
    price_min: int | None = Query(None, ge=0, description="min price in AED"),
    price_max: int | None = Query(None, ge=0, description="max price in AED"),
    bedrooms: int | None = Query(None, ge=0, le=20, description="0 = studio"),
    handover_year: int | None = Query(None, ge=2020, le=2040),
    handover_before: int | None = Query(None, ge=2020, le=2040),
    has_price: bool | None = Query(None, description="only projects with a real price"),
    sort: Literal["name", "price", "price_desc", "handover", "updated", "sqft"] = "name",
    include_delisted: bool = False,
):
    page, page_size = paging
    stmt = active_filter(select(Project), Project, include_delisted)

    if q:
        like = f"%{q}%"
        stmt = stmt.where(
            or_(Project.name.ilike(like), Project.area_name.ilike(like),
                Project.developer_name.ilike(like))
        )
    if developer:
        stmt = stmt.where(Project.developer_slug == developer)
    if area:
        stmt = stmt.where(Project.area_slug == area)
    if status:
        stmt = stmt.where(Project.status == status)
    if construction_status:
        stmt = stmt.where(Project.construction_status == construction_status)

    # A project matches a price window if its advertised range overlaps it.
    if price_min is not None:
        stmt = stmt.where(Project.price_max_aed >= price_min)
    if price_max is not None:
        stmt = stmt.where(Project.price_min_aed <= price_max)
    if has_price is True:
        stmt = stmt.where(Project.price_min_aed.isnot(None))
    elif has_price is False:
        stmt = stmt.where(Project.price_min_aed.is_(None))

    if bedrooms is not None:
        stmt = stmt.where(
            Project.bedrooms_min <= bedrooms, Project.bedrooms_max >= bedrooms
        )
    if handover_year is not None:
        stmt = stmt.where(Project.handover_year == handover_year)
    if handover_before is not None:
        stmt = stmt.where(Project.handover_year <= handover_before)

    col = SORTS[sort]
    direction = desc if sort == "price_desc" else asc
    # NULLs last regardless of dialect, so unpriced projects don't head the list
    stmt = stmt.order_by(col.is_(None), direction(col), asc(Project.slug))

    rows, total, pages = paginate(db, stmt, page, page_size)
    return Page[ProjectSummary](
        items=[ProjectSummary.model_validate(r) for r in rows],
        total=total, page=page, page_size=page_size, pages=pages,
    )


@router.get("/{slug}", response_model=ProjectDetail, summary="One project, in full")
def get_project(slug: str, db: Session = Depends(get_db)):
    return ProjectDetail.model_validate(get_or_404(db, Project, slug))


@router.get(
    "/{slug}/changes",
    response_model=list[ChangeOut],
    summary="Change history for one project",
)
def project_changes(
    slug: str,
    db: Session = Depends(get_db),
    field: str | None = Query(None, description='e.g. "price_min_aed"'),
    limit: int = Query(100, ge=1, le=500),
):
    get_or_404(db, Project, slug)  # 404 rather than an empty list for a bad slug
    stmt = (
        select(RecordChange)
        .where(RecordChange.entity == "projects", RecordChange.slug == slug)
        .order_by(desc(RecordChange.changed_at))
        .limit(limit)
    )
    if field:
        stmt = stmt.where(RecordChange.field == field)
    return [ChangeOut.model_validate(c) for c in db.execute(stmt).scalars()]
