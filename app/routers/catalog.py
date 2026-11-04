"""Developers, areas, brokers, articles and static pages."""

from __future__ import annotations

from fastapi import APIRouter, Depends, Query
from sqlalchemy import asc, desc, select
from sqlalchemy.orm import Session

from app.db import get_db
from app.models import Area, Article, Broker, Developer, Page as PageModel, Project
from app.routers.common import active_filter, get_or_404, page_params, paginate
from app.schemas import (
    AreaDetail,
    AreaSummary,
    ArticleDetail,
    ArticleSummary,
    BrokerDetail,
    BrokerSummary,
    DeveloperDetail,
    DeveloperSummary,
    Page,
    PageDetail,
    ProjectSummary,
)

router = APIRouter(prefix="/api", tags=["catalog"])


def _projects_for(db: Session, column, value: str, page: int, page_size: int):
    stmt = (
        select(Project)
        .where(column == value, Project.is_active.is_(True))
        .order_by(Project.price_min_aed.is_(None), asc(Project.price_min_aed))
    )
    rows, total, pages = paginate(db, stmt, page, page_size)
    return Page[ProjectSummary](
        items=[ProjectSummary.model_validate(r) for r in rows],
        total=total, page=page, page_size=page_size, pages=pages,
    )


# ---------------------------------------------------------------------------
# developers
# ---------------------------------------------------------------------------
@router.get("/developers", response_model=Page[DeveloperSummary])
def list_developers(
    db: Session = Depends(get_db),
    paging: tuple[int, int] = Depends(page_params),
    q: str | None = None,
    min_projects: int | None = Query(None, ge=0),
    sort: str = Query("projects", pattern="^(projects|name)$"),
    include_delisted: bool = False,
):
    page, page_size = paging
    stmt = active_filter(select(Developer), Developer, include_delisted)
    if q:
        stmt = stmt.where(Developer.name.ilike(f"%{q}%"))
    if min_projects is not None:
        stmt = stmt.where(Developer.projects_tracked >= min_projects)
    stmt = (
        stmt.order_by(desc(Developer.projects_tracked), asc(Developer.name))
        if sort == "projects"
        else stmt.order_by(asc(Developer.name))
    )
    rows, total, pages = paginate(db, stmt, page, page_size)
    return Page[DeveloperSummary](
        items=[DeveloperSummary.model_validate(r) for r in rows],
        total=total, page=page, page_size=page_size, pages=pages,
    )


@router.get("/developers/{slug}", response_model=DeveloperDetail)
def get_developer(slug: str, db: Session = Depends(get_db)):
    return DeveloperDetail.model_validate(get_or_404(db, Developer, slug))


@router.get("/developers/{slug}/projects", response_model=Page[ProjectSummary])
def developer_projects(
    slug: str,
    db: Session = Depends(get_db),
    paging: tuple[int, int] = Depends(page_params),
):
    get_or_404(db, Developer, slug)
    return _projects_for(db, Project.developer_slug, slug, *paging)


# ---------------------------------------------------------------------------
# areas
# ---------------------------------------------------------------------------
@router.get("/areas", response_model=Page[AreaSummary])
def list_areas(
    db: Session = Depends(get_db),
    paging: tuple[int, int] = Depends(page_params),
    q: str | None = None,
    min_projects: int | None = Query(None, ge=0),
    sort: str = Query("projects", pattern="^(projects|name)$"),
    include_delisted: bool = False,
):
    page, page_size = paging
    stmt = active_filter(select(Area), Area, include_delisted)
    if q:
        stmt = stmt.where(Area.name.ilike(f"%{q}%"))
    if min_projects is not None:
        stmt = stmt.where(Area.projects_tracked >= min_projects)
    stmt = (
        stmt.order_by(desc(Area.projects_tracked), asc(Area.name))
        if sort == "projects"
        else stmt.order_by(asc(Area.name))
    )
    rows, total, pages = paginate(db, stmt, page, page_size)
    return Page[AreaSummary](
        items=[AreaSummary.model_validate(r) for r in rows],
        total=total, page=page, page_size=page_size, pages=pages,
    )


@router.get("/areas/{slug}", response_model=AreaDetail)
def get_area(slug: str, db: Session = Depends(get_db)):
    return AreaDetail.model_validate(get_or_404(db, Area, slug))


@router.get("/areas/{slug}/projects", response_model=Page[ProjectSummary])
def area_projects(
    slug: str,
    db: Session = Depends(get_db),
    paging: tuple[int, int] = Depends(page_params),
):
    get_or_404(db, Area, slug)
    return _projects_for(db, Project.area_slug, slug, *paging)


# ---------------------------------------------------------------------------
# brokers / articles / pages
# ---------------------------------------------------------------------------
@router.get("/brokers", response_model=Page[BrokerSummary])
def list_brokers(
    db: Session = Depends(get_db),
    paging: tuple[int, int] = Depends(page_params),
    include_delisted: bool = False,
):
    page, page_size = paging
    stmt = active_filter(select(Broker), Broker, include_delisted).order_by(
        asc(Broker.name)
    )
    rows, total, pages = paginate(db, stmt, page, page_size)
    return Page[BrokerSummary](
        items=[BrokerSummary.model_validate(r) for r in rows],
        total=total, page=page, page_size=page_size, pages=pages,
    )


@router.get("/brokers/{slug}", response_model=BrokerDetail)
def get_broker(slug: str, db: Session = Depends(get_db)):
    return BrokerDetail.model_validate(get_or_404(db, Broker, slug))


@router.get("/articles", response_model=Page[ArticleSummary])
def list_articles(
    db: Session = Depends(get_db),
    paging: tuple[int, int] = Depends(page_params),
    include_delisted: bool = False,
):
    page, page_size = paging
    stmt = active_filter(select(Article), Article, include_delisted).order_by(
        asc(Article.title)
    )
    rows, total, pages = paginate(db, stmt, page, page_size)
    return Page[ArticleSummary](
        items=[ArticleSummary.model_validate(r) for r in rows],
        total=total, page=page, page_size=page_size, pages=pages,
    )


@router.get("/articles/{slug}", response_model=ArticleDetail)
def get_article(slug: str, db: Session = Depends(get_db)):
    return ArticleDetail.model_validate(get_or_404(db, Article, slug))


@router.get("/pages", response_model=list[PageDetail])
def list_pages(db: Session = Depends(get_db)):
    rows = db.execute(
        select(PageModel).where(PageModel.is_active.is_(True)).order_by(PageModel.slug)
    ).scalars()
    return [PageDetail.model_validate(r) for r in rows]
