"""Pydantic response models. Lists return summaries; detail routes return everything."""

from __future__ import annotations

from datetime import datetime
from typing import Any, Generic, TypeVar

from pydantic import BaseModel, ConfigDict

T = TypeVar("T")


class ORMModel(BaseModel):
    model_config = ConfigDict(from_attributes=True)


class Page(BaseModel, Generic[T]):
    items: list[T]
    total: int
    page: int
    page_size: int
    pages: int


class Lifecycle(ORMModel):
    first_seen_at: datetime
    last_seen_at: datetime
    last_changed_at: datetime
    is_active: bool
    delisted_at: datetime | None = None


# ---------------------------------------------------------------------------
# projects
# ---------------------------------------------------------------------------
class ProjectSummary(ORMModel):
    slug: str
    name: str
    url: str
    status: str | None = None
    developer_slug: str | None = None
    developer_name: str | None = None
    area_slug: str | None = None
    area_name: str | None = None
    price_min_aed: int | None = None
    price_max_aed: int | None = None
    price_raw: str | None = None
    price_on_request: bool = False
    bedrooms_raw: str | None = None
    bedrooms_min: int | None = None
    bedrooms_max: int | None = None
    handover: str | None = None
    handover_year: int | None = None
    size_min_sqft: float | None = None
    size_max_sqft: float | None = None
    aed_per_sqft_min: float | None = None
    aed_per_sqft_max: float | None = None
    units_total: int | None = None
    construction_status: str | None = None
    readiness_percent: float | None = None
    is_active: bool = True
    last_changed_at: datetime | None = None
    # first gallery image -- populated from Project.thumbnail_url (see models.py)
    thumbnail_url: str | None = None


class ProjectDetail(ProjectSummary, Lifecycle):
    handover_quarter: int | None = None
    inventory_raw: str | None = None
    service_charge: str | None = None
    furnishing: str | None = None
    post_handover_plan: str | None = None
    brochure_url: str | None = None
    floor_plan_url: str | None = None
    price_usd_approx: str | None = None
    unit_types: list[Any] | None = None
    payment_plans: list[Any] | None = None
    amenities: list[Any] | None = None
    gallery: list[Any] | None = None
    faqs: list[Any] | None = None
    sections: list[Any] | None = None
    all_facts: dict[str, Any] | None = None
    address: dict[str, Any] | None = None
    meta: dict[str, Any] | None = None


# ---------------------------------------------------------------------------
# developers / areas
# ---------------------------------------------------------------------------
class SnapshotFields(ORMModel):
    projects_tracked: int | None = None
    price_from_aed: int | None = None
    median_per_sqft_aed: int | None = None
    snapshot_bedrooms_raw: str | None = None
    snapshot_handover_raw: str | None = None
    snapshot_summary: str | None = None


class DeveloperSummary(SnapshotFields):
    slug: str
    name: str
    url: str
    website: str | None = None
    logo_url: str | None = None
    is_active: bool = True


class DeveloperDetail(DeveloperSummary, Lifecycle):
    about: str | None = None
    offices: list[Any] | None = None
    areas_active_in: list[Any] | None = None
    sections: list[Any] | None = None
    faqs: list[Any] | None = None
    meta: dict[str, Any] | None = None


class AreaSummary(SnapshotFields):
    slug: str
    name: str
    url: str
    intro: str | None = None
    thumbnail_url: str | None = None
    is_active: bool = True


class AreaDetail(AreaSummary, Lifecycle):
    developers_active: list[Any] | None = None
    sections: list[Any] | None = None
    faqs: list[Any] | None = None
    gallery: list[Any] | None = None
    meta: dict[str, Any] | None = None


# ---------------------------------------------------------------------------
# brokers / articles / pages
# ---------------------------------------------------------------------------
class BrokerSummary(ORMModel):
    slug: str
    name: str
    url: str
    role: str | None = None
    email: str | None = None
    phone: str | None = None
    photo_url: str | None = None
    is_active: bool = True


class BrokerDetail(BrokerSummary, Lifecycle):
    bio: str | None = None
    emails: list[Any] | None = None
    phones: list[Any] | None = None
    socials: list[Any] | None = None
    languages: list[Any] | None = None
    meta: dict[str, Any] | None = None


class ArticleSummary(ORMModel):
    slug: str
    title: str
    url: str
    published: str | None = None
    author: str | None = None
    is_active: bool = True


class ArticleDetail(ArticleSummary, Lifecycle):
    body: str | None = None
    sections: list[Any] | None = None
    faqs: list[Any] | None = None
    meta: dict[str, Any] | None = None


class PageDetail(ORMModel):
    slug: str
    url: str
    title: str | None = None
    sections: list[Any] | None = None
    faqs: list[Any] | None = None
    meta: dict[str, Any] | None = None
    is_active: bool = True


# ---------------------------------------------------------------------------
# runs / changes / meta
# ---------------------------------------------------------------------------
class ChangeOut(ORMModel):
    entity: str
    slug: str
    field: str
    old_value: str | None = None
    new_value: str | None = None
    changed_at: datetime
    run_id: int


class RunOut(ORMModel):
    id: int
    trigger: str
    status: str
    started_at: datetime
    finished_at: datetime | None = None
    duration_seconds: float | None = None
    pages_fetched: int = 0
    pages_not_modified: int = 0
    pages_from_cache: int = 0
    pages_failed: int = 0
    progress: dict[str, Any] | None = None
    counts: dict[str, Any] | None = None
    validation: dict[str, Any] | None = None
    error: str | None = None


class ScrapeAccepted(BaseModel):
    run_id: int
    status: str
    message: str
    poll: str


class MediaRunOut(ORMModel):
    id: int
    trigger: str
    status: str
    started_at: datetime
    approved_at: datetime | None = None
    finished_at: datetime | None = None
    duration_seconds: float | None = None
    scan_total_urls: int = 0
    scan_occurrences: int = 0
    scan_already_migrated: int = 0
    scan_pending: int = 0
    scan_by_table: dict[str, Any] | None = None
    scan_samples: list[Any] | None = None
    uploaded: int = 0
    failed: int = 0
    rows_updated: int = 0
    fields_updated: int = 0
    progress: dict[str, Any] | None = None
    error: str | None = None


class Health(BaseModel):
    status: str
    database: str
    projects_active: int | None = None
    last_successful_run: RunOut | None = None


class Stats(BaseModel):
    counts: dict[str, int]
    projects_with_price: int
    price_coverage_pct: float
    price_aed: dict[str, Any]
    by_status: dict[str, int]
    top_developers: list[dict[str, Any]]
    top_areas: list[dict[str, Any]]
    handover_years: dict[str, int]
    last_run: RunOut | None = None
