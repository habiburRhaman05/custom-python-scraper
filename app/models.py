"""
SQLAlchemy models.

Every content table carries the same five lifecycle columns so the ingest can
treat them uniformly:

    content_hash     sha256 of the canonical scraped record
    first_seen_at    when this slug was first seen
    last_seen_at     touched on every run the slug appears in
    last_changed_at  bumped only when content_hash actually changes
    is_active        False once the slug disappears from the site
    delisted_at      when that happened

Records are never hard-deleted: a project that goes out of stock and later
returns keeps its history, and a partial scrape can't wipe rows.
"""

from __future__ import annotations

from datetime import datetime, timezone

from sqlalchemy import (
    JSON,
    Boolean,
    DateTime,
    Float,
    ForeignKey,
    Index,
    Integer,
    String,
    Text,
    func,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.config import settings
from app.db import Base

# JSONB on Postgres (indexable, binary), plain JSON on SQLite for local dev.
JsonCol = JSONB if not settings.is_sqlite else JSON


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


class LifecycleMixin:
    """Shared change-tracking columns."""

    content_hash: Mapped[str] = mapped_column(String(64), index=True)
    first_seen_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=utcnow
    )
    last_seen_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=utcnow
    )
    last_changed_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=utcnow
    )
    is_active: Mapped[bool] = mapped_column(Boolean, default=True, index=True)
    delisted_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )


# ---------------------------------------------------------------------------
# Projects — the main dataset (~2,021 off-plan developments)
# ---------------------------------------------------------------------------
class Project(LifecycleMixin, Base):
    __tablename__ = "projects"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    slug: Mapped[str] = mapped_column(String(255), unique=True, index=True)
    url: Mapped[str] = mapped_column(Text)
    name: Mapped[str] = mapped_column(String(500), index=True)
    status: Mapped[str | None] = mapped_column(String(50), index=True)

    developer_slug: Mapped[str | None] = mapped_column(String(255), index=True)
    developer_name: Mapped[str | None] = mapped_column(String(500))
    area_slug: Mapped[str | None] = mapped_column(String(255), index=True)
    area_name: Mapped[str | None] = mapped_column(String(500))

    # price. Upstream (Reelly) uses a literal 1 as its "on request" placeholder,
    # so price_on_request is authoritative when the numbers are NULL.
    price_min_aed: Mapped[int | None] = mapped_column(Integer, index=True)
    price_max_aed: Mapped[int | None] = mapped_column(Integer, index=True)
    price_raw: Mapped[str | None] = mapped_column(String(255))
    price_on_request: Mapped[bool] = mapped_column(Boolean, default=False, index=True)
    price_usd_approx: Mapped[str | None] = mapped_column(String(255))

    # parsed out of "Studio, 1, 2, 3" so bedroom filtering is a real query
    bedrooms_raw: Mapped[str | None] = mapped_column(String(255))
    bedrooms_min: Mapped[int | None] = mapped_column(Integer, index=True)
    bedrooms_max: Mapped[int | None] = mapped_column(Integer, index=True)

    # parsed out of "Q4 2027"
    handover: Mapped[str | None] = mapped_column(String(100))
    handover_year: Mapped[int | None] = mapped_column(Integer, index=True)
    handover_quarter: Mapped[int | None] = mapped_column(Integer)

    size_min_sqft: Mapped[float | None] = mapped_column(Float)
    size_max_sqft: Mapped[float | None] = mapped_column(Float)
    aed_per_sqft_min: Mapped[float | None] = mapped_column(Float, index=True)
    aed_per_sqft_max: Mapped[float | None] = mapped_column(Float)

    units_total: Mapped[int | None] = mapped_column(Integer)
    inventory_raw: Mapped[str | None] = mapped_column(String(255))
    construction_status: Mapped[str | None] = mapped_column(String(100), index=True)
    readiness_percent: Mapped[float | None] = mapped_column(Float)
    service_charge: Mapped[str | None] = mapped_column(String(255))
    furnishing: Mapped[str | None] = mapped_column(Text)
    post_handover_plan: Mapped[str | None] = mapped_column(String(100))
    brochure_url: Mapped[str | None] = mapped_column(Text)
    floor_plan_url: Mapped[str | None] = mapped_column(Text)

    # nested payloads
    unit_types: Mapped[list | None] = mapped_column(JsonCol)
    payment_plans: Mapped[list | None] = mapped_column(JsonCol)
    amenities: Mapped[list | None] = mapped_column(JsonCol)
    gallery: Mapped[list | None] = mapped_column(JsonCol)
    faqs: Mapped[list | None] = mapped_column(JsonCol)
    sections: Mapped[list | None] = mapped_column(JsonCol)
    all_facts: Mapped[dict | None] = mapped_column(JsonCol)
    address: Mapped[dict | None] = mapped_column(JsonCol)
    meta: Mapped[dict | None] = mapped_column(JsonCol)
    json_ld: Mapped[list | None] = mapped_column(JsonCol)

    @property
    def thumbnail_url(self) -> str | None:
        """First gallery image. Picked up by ProjectSummary via from_attributes."""
        if isinstance(self.gallery, list) and self.gallery:
            first = self.gallery[0]
            if isinstance(first, dict):
                return first.get("url")
        og = (self.meta or {}).get("og_image") if isinstance(self.meta, dict) else None
        return og

    __table_args__ = (
        Index("ix_projects_price_range", "price_min_aed", "price_max_aed"),
        Index("ix_projects_area_price", "area_slug", "price_min_aed"),
        Index("ix_projects_active_status", "is_active", "status"),
    )


# ---------------------------------------------------------------------------
# Developers / areas — both carry the same "market snapshot" strip
# ---------------------------------------------------------------------------
class SnapshotMixin:
    projects_tracked: Mapped[int | None] = mapped_column(Integer)
    price_from_aed: Mapped[int | None] = mapped_column(Integer)
    median_per_sqft_aed: Mapped[int | None] = mapped_column(Integer)
    snapshot_bedrooms_raw: Mapped[str | None] = mapped_column(String(255))
    snapshot_handover_raw: Mapped[str | None] = mapped_column(String(255))
    snapshot_summary: Mapped[str | None] = mapped_column(Text)


class Developer(LifecycleMixin, SnapshotMixin, Base):
    __tablename__ = "developers"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    slug: Mapped[str] = mapped_column(String(255), unique=True, index=True)
    url: Mapped[str] = mapped_column(Text)
    name: Mapped[str] = mapped_column(String(500), index=True)
    website: Mapped[str | None] = mapped_column(Text)
    about: Mapped[str | None] = mapped_column(Text)
    logo_url: Mapped[str | None] = mapped_column(Text)

    offices: Mapped[list | None] = mapped_column(JsonCol)
    areas_active_in: Mapped[list | None] = mapped_column(JsonCol)
    sections: Mapped[list | None] = mapped_column(JsonCol)
    faqs: Mapped[list | None] = mapped_column(JsonCol)
    meta: Mapped[dict | None] = mapped_column(JsonCol)


class Area(LifecycleMixin, SnapshotMixin, Base):
    __tablename__ = "areas"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    slug: Mapped[str] = mapped_column(String(255), unique=True, index=True)
    url: Mapped[str] = mapped_column(Text)
    name: Mapped[str] = mapped_column(String(500), index=True)
    intro: Mapped[str | None] = mapped_column(Text)

    developers_active: Mapped[list | None] = mapped_column(JsonCol)
    sections: Mapped[list | None] = mapped_column(JsonCol)
    faqs: Mapped[list | None] = mapped_column(JsonCol)
    gallery: Mapped[list | None] = mapped_column(JsonCol)
    meta: Mapped[dict | None] = mapped_column(JsonCol)

    @property
    def thumbnail_url(self) -> str | None:
        """First gallery image, same derivation as Project.thumbnail_url."""
        if isinstance(self.gallery, list) and self.gallery:
            first = self.gallery[0]
            if isinstance(first, dict):
                return first.get("url")
        return None


class Broker(LifecycleMixin, Base):
    __tablename__ = "brokers"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    slug: Mapped[str] = mapped_column(String(255), unique=True, index=True)
    url: Mapped[str] = mapped_column(Text)
    name: Mapped[str] = mapped_column(String(500), index=True)
    role: Mapped[str | None] = mapped_column(String(255))
    email: Mapped[str | None] = mapped_column(String(255))
    phone: Mapped[str | None] = mapped_column(String(100))
    bio: Mapped[str | None] = mapped_column(Text)
    photo_url: Mapped[str | None] = mapped_column(Text)

    emails: Mapped[list | None] = mapped_column(JsonCol)
    phones: Mapped[list | None] = mapped_column(JsonCol)
    socials: Mapped[list | None] = mapped_column(JsonCol)
    languages: Mapped[list | None] = mapped_column(JsonCol)
    meta: Mapped[dict | None] = mapped_column(JsonCol)


class Article(LifecycleMixin, Base):
    """The /learn guides."""

    __tablename__ = "articles"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    slug: Mapped[str] = mapped_column(String(255), unique=True, index=True)
    url: Mapped[str] = mapped_column(Text)
    title: Mapped[str] = mapped_column(String(500), index=True)
    body: Mapped[str | None] = mapped_column(Text)
    published: Mapped[str | None] = mapped_column(String(100))
    author: Mapped[str | None] = mapped_column(String(255))

    sections: Mapped[list | None] = mapped_column(JsonCol)
    faqs: Mapped[list | None] = mapped_column(JsonCol)
    meta: Mapped[dict | None] = mapped_column(JsonCol)


class Page(LifecycleMixin, Base):
    """Static marketing pages, keyed by path rather than slug."""

    __tablename__ = "pages"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    slug: Mapped[str] = mapped_column(String(255), unique=True, index=True)  # the path
    url: Mapped[str] = mapped_column(Text)
    title: Mapped[str | None] = mapped_column(String(500))

    sections: Mapped[list | None] = mapped_column(JsonCol)
    faqs: Mapped[list | None] = mapped_column(JsonCol)
    meta: Mapped[dict | None] = mapped_column(JsonCol)


# ---------------------------------------------------------------------------
# Run bookkeeping
# ---------------------------------------------------------------------------
class ScrapeRun(Base):
    __tablename__ = "scrape_runs"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    trigger: Mapped[str] = mapped_column(String(20))  # api | cli | cron
    status: Mapped[str] = mapped_column(
        String(20), index=True
    )  # running | success | failed | aborted

    started_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=utcnow, index=True
    )
    finished_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    duration_seconds: Mapped[float | None] = mapped_column(Float)

    pages_fetched: Mapped[int] = mapped_column(Integer, default=0)
    pages_not_modified: Mapped[int] = mapped_column(Integer, default=0)
    pages_from_cache: Mapped[int] = mapped_column(Integer, default=0)
    pages_failed: Mapped[int] = mapped_column(Integer, default=0)

    # Live progress while status='running': {"stage": "projects", "stage_done": 1245,
    # "stage_total": 2021, "overall_done": 1245, "overall_total": 2662}. Updated
    # every ~25 pages by the scraper's on_progress callback -- this is what lets
    # a polling dashboard show real numbers instead of just elapsed time.
    progress: Mapped[dict | None] = mapped_column(JsonCol)

    # {"projects": {"created": 3, "updated": 1, "unchanged": 2017, "delisted": 0}, ...}
    counts: Mapped[dict | None] = mapped_column(JsonCol)
    # safety-rail results — populated whether or not they tripped
    validation: Mapped[dict | None] = mapped_column(JsonCol)
    error: Mapped[str | None] = mapped_column(Text)

    changes: Mapped[list[RecordChange]] = relationship(
        back_populates="run", cascade="all, delete-orphan"
    )

    @property
    def total_changed(self) -> int:
        if not self.counts:
            return 0
        return sum(
            c.get("created", 0) + c.get("updated", 0) for c in self.counts.values()
        )


class RecordChange(Base):
    """
    Field-level diff, written only when a watched scalar actually changes.

    This is what makes "which projects dropped their price this week" a query
    rather than a guess.
    """

    __tablename__ = "record_changes"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    run_id: Mapped[int] = mapped_column(
        ForeignKey("scrape_runs.id", ondelete="CASCADE"), index=True
    )
    entity: Mapped[str] = mapped_column(String(30), index=True)
    slug: Mapped[str] = mapped_column(String(255), index=True)
    field: Mapped[str] = mapped_column(String(60), index=True)
    old_value: Mapped[str | None] = mapped_column(Text)
    new_value: Mapped[str | None] = mapped_column(Text)
    changed_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=utcnow, index=True
    )

    run: Mapped[ScrapeRun] = relationship(back_populates="changes")

    __table_args__ = (Index("ix_record_changes_entity_slug", "entity", "slug"),)


class MediaMigration(Base):
    """
    One row per distinct source media URL mirrored to GoHighLevel.

    This is the dedup + resume ledger for app/media_migration.py: a source
    URL already present here with status='success' is never re-uploaded,
    so the migration script is safe to re-run (new scrapes bring new URLs;
    old ones are skipped).
    """

    __tablename__ = "media_migrations"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    source_url: Mapped[str] = mapped_column(Text, unique=True, index=True)
    status: Mapped[str] = mapped_column(String(20), index=True)  # success | failed
    ghl_file_id: Mapped[str | None] = mapped_column(String(100))
    ghl_url: Mapped[str | None] = mapped_column(Text)
    error: Mapped[str | None] = mapped_column(Text)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=utcnow, onupdate=utcnow
    )


class MediaRun(Base):
    """
    One row per media-migration job triggered from the dashboard.

    Deliberately two-phase so nothing is uploaded without a human saying yes:
      scanning          -> counting what would change (read-only)
      awaiting_approval -> scan finished; results shown to the user
      uploading         -> approved; downloading + uploading + rewriting URLs
      success/failed/cancelled

    `progress` mirrors ScrapeRun.progress so the dashboard can render the
    same style of live progress bar.
    """

    __tablename__ = "media_runs"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    trigger: Mapped[str] = mapped_column(String(20))  # api | cli
    status: Mapped[str] = mapped_column(String(25), index=True)

    started_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=utcnow, index=True
    )
    approved_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    finished_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    duration_seconds: Mapped[float | None] = mapped_column(Float)

    # scan results (the preview the user approves)
    scan_total_urls: Mapped[int] = mapped_column(Integer, default=0)
    scan_occurrences: Mapped[int] = mapped_column(Integer, default=0)
    scan_already_migrated: Mapped[int] = mapped_column(Integer, default=0)
    scan_pending: Mapped[int] = mapped_column(Integer, default=0)
    scan_by_table: Mapped[dict | None] = mapped_column(JsonCol)
    scan_samples: Mapped[list | None] = mapped_column(JsonCol)

    # upload results
    uploaded: Mapped[int] = mapped_column(Integer, default=0)
    failed: Mapped[int] = mapped_column(Integer, default=0)
    rows_updated: Mapped[int] = mapped_column(Integer, default=0)
    fields_updated: Mapped[int] = mapped_column(Integer, default=0)

    progress: Mapped[dict | None] = mapped_column(JsonCol)
    error: Mapped[str | None] = mapped_column(Text)


# entity name -> model, used by the ingest and the admin routes
ENTITY_MODELS: dict[str, type] = {
    "projects": Project,
    "developers": Developer,
    "areas": Area,
    "brokers": Broker,
    "learn": Article,
    "pages": Page,
}
