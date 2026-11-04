"""
Incremental ingest: scraped records -> database.

The contract is that a run which finds nothing new writes nothing. Each record
is hashed; only a changed hash causes an UPDATE, and only watched scalar fields
produce a RecordChange row. Slugs that vanish from the site are soft-deleted.

This is why the scrape must stay deterministic — the hash is over the parser's
exact output, so a non-deterministic parser (or an LLM in the path) would make
every record look changed on every run.
"""

from __future__ import annotations

import hashlib
import json
import re
from datetime import datetime, timezone
from typing import Any

from sqlalchemy import select
from sqlalchemy.orm import Session

from app.models import (
    ENTITY_MODELS,
    Area,
    Article,
    Broker,
    Developer,
    Page,
    Project,
    RecordChange,
    utcnow,
)

# Keys that change on every run without the content changing.
VOLATILE_KEYS = {"scraped_at"}

# Scalar fields worth a RecordChange row. Deliberately not the JSON blobs —
# a reordered gallery is not a business event; a price move is.
WATCHED_FIELDS: dict[str, tuple[str, ...]] = {
    "projects": (
        "name", "status", "price_min_aed", "price_max_aed", "price_on_request",
        "bedrooms_raw", "handover", "handover_year", "units_total",
        "construction_status", "readiness_percent", "service_charge",
        "developer_slug", "area_slug", "aed_per_sqft_min", "aed_per_sqft_max",
    ),
    "developers": ("name", "website", "projects_tracked", "price_from_aed",
                   "median_per_sqft_aed"),
    "areas": ("name", "projects_tracked", "price_from_aed", "median_per_sqft_aed"),
    "brokers": ("name", "role", "email", "phone"),
    "learn": ("title", "published"),
    "pages": ("title",),
}


# ---------------------------------------------------------------------------
# hashing
# ---------------------------------------------------------------------------
def canonical_hash(record: dict) -> str:
    """Stable sha256 over the record, ignoring volatile keys."""
    trimmed = {k: v for k, v in record.items() if k not in VOLATILE_KEYS}
    blob = json.dumps(trimmed, sort_keys=True, ensure_ascii=False, default=str)
    return hashlib.sha256(blob.encode("utf-8")).hexdigest()


# ---------------------------------------------------------------------------
# parsing helpers — promote raw strings into queryable columns
# ---------------------------------------------------------------------------
def parse_bedrooms(raw: str | None) -> tuple[int | None, int | None]:
    """'Studio, 1, 2, 3' -> (0, 3).  'studio to 6 beds' -> (0, 6)."""
    if not raw:
        return None, None
    vals: list[int] = []
    if re.search(r"studio", raw, re.I):
        vals.append(0)
    vals.extend(int(n) for n in re.findall(r"\d+", raw))
    if not vals:
        return None, None
    return min(vals), max(vals)


def parse_handover(raw: str | None) -> tuple[int | None, int | None]:
    """'Q4 2027' -> (2027, 4).  'Delivered' -> (None, None)."""
    if not raw:
        return None, None
    year = None
    quarter = None
    m = re.search(r"\b(20\d{2})\b", raw)
    if m:
        year = int(m.group(1))
    q = re.search(r"\bQ([1-4])\b", raw, re.I)
    if q:
        quarter = int(q.group(1))
    return year, quarter


def _rng(d: Any) -> tuple[Any, Any, Any]:
    """Unpack the scraper's {'min','max','raw'} range dicts."""
    if not isinstance(d, dict):
        return None, None, None
    return d.get("min"), d.get("max"), d.get("raw")


def _int(v: Any) -> int | None:
    if v is None:
        return None
    try:
        return int(round(float(v)))
    except (TypeError, ValueError):
        return None


# ---------------------------------------------------------------------------
# record -> column mapping, one per entity
# ---------------------------------------------------------------------------
def map_project(r: dict) -> dict:
    dev = r.get("developer") or {}
    area = r.get("area") or {}
    pmin, pmax, praw = _rng(r.get("price_aed"))
    smin, smax, _ = _rng(r.get("size_sqft"))
    fmin, fmax, _ = _rng(r.get("aed_per_sqft"))
    bmin, bmax = parse_bedrooms(r.get("bedrooms_raw"))
    hyear, hq = parse_handover(r.get("handover"))

    return {
        "slug": r["slug"],
        "url": r.get("url", ""),
        "name": r.get("name") or r["slug"],
        "status": r.get("status"),
        "developer_slug": dev.get("slug"),
        "developer_name": dev.get("name"),
        "area_slug": area.get("slug"),
        "area_name": area.get("name"),
        "price_min_aed": _int(pmin),
        "price_max_aed": _int(pmax),
        "price_raw": praw,
        "price_on_request": bool(r.get("price_on_request")),
        "price_usd_approx": r.get("price_usd_approx"),
        "bedrooms_raw": r.get("bedrooms_raw"),
        "bedrooms_min": bmin,
        "bedrooms_max": bmax,
        "handover": r.get("handover"),
        "handover_year": hyear,
        "handover_quarter": hq,
        "size_min_sqft": smin,
        "size_max_sqft": smax,
        "aed_per_sqft_min": fmin,
        "aed_per_sqft_max": fmax,
        "units_total": _int(r.get("units_total")),
        "inventory_raw": r.get("inventory_raw"),
        "construction_status": r.get("construction_status"),
        "readiness_percent": r.get("readiness_percent"),
        "service_charge": r.get("service_charge"),
        "furnishing": r.get("furnishing"),
        "post_handover_plan": r.get("post_handover_plan"),
        "unit_types": r.get("unit_types"),
        "payment_plans": r.get("payment_plans"),
        "amenities": r.get("amenities"),
        "gallery": r.get("gallery"),
        "faqs": r.get("faqs"),
        "sections": r.get("sections"),
        "all_facts": r.get("all_facts"),
        "address": r.get("address"),
        "meta": r.get("meta"),
        "json_ld": r.get("json_ld"),
    }


def _snapshot_cols(r: dict) -> dict:
    s = r.get("snapshot") or {}
    return {
        "projects_tracked": _int(s.get("projects_tracked")),
        "price_from_aed": _int(s.get("price_from_aed")),
        "median_per_sqft_aed": _int(s.get("median_per_sqft_aed")),
        "snapshot_bedrooms_raw": s.get("bedrooms_raw"),
        "snapshot_handover_raw": s.get("handover_raw"),
        "snapshot_summary": s.get("summary"),
    }


def map_developer(r: dict) -> dict:
    return {
        "slug": r["slug"],
        "url": r.get("url", ""),
        "name": r.get("name") or r["slug"],
        "website": r.get("website"),
        "logo_url": r.get("logo_url"),
        "about": r.get("about"),
        "offices": r.get("offices"),
        "areas_active_in": r.get("areas_active_in"),
        "sections": r.get("sections"),
        "faqs": r.get("faqs"),
        "meta": r.get("meta"),
        **_snapshot_cols(r),
    }


def map_area(r: dict) -> dict:
    return {
        "slug": r["slug"],
        "url": r.get("url", ""),
        "name": r.get("name") or r["slug"],
        "intro": r.get("intro"),
        "developers_active": r.get("developers_active"),
        "sections": r.get("sections"),
        "faqs": r.get("faqs"),
        "gallery": r.get("gallery"),
        "meta": r.get("meta"),
        **_snapshot_cols(r),
    }


def map_broker(r: dict) -> dict:
    return {
        "slug": r["slug"],
        "url": r.get("url", ""),
        "name": r.get("name") or r["slug"],
        "role": r.get("role"),
        "photo_url": r.get("photo_url"),
        "email": r.get("email"),
        "phone": r.get("phone"),
        "bio": r.get("bio"),
        "emails": r.get("emails"),
        "phones": r.get("phones"),
        "socials": r.get("socials"),
        "languages": r.get("languages") if isinstance(r.get("languages"), list) else None,
        "meta": r.get("meta"),
    }


def map_article(r: dict) -> dict:
    return {
        "slug": r["slug"],
        "url": r.get("url", ""),
        "title": r.get("title") or r["slug"],
        "body": r.get("body"),
        "published": r.get("published"),
        "author": r.get("author") if isinstance(r.get("author"), str) else None,
        "sections": r.get("sections"),
        "faqs": r.get("faqs"),
        "meta": r.get("meta"),
    }


def map_page(r: dict) -> dict:
    return {
        "slug": r.get("path") or r.get("url", ""),
        "url": r.get("url", ""),
        "title": r.get("title"),
        "sections": r.get("sections"),
        "faqs": r.get("faqs"),
        "meta": r.get("meta"),
    }


MAPPERS = {
    "projects": map_project,
    "developers": map_developer,
    "areas": map_area,
    "brokers": map_broker,
    "learn": map_article,
    "pages": map_page,
}


# ---------------------------------------------------------------------------
# the ingest itself
# ---------------------------------------------------------------------------
def _as_text(v: Any) -> str | None:
    if v is None:
        return None
    if isinstance(v, bool):
        return str(v)
    if isinstance(v, float) and v.is_integer():
        # Float columns read back as 378.0 where the scrape produced 378;
        # normalise so that is not reported as a change.
        return str(int(v))
    return str(v)


def _changed(old: Any, new: Any) -> bool:
    """True only on a real value change, not a type round-trip artifact."""
    if old is None and new is None:
        return False
    if isinstance(old, bool) or isinstance(new, bool):
        return bool(old) != bool(new)
    if isinstance(old, (int, float)) and isinstance(new, (int, float)):
        return abs(float(old) - float(new)) > 1e-9
    return _as_text(old) != _as_text(new)


def ingest_entity(
    db: Session,
    entity: str,
    records: list[dict],
    run_id: int,
    *,
    delist_missing: bool = True,
) -> dict[str, int]:
    """
    Upsert one entity type. Returns
    {"created": n, "updated": n, "unchanged": n, "delisted": n, "skipped": n}.
    """
    model = ENTITY_MODELS[entity]
    mapper = MAPPERS[entity]
    watched = WATCHED_FIELDS.get(entity, ())
    now = utcnow()

    stats = {"created": 0, "updated": 0, "unchanged": 0, "delisted": 0, "skipped": 0}

    existing: dict[str, Any] = {
        row.slug: row for row in db.execute(select(model)).scalars()
    }
    seen: set[str] = set()

    for rec in records:
        if rec.get("parse_error") or not rec.get("slug") and entity != "pages":
            stats["skipped"] += 1
            continue
        try:
            cols = mapper(rec)
        except (KeyError, TypeError):
            stats["skipped"] += 1
            continue
        if not cols.get("slug"):
            stats["skipped"] += 1
            continue

        slug = cols["slug"]
        seen.add(slug)
        digest = canonical_hash(rec)
        row = existing.get(slug)

        if row is None:
            row = model(**cols)
            row.content_hash = digest
            row.first_seen_at = now
            row.last_seen_at = now
            row.last_changed_at = now
            row.is_active = True
            row.delisted_at = None
            db.add(row)
            stats["created"] += 1
            continue

        if row.content_hash == digest and row.is_active:
            # nothing changed — just record that we still saw it
            row.last_seen_at = now
            stats["unchanged"] += 1
            continue

        # content changed: diff the watched scalars before overwriting
        for field in watched:
            old = getattr(row, field, None)
            new = cols.get(field)
            if _changed(old, new):
                db.add(
                    RecordChange(
                        run_id=run_id,
                        entity=entity,
                        slug=slug,
                        field=field,
                        old_value=_as_text(old),
                        new_value=_as_text(new),
                        changed_at=now,
                    )
                )

        for key, value in cols.items():
            setattr(row, key, value)
        row.content_hash = digest
        row.last_seen_at = now
        row.last_changed_at = now
        if not row.is_active:
            # it came back — relist it and note that as a change
            db.add(
                RecordChange(
                    run_id=run_id, entity=entity, slug=slug, field="is_active",
                    old_value="False", new_value="True", changed_at=now,
                )
            )
            row.is_active = True
            row.delisted_at = None
        stats["updated"] += 1

    if delist_missing:
        for slug, row in existing.items():
            if slug in seen or not row.is_active:
                continue
            row.is_active = False
            row.delisted_at = now
            db.add(
                RecordChange(
                    run_id=run_id, entity=entity, slug=slug, field="is_active",
                    old_value="True", new_value="False", changed_at=now,
                )
            )
            stats["delisted"] += 1

    db.flush()
    return stats


def ingest_bundle(
    db: Session, bundle: dict, run_id: int, *, delist_missing: bool = True
) -> dict[str, dict[str, int]]:
    """Ingest every entity type present in a scrape bundle."""
    out: dict[str, dict[str, int]] = {}
    for entity in ENTITY_MODELS:
        records = bundle.get(entity)
        if not isinstance(records, list):
            continue
        out[entity] = ingest_entity(
            db, entity, records, run_id,
            # Only delist when the scrape covered that entity in full. A
            # --limit or --only run must never mass-delist the rest.
            delist_missing=delist_missing,
        )
    return out
