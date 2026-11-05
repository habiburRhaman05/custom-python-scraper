"""
Mirror every Reelly/S3-hosted media URL in the database into GoHighLevel's
Media Library, then rewrite the DB to point at the GHL-hosted copy instead.

Deliberately NOT wired into the scrape/ingest pipeline. That pipeline's
correctness depends on being deterministic (same input -> same hash -> no
spurious "changed" rows); a third-party API call does not belong in that
path. This is a separate, explicitly-triggered, resumable operation.

Design:
  1. SCAN   -- generically walk every row of every content table and every
              column (scalar + nested JSON), collecting every string value
              whose host is a known Reelly/S3 media domain. No per-field
              hardcoding: a new JSON field or table is covered automatically.
  2. UPLOAD -- for each *distinct* URL not already recorded with
              status='success' in `media_migrations`, call GoHighLevel's
              /medias/upload-file with hosted=true + fileUrl=<source url>,
              so GHL fetches the bytes itself -- we never download anything.
              Every outcome (success or failure) is persisted immediately,
              so a crash mid-run loses no progress and a re-run only
              attempts what's left.
  3. REPLACE -- using the url -> ghl_url mapping built in step 2 (successes
              only), rewrite matching values in place across every table and
              commit. A URL whose upload failed is left untouched -- the
              original keeps working rather than ever being replaced with
              something broken.

Defaults to a dry run. Nothing is uploaded or written without --apply.
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any
from urllib.parse import urlparse

import requests
from sqlalchemy import select
from sqlalchemy.orm import Session
from sqlalchemy.orm.attributes import flag_modified

from app.config import settings
from app.models import ENTITY_MODELS, MediaMigration

GHL_API_BASE = "https://services.leadconnectorhq.com"
GHL_VERSION = "2021-07-28"

# The only two hosts real media lives on, confirmed against the live site --
# everything else (maisondubai.com links, social profiles, developer
# websites) is left alone.
MEDIA_DOMAINS = ("reelly-backend.s3.amazonaws.com", "api.reelly.io")


def is_media_url(value: Any) -> bool:
    if not isinstance(value, str) or not value.startswith(("http://", "https://")):
        return False
    host = urlparse(value).netloc.lower()
    return any(host == d or host.endswith("." + d) for d in MEDIA_DOMAINS)


# ---------------------------------------------------------------------------
# generic scan: every column, every row, every table -- no hardcoded fields
# ---------------------------------------------------------------------------
def _find_urls(value: Any, found: set[str]) -> None:
    """Recursively collect every media URL inside a column value."""
    if isinstance(value, str):
        if is_media_url(value):
            found.add(value)
    elif isinstance(value, dict):
        for v in value.values():
            _find_urls(v, found)
    elif isinstance(value, list):
        for v in value:
            _find_urls(v, found)


def _substitute(value: Any, mapping: dict[str, str]) -> Any:
    """Deep-copy `value`, replacing any mapped URL. Unmapped values pass through."""
    if isinstance(value, str):
        return mapping.get(value, value)
    if isinstance(value, dict):
        return {k: _substitute(v, mapping) for k, v in value.items()}
    if isinstance(value, list):
        return [_substitute(v, mapping) for v in value]
    return value


@dataclass
class ScanResult:
    unique_urls: set[str] = field(default_factory=set)
    occurrences: int = 0
    by_table: dict[str, int] = field(default_factory=dict)


def scan(db: Session) -> ScanResult:
    """Read-only: find every distinct media URL referenced anywhere in the DB."""
    result = ScanResult()
    for name, model in ENTITY_MODELS.items():
        columns = [c.name for c in model.__table__.columns]
        count = 0
        for row in db.execute(select(model)).scalars():
            row_urls: set[str] = set()
            for col in columns:
                _find_urls(getattr(row, col), row_urls)
            if row_urls:
                count += len(row_urls)
                result.unique_urls.update(row_urls)
        if count:
            result.by_table[name] = count
            result.occurrences += count
    return result


# ---------------------------------------------------------------------------
# GHL upload
# ---------------------------------------------------------------------------
class GHLError(Exception):
    pass


class GHLClient:
    def __init__(self, pit_token: str, location_id: str, parent_folder_id: str = "",
                 rate_limit_per_sec: float = 4.0):
        if not pit_token or not location_id:
            raise GHLError("GHL_PIT_TOKEN and GHL_LOCATION_ID must be set in .env")
        self.location_id = location_id
        self.parent_folder_id = parent_folder_id
        self.session = requests.Session()
        self.session.headers.update({
            "Authorization": f"Bearer {pit_token}",
            "Version": GHL_VERSION,
        })
        self._min_interval = 1.0 / rate_limit_per_sec if rate_limit_per_sec > 0 else 0.0
        self._last_call = 0.0

    def _throttle(self) -> None:
        wait = self._min_interval - (time.monotonic() - self._last_call)
        if wait > 0:
            time.sleep(wait)
        self._last_call = time.monotonic()

    def upload_from_url(self, source_url: str, *, name: str | None = None,
                         retries: int = 3) -> dict:
        """
        hosted=true + fileUrl=<source_url>: GHL fetches the bytes itself --
        we never download the file through our own process.
        """
        fields = {
            "hosted": "true",
            "fileUrl": source_url,
            "locationId": self.location_id,
        }
        if name:
            fields["name"] = name[:120]
        if self.parent_folder_id:
            fields["parentId"] = self.parent_folder_id
        # GHL's upload endpoint requires multipart/form-data even when every
        # field is a plain string -- requests only sends that encoding when
        # `files=` is used, so each field is wrapped as a (filename=None) part.
        multipart = {k: (None, v) for k, v in fields.items()}

        last_exc: Exception | None = None
        for attempt in range(retries):
            self._throttle()
            try:
                resp = self.session.post(
                    f"{GHL_API_BASE}/medias/upload-file", files=multipart, timeout=60
                )
            except requests.RequestException as exc:
                last_exc = exc
                time.sleep(2 ** attempt)
                continue

            if resp.status_code == 429:
                time.sleep(float(resp.headers.get("Retry-After", 2 ** (attempt + 1))))
                continue
            if resp.status_code >= 500:
                time.sleep(2 ** attempt)
                continue
            if not resp.ok:
                raise GHLError(f"{resp.status_code}: {resp.text[:500]}")

            body = resp.json()
            if "url" not in body:
                raise GHLError(f"unexpected response shape: {body}")
            return body

        raise GHLError(f"gave up after {retries} attempts: {last_exc}")


# ---------------------------------------------------------------------------
# upload phase -- persists every outcome immediately
# ---------------------------------------------------------------------------
def upload_pending(db: Session, urls: set[str], client: GHLClient,
                    limit: int | None = None) -> dict[str, int]:
    """Upload every URL not already recorded as a success. Returns counts."""
    existing = {
        row.source_url: row
        for row in db.execute(
            select(MediaMigration).where(MediaMigration.source_url.in_(urls))
        ).scalars()
    }
    already_ok = {u for u, row in existing.items() if row.status == "success"}
    pending = sorted(urls - already_ok)
    if limit:
        pending = pending[:limit]

    counts = {"already_cached": len(already_ok), "uploaded": 0, "failed": 0}
    for url in pending:
        name = url.rsplit("/", 1)[-1].split("?", 1)[0] or None
        try:
            body = client.upload_from_url(url, name=name)
            row = existing.get(url) or MediaMigration(source_url=url)
            row.status = "success"
            row.ghl_file_id = body.get("fileId")
            row.ghl_url = body["url"]
            row.error = None
            row.updated_at = datetime.now(timezone.utc)
            db.add(row)
            db.commit()
            counts["uploaded"] += 1
        except GHLError as exc:
            row = existing.get(url) or MediaMigration(source_url=url)
            row.status = "failed"
            row.error = str(exc)[:2000]
            row.updated_at = datetime.now(timezone.utc)
            db.add(row)
            db.commit()
            counts["failed"] += 1
    return counts


# ---------------------------------------------------------------------------
# replace phase -- only successes are substituted; failures leave originals
# ---------------------------------------------------------------------------
def replace_everywhere(db: Session) -> dict[str, int]:
    mapping = {
        row.source_url: row.ghl_url
        for row in db.execute(
            select(MediaMigration).where(MediaMigration.status == "success")
        ).scalars()
    }
    if not mapping:
        return {"rows_updated": 0, "fields_updated": 0}

    rows_updated = fields_updated = 0
    for model in ENTITY_MODELS.values():
        columns = [c.name for c in model.__table__.columns]
        for row in db.execute(select(model)).scalars():
            touched = False
            for col in columns:
                old = getattr(row, col)
                found: set[str] = set()
                _find_urls(old, found)
                if not (found & mapping.keys()):
                    continue
                new = _substitute(old, mapping)
                setattr(row, col, new)
                flag_modified(row, col)
                touched = True
                fields_updated += 1
            if touched:
                rows_updated += 1
    db.commit()
    return {"rows_updated": rows_updated, "fields_updated": fields_updated}


def build_client() -> GHLClient:
    return GHLClient(
        settings.ghl_pit_token, settings.ghl_location_id,
        settings.ghl_parent_folder_id, settings.ghl_rate_limit_per_sec,
    )
