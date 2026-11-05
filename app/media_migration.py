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
import concurrent.futures as cf
import os
import re
import threading
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Callable
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


def _find_urls_ordered(value: Any, out: list[str]) -> None:
    """Same walk as _find_urls but preserves order, so list indexes are stable."""
    if isinstance(value, str):
        if is_media_url(value):
            out.append(value)
    elif isinstance(value, dict):
        for v in value.values():
            _find_urls_ordered(v, out)
    elif isinstance(value, list):
        for v in value:
            _find_urls_ordered(v, out)


# How each column should be described in a generated filename.
_COLUMN_LABELS = {
    "gallery": "gallery",
    "brochure_url": "brochure",
    "floor_plan_url": "floor-plan",
    "logo_url": "logo",
    "photo_url": "photo",
    "thumbnail_url": "thumbnail",
}

_ENTITY_PREFIX = {
    "projects": "project", "developers": "developer", "areas": "area",
    "brokers": "broker", "learn": "article", "pages": "page",
}

_SAFE_NAME = re.compile(r"[^a-zA-Z0-9._-]+")


def build_filename(entity: str, slug: str, column: str, index: int | None,
                    url: str) -> str:
    """
    A human-readable GHL filename, e.g.

        project-09-life-residences-gallery-03.webp
        project-09-life-residences-brochure.pdf
        developer-lmd-logo.webp

    Falls back to the source basename only if there is no usable context,
    which is what produced the opaque hashed names before.
    """
    ext = os.path.splitext(urlparse(url).path)[1].lower()
    if not ext or len(ext) > 6:
        ext = ".jpg" if not url.lower().endswith(".pdf") else ".pdf"

    label = _COLUMN_LABELS.get(column, column.replace("_url", "").replace("_", "-"))
    parts = [_ENTITY_PREFIX.get(entity, entity.rstrip("s")), slug, label]
    if index is not None:
        parts.append(f"{index + 1:02d}")

    name = _SAFE_NAME.sub("-", "-".join(p for p in parts if p)).strip("-")
    return f"{name[:110]}{ext}"


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
    # url -> generated filename, built from the row it was found on. First
    # occurrence wins, so a gallery image shared by two projects keeps one
    # stable name rather than flip-flopping between runs.
    names: dict[str, str] = field(default_factory=dict)

    def samples(self, n: int = 8) -> list[str]:
        """A few real URLs (with their new names) for the approval screen."""
        out = []
        for url in sorted(self.unique_urls)[:n]:
            name = self.names.get(url)
            out.append(f"{name}  <-  {url}" if name else url)
        return out


def scan(db: Session) -> ScanResult:
    """Read-only: find every distinct media URL referenced anywhere in the DB."""
    result = ScanResult()
    for name, model in ENTITY_MODELS.items():
        columns = [c.name for c in model.__table__.columns]
        count = 0
        for row in db.execute(select(model)).scalars():
            row_urls: set[str] = set()
            slug = getattr(row, "slug", None) or ""
            for col in columns:
                value = getattr(row, col)
                ordered: list[str] = []
                _find_urls_ordered(value, ordered)
                if not ordered:
                    continue
                row_urls.update(ordered)
                multiple = len(ordered) > 1
                for idx, url in enumerate(ordered):
                    if url not in result.names:
                        result.names[url] = build_filename(
                            name, slug, col, idx if multiple else None, url
                        )
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
                 rate_limit_per_sec: float = 4.0,
                 image_folder_id: str = "", pdf_folder_id: str = ""):
        if not pit_token or not location_id:
            raise GHLError("GHL_PIT_TOKEN and GHL_LOCATION_ID must be set in .env")
        self.location_id = location_id
        self.parent_folder_id = parent_folder_id
        # Routed by file type so the media library stays organised instead of
        # dumping ~38k files into the root.
        self.image_folder_id = image_folder_id
        self.pdf_folder_id = pdf_folder_id
        self._headers = {
            "Authorization": f"Bearer {pit_token}",
            "Version": GHL_VERSION,
        }
        # One Session per thread: connection pooling without sharing a
        # Session across the worker threads that upload concurrently.
        self._local = threading.local()
        self._min_interval = 1.0 / rate_limit_per_sec if rate_limit_per_sec > 0 else 0.0
        self._last_call = 0.0
        # Worker threads share this client, so the throttle has to be guarded
        # or the rate limit is meaningless (and GHL starts returning 429s).
        self._throttle_lock = threading.Lock()

    def folder_for(self, url: str, filename: str) -> str:
        """PDFs to the pdf folder, everything else to the image folder."""
        is_pdf = (url.lower().split("?")[0].endswith(".pdf")
                  or filename.lower().endswith(".pdf"))
        chosen = self.pdf_folder_id if is_pdf else self.image_folder_id
        return chosen or self.parent_folder_id

    @property
    def session(self) -> requests.Session:
        s = getattr(self._local, "session", None)
        if s is None:
            s = requests.Session()
            s.headers.update(self._headers)
            self._local.session = s
        return s

    def _throttle(self) -> None:
        with self._throttle_lock:
            wait = self._min_interval - (time.monotonic() - self._last_call)
            if wait > 0:
                time.sleep(wait)
            self._last_call = time.monotonic()

    MAX_BYTES = 24 * 1024 * 1024  # GHL's own cap is 25MB for non-video files

    def upload_from_url(self, source_url: str, *, name: str | None = None,
                         retries: int = 3) -> dict:
        """
        Downloads the file ourselves, then uploads the actual bytes.

        hosted=true + fileUrl=<source_url> looked like it should let GHL fetch
        the file server-side, but verified empirically (GET /medias/files)
        that it does NOT: the stored entry has hosted=true, bucketName="",
        and its url is just an echo of the source -- no copy is ever made.
        A genuine independent copy only exists for entries with
        hosted=false, bucketName="msgsndr", url on assets.cdn.filesafe.space
        -- which this real multipart file upload produces.
        """
        last_exc: Exception | None = None
        for attempt in range(retries):
            try:
                dl = requests.get(source_url, timeout=60, stream=True)
                dl.raise_for_status()
                content = dl.content
            except requests.RequestException as exc:
                last_exc = exc
                time.sleep(2 ** attempt)
                continue
            if len(content) > self.MAX_BYTES:
                raise GHLError(f"{len(content)} bytes exceeds GHL's {self.MAX_BYTES}-byte limit")
            content_type = dl.headers.get("Content-Type") or "application/octet-stream"
            filename = (name or source_url.rsplit("/", 1)[-1].split("?", 1)[0] or "file")[:120]

            fields: dict[str, Any] = {"locationId": (None, self.location_id)}
            folder_id = self.folder_for(source_url, filename)
            if folder_id:
                fields["parentId"] = (None, folder_id)
            # Send the name both ways: GHL honours the explicit `name` field
            # over the multipart filename, and relying on the filename alone
            # is undocumented behaviour.
            fields["name"] = (None, filename)
            fields["file"] = (filename, content, content_type)

            self._throttle()
            try:
                resp = self.session.post(
                    f"{GHL_API_BASE}/medias/upload-file", files=fields, timeout=120
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
def pending_urls(db: Session, urls: set[str]) -> tuple[set[str], int]:
    """Split a URL set into (not-yet-uploaded, already_successful_count)."""
    already_ok = {
        row.source_url
        for row in db.execute(
            select(MediaMigration).where(
                MediaMigration.source_url.in_(urls),
                MediaMigration.status == "success",
            )
        ).scalars()
    }
    return urls - already_ok, len(already_ok)


def upload_pending(db: Session, urls: set[str], client: GHLClient,
                    limit: int | None = None,
                    on_progress: Callable[[int, int, int, int], None] | None = None,
                    workers: int = 6,
                    names: dict[str, str] | None = None,
                    ) -> dict[str, int]:
    """
    Upload every URL not already recorded as a success. Returns counts.

    on_progress(done, total, uploaded, failed) fires every 10 files so the
    dashboard can show live progress over what is a multi-hour job.
    """
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
    total = len(pending)

    def _fetch(url: str) -> tuple[str, dict | None, str | None]:
        """Network-only; runs on a worker thread. Never touches the session."""
        # Prefer the meaningful name derived from the owning row; fall back to
        # the source basename only when there was no usable context.
        name = (names or {}).get(url) or url.rsplit("/", 1)[-1].split("?", 1)[0] or None
        try:
            return url, client.upload_from_url(url, name=name), None
        except GHLError as exc:
            return url, None, str(exc)[:2000]
        except Exception as exc:  # a single bad file must not kill the batch
            return url, None, f"{type(exc).__name__}: {exc}"[:2000]

    # Each file costs ~2s wall-clock, almost all of it waiting on network, so
    # running these sequentially would take ~20h for the full set. Parallel
    # workers do the download+upload; the DB is only ever written from this
    # thread, so the Session stays single-threaded.
    idx = 0
    with cf.ThreadPoolExecutor(max_workers=workers) as ex:
        for url, body, error in ex.map(_fetch, pending):
            idx += 1
            row = existing.get(url) or MediaMigration(source_url=url)
            if body is not None:
                row.status = "success"
                row.ghl_file_id = body.get("fileId")
                row.ghl_url = body["url"]
                row.error = None
                counts["uploaded"] += 1
            else:
                row.status = "failed"
                row.error = error
                counts["failed"] += 1
            row.updated_at = datetime.now(timezone.utc)
            db.add(row)
            db.commit()

            # idx == 1 so the progress bar appears right away rather than after
            # the first 10 files (which, on a slow batch, looks like a hang).
            if on_progress and (idx == 1 or idx % 10 == 0 or idx == total):
                on_progress(idx, total, counts["uploaded"], counts["failed"])
    return counts


# ---------------------------------------------------------------------------
# replace phase -- only successes are substituted; failures leave originals
# ---------------------------------------------------------------------------
def replace_everywhere(db: Session,
                        on_progress: Callable[[int, int], None] | None = None,
                        ) -> dict[str, int]:
    mapping = {
        row.source_url: row.ghl_url
        for row in db.execute(
            select(MediaMigration).where(MediaMigration.status == "success")
        ).scalars()
    }
    if not mapping:
        return {"rows_updated": 0, "fields_updated": 0}

    rows_updated = fields_updated = 0
    tables_done = 0
    tables_total = len(ENTITY_MODELS)
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
        tables_done += 1
        if on_progress:
            on_progress(tables_done, tables_total)
    db.commit()
    return {"rows_updated": rows_updated, "fields_updated": fields_updated}


def build_client() -> GHLClient:
    return GHLClient(
        settings.ghl_pit_token, settings.ghl_location_id,
        settings.ghl_parent_folder_id, settings.ghl_rate_limit_per_sec,
        image_folder_id=settings.ghl_image_folder_id,
        pdf_folder_id=settings.ghl_pdf_folder_id,
    )
