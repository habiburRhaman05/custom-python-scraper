"""
Command line entrypoint — `python -m app.cli <command>`.

    scrape      scrape + ingest (what cron calls if you'd rather skip HTTP)
    ingest      re-ingest existing JSON from disk without re-scraping
    runs        show recent run history
    initdb      create tables directly (use alembic in production)
    stats       quick counts straight from the database
    migrate-media   mirror Reelly/S3 media into GoHighLevel (dry-run by default)
"""

from __future__ import annotations

import argparse
import json
import os
import sys

from sqlalchemy import desc, func, select

from app.config import settings
from app.db import Base, SessionLocal, engine
from app.ingest import ingest_bundle
from app.jobs import run_scrape_and_ingest, validate_bundle
from app.media_migration import build_client, replace_everywhere, scan, upload_pending
from app.models import ENTITY_MODELS, Project, ScrapeRun, utcnow


def cmd_scrape(args: argparse.Namespace) -> int:
    result = run_scrape_and_ingest(
        trigger=args.trigger, only=args.only, limit=args.limit
    )
    print("\n" + json.dumps(result, indent=2, default=str))
    return 0 if result.get("status") == "success" else 1


def cmd_ingest(args: argparse.Namespace) -> int:
    """Load the JSON files already on disk into the database."""
    out = args.dir or settings.scrape_output_dir
    bundle: dict = {}
    for entity in ENTITY_MODELS:
        path = os.path.join(out, f"{entity}.json")
        if os.path.exists(path):
            with open(path, encoding="utf-8") as fh:
                bundle[entity] = json.load(fh)
            print(f"  loaded {entity:<12} {len(bundle[entity]):>5} records")
    if not bundle:
        print(f"no entity JSON found in {out!r}", file=sys.stderr)
        return 1

    full = not args.only and not args.partial
    db = SessionLocal()
    try:
        checks = validate_bundle(db, bundle, full_run=full)
        failed = [c for c in checks if not c["passed"]]
        if failed and settings.guard_enabled and not args.force:
            print("\nsafety rails tripped — nothing written:", file=sys.stderr)
            for c in failed:
                print(f"  - {c['check']}: {c['detail']}", file=sys.stderr)
            print("\nre-run with --force to override.", file=sys.stderr)
            return 2

        run = ScrapeRun(
            trigger="cli", status="running", started_at=utcnow(),
            validation={"full_run": full, "checks": checks},
        )
        db.add(run)
        db.commit()

        counts = ingest_bundle(db, bundle, run.id, delist_missing=full)
        run.counts = counts
        run.status = "success"
        run.finished_at = utcnow()
        db.commit()
        print("\n" + json.dumps(counts, indent=2))
        return 0
    finally:
        db.close()


def cmd_runs(args: argparse.Namespace) -> int:
    db = SessionLocal()
    try:
        rows = db.execute(
            select(ScrapeRun).order_by(desc(ScrapeRun.started_at)).limit(args.limit)
        ).scalars()
        print(f"{'id':>4}  {'status':<9} {'trigger':<7} {'started':<20} {'dur':>7}  changed")
        for r in rows:
            changed = r.total_changed
            print(
                f"{r.id:>4}  {r.status:<9} {r.trigger:<7} "
                f"{r.started_at:%Y-%m-%d %H:%M:%S}  "
                f"{(r.duration_seconds or 0):>6.1f}s  {changed}"
                + (f"  [{r.error[:60]}]" if r.error else "")
            )
        return 0
    finally:
        db.close()


def cmd_initdb(args: argparse.Namespace) -> int:
    Base.metadata.create_all(engine)
    print(f"tables created on {engine.url.render_as_string(hide_password=True)}")
    return 0


def cmd_stats(args: argparse.Namespace) -> int:
    db = SessionLocal()
    try:
        for name, model in ENTITY_MODELS.items():
            total = db.scalar(select(func.count()).select_from(model)) or 0
            live = db.scalar(
                select(func.count()).select_from(model).where(model.is_active.is_(True))
            ) or 0
            print(f"  {name:<12} {live:>5} active / {total:>5} total")
        priced = db.scalar(
            select(func.count()).select_from(Project).where(
                Project.is_active.is_(True), Project.price_min_aed.isnot(None)
            )
        ) or 0
        print(f"\n  projects with a price: {priced}")
        return 0
    finally:
        db.close()


def cmd_migrate_media(args: argparse.Namespace) -> int:
    db = SessionLocal()
    try:
        print("scanning every table/column for Reelly/S3 media URLs...")
        result = scan(db)
        print(f"\n  {len(result.unique_urls)} distinct URL(s), "
              f"{result.occurrences} occurrence(s) across:")
        for table, count in sorted(result.by_table.items(), key=lambda kv: -kv[1]):
            print(f"    {table:<12} {count}")

        if not args.apply:
            print("\nDRY RUN -- nothing uploaded, nothing written.")
            print("Re-run with --apply to upload to GoHighLevel and replace the URLs.")
            return 0

        if not result.unique_urls:
            print("\nnothing to upload.")
            return 0

        client = build_client()
        print(f"\nuploading (rate limit: {settings.ghl_rate_limit_per_sec}/s)...")
        upload_counts = upload_pending(db, result.unique_urls, client, limit=args.limit)
        print(f"  already cached: {upload_counts['already_cached']}")
        print(f"  uploaded:       {upload_counts['uploaded']}")
        print(f"  failed:         {upload_counts['failed']}")

        print("\nreplacing URLs in the database (uploaded ones only)...")
        replace_counts = replace_everywhere(db)
        print(f"  rows updated:   {replace_counts['rows_updated']}")
        print(f"  fields updated: {replace_counts['fields_updated']}")

        if upload_counts["failed"]:
            print(f"\n{upload_counts['failed']} URL(s) failed and were left untouched "
                  f"(original URL still in the DB) -- re-run to retry just those.")
        return 0
    finally:
        db.close()


def main() -> int:
    ap = argparse.ArgumentParser(prog="python -m app.cli")
    sub = ap.add_subparsers(dest="cmd", required=True)

    s = sub.add_parser("scrape", help="scrape the site then ingest the result")
    s.add_argument("--only", nargs="+", help="limit to these entity types")
    s.add_argument("--limit", type=int, help="max pages per entity type")
    s.add_argument("--trigger", default="cli", choices=["cli", "cron", "api"])
    s.set_defaults(func=cmd_scrape)

    i = sub.add_parser("ingest", help="ingest JSON already on disk")
    i.add_argument("--dir", help=f"default: {settings.scrape_output_dir}")
    i.add_argument("--only", nargs="+", help="only these entity types")
    i.add_argument("--partial", action="store_true",
                   help="treat as a partial run (never delist)")
    i.add_argument("--force", action="store_true", help="ignore the safety rails")
    i.set_defaults(func=cmd_ingest)

    r = sub.add_parser("runs", help="recent run history")
    r.add_argument("--limit", type=int, default=20)
    r.set_defaults(func=cmd_runs)

    d = sub.add_parser("initdb", help="create tables without alembic")
    d.set_defaults(func=cmd_initdb)

    t = sub.add_parser("stats", help="row counts from the database")
    t.set_defaults(func=cmd_stats)

    m = sub.add_parser("migrate-media",
                        help="mirror Reelly/S3 media into GoHighLevel (dry-run by default)")
    m.add_argument("--apply", action="store_true",
                    help="actually upload and rewrite the DB (default: scan + report only)")
    m.add_argument("--limit", type=int, help="upload at most N URLs this run (for testing)")
    m.set_defaults(func=cmd_migrate_media)

    args = ap.parse_args()
    return args.func(args)


if __name__ == "__main__":
    try:
        sys.exit(main())
    except KeyboardInterrupt:
        print("\ninterrupted", file=sys.stderr)
        sys.exit(130)
