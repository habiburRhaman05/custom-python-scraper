"""
Smoke checks for the pieces that protect the dataset.

Run with:  python tests_smoke.py
Uses the configured DATABASE_URL; safe to run against the dev SQLite file.
"""

from __future__ import annotations

import sys

from sqlalchemy import func, select

from app.config import settings
from app.db import SessionLocal
from app.ingest import canonical_hash, parse_bedrooms, parse_handover
from app.jobs import validate_bundle
from app.models import Project

failures: list[str] = []


def check(name: str, condition: bool, detail: str = "") -> None:
    print(f"  {'PASS' if condition else 'FAIL'}  {name}" + (f"  — {detail}" if detail else ""))
    if not condition:
        failures.append(name)


print("\n--- parsers ---")
check("bedrooms 'Studio, 1, 2, 3' -> (0,3)", parse_bedrooms("Studio, 1, 2, 3") == (0, 3))
check("bedrooms 'studio to 6 beds' -> (0,6)", parse_bedrooms("studio to 6 beds") == (0, 6))
check("bedrooms '1, 2' -> (1,2)", parse_bedrooms("1, 2") == (1, 2))
check("bedrooms None -> (None,None)", parse_bedrooms(None) == (None, None))
check("handover 'Q4 2027' -> (2027,4)", parse_handover("Q4 2027") == (2027, 4))
check("handover 'Delivered' -> (None,None)", parse_handover("Delivered") == (None, None))
check("handover '2028' -> (2028,None)", parse_handover("2028") == (2028, None))

print("\n--- hashing ---")
rec = {"slug": "x", "name": "A", "scraped_at": "2026-01-01T00:00:00Z"}
same_later = {"slug": "x", "name": "A", "scraped_at": "2026-09-09T09:09:09Z"}
different = {"slug": "x", "name": "B", "scraped_at": "2026-01-01T00:00:00Z"}
check("hash ignores scraped_at", canonical_hash(rec) == canonical_hash(same_later))
check("hash reacts to content", canonical_hash(rec) != canonical_hash(different))
check("hash is key-order stable",
      canonical_hash({"a": 1, "b": 2}) == canonical_hash({"b": 2, "a": 1}))

print("\n--- safety rails (the HTML-changed protection) ---")
db = SessionLocal()
try:
    live = db.scalar(
        select(func.count()).select_from(Project).where(Project.is_active.is_(True))
    ) or 0
    print(f"  (baseline: {live} active projects in DB, guards "
          f"{'on' if settings.guard_enabled else 'OFF'})")

    if live == 0:
        print("  SKIP — ingest something first so there is a baseline")
    else:
        # Scenario: selectors broke, the scrape returns almost nothing.
        tiny = {"projects": [{"slug": "only-one", "price_aed": {"min": 1000}}]}
        checks = validate_bundle(db, tiny, full_run=True)
        by = {c["check"]: c for c in checks}
        check("collapsed project count trips the rail",
              by.get("project_count_ratio", {}).get("passed") is False,
              by.get("project_count_ratio", {}).get("detail", "rail missing"))
        check("mass delist trips the rail",
              by.get("delist_ratio", {}).get("passed") is False,
              by.get("delist_ratio", {}).get("detail", "rail missing"))

        # Scenario: prices stopped parsing but counts look fine.
        slugs = list(db.execute(select(Project.slug).where(Project.is_active.is_(True))).scalars())
        no_prices = {"projects": [{"slug": s, "price_aed": {"min": None}} for s in slugs]}
        checks = validate_bundle(db, no_prices, full_run=True)
        by = {c["check"]: c for c in checks}
        check("price-coverage collapse trips the rail",
              by.get("price_coverage_drop", {}).get("passed") is False,
              by.get("price_coverage_drop", {}).get("detail", "rail missing"))

        # Healthy run must pass everything.
        healthy = {
            "projects": [
                {"slug": s, "price_aed": {"min": 1_000_000}} for s in slugs
            ]
        }
        checks = validate_bundle(db, healthy, full_run=True)
        check("a healthy run passes every rail",
              all(c["passed"] for c in checks),
              ", ".join(c["check"] for c in checks if not c["passed"]) or "all green")

        # A partial run must never trip the count/delist rails.
        checks = validate_bundle(db, tiny, full_run=False)
        names = {c["check"] for c in checks}
        check("partial run skips count/delist rails",
              "project_count_ratio" not in names and "delist_ratio" not in names)
finally:
    db.close()

print()
if failures:
    print(f"{len(failures)} check(s) FAILED: {', '.join(failures)}")
    sys.exit(1)
print("all checks passed")
