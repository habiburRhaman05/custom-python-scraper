"""
Exercise every route in-process via FastAPI TestClient (no server needed).

Run with:  python tests_api.py
"""

from __future__ import annotations

import sys

from fastapi.testclient import TestClient

from app.config import settings
from app.main import app

KEY = {"X-API-Key": settings.admin_api_key}
failures: list[str] = []


def check(name: str, ok: bool, detail: str = "") -> None:
    print(f"  {'PASS' if ok else 'FAIL'}  {name}" + (f"  — {detail}" if detail else ""))
    if not ok:
        failures.append(name)


with TestClient(app) as c:
    print("\n--- meta ---")
    r = c.get("/health")
    check("GET /health", r.status_code == 200 and r.json()["database"] == "ok",
          f"{r.status_code} active={r.json().get('projects_active')}")

    r = c.get("/api/stats")
    s = r.json() if r.status_code == 200 else {}
    check("GET /api/stats", r.status_code == 200,
          f"counts={s.get('counts')} coverage={s.get('price_coverage_pct')}%")

    print("\n--- projects ---")
    r = c.get("/api/projects")
    body = r.json() if r.status_code == 200 else {}
    check("GET /api/projects", r.status_code == 200 and "items" in body,
          f"total={body.get('total')} pages={body.get('pages')}")

    first = (body.get("items") or [{}])[0]
    slug = first.get("slug")

    r = c.get("/api/projects", params={"page_size": 2})
    check("pagination caps page_size", r.status_code == 200 and len(r.json()["items"]) <= 2)

    r = c.get("/api/projects", params={"sort": "price_desc"})
    prices = [i["price_min_aed"] for i in r.json()["items"] if i["price_min_aed"]]
    check("sort=price_desc is descending", prices == sorted(prices, reverse=True),
          str(prices[:4]))

    r = c.get("/api/projects", params={"has_price": True})
    check("has_price=true filter",
          r.status_code == 200
          and all(i["price_min_aed"] is not None for i in r.json()["items"]))

    r = c.get("/api/projects", params={"price_max": 1_000_000, "has_price": True})
    check("price_max filter", r.status_code == 200
          and all(i["price_min_aed"] <= 1_000_000 for i in r.json()["items"]),
          f"{r.json()['total']} match")

    r = c.get("/api/projects", params={"bedrooms": 0})
    check("bedrooms=0 (studio) filter", r.status_code == 200,
          f"{r.json()['total']} with a studio")

    if slug:
        r = c.get(f"/api/projects/{slug}")
        d = r.json() if r.status_code == 200 else {}
        check(f"GET /api/projects/{slug}", r.status_code == 200 and d.get("slug") == slug,
              f"unit_types={len(d.get('unit_types') or [])} "
              f"amenities={len(d.get('amenities') or [])} "
              f"payment_plans={len(d.get('payment_plans') or [])}")

        r = c.get(f"/api/projects/{slug}/changes")
        check("GET project changes", r.status_code == 200, f"{len(r.json())} change(s)")

    # Use a developer that is actually in the developers table — a project's
    # developer_slug may not be ingested in a --limit run, and that correctly 404s.
    devs = c.get("/api/developers").json().get("items") or []
    if devs:
        dev = devs[0]["slug"]
        r = c.get(f"/api/developers/{dev}/projects")
        check("GET developer projects", r.status_code == 200,
              f"{dev}: {r.json().get('total')} project(s)")
        r = c.get("/api/developers/not-a-real-developer/projects")
        check("unknown developer projects -> 404", r.status_code == 404)

    r = c.get("/api/projects/does-not-exist-xyz")
    check("unknown slug -> 404", r.status_code == 404)

    print("\n--- catalog ---")
    for path in ("/api/developers", "/api/areas", "/api/brokers", "/api/articles"):
        r = c.get(path)
        check(f"GET {path}", r.status_code == 200, f"total={r.json().get('total')}")
    r = c.get("/api/pages")
    check("GET /api/pages", r.status_code == 200, f"{len(r.json())} page(s)")

    print("\n--- changes feed ---")
    r = c.get("/api/changes/recent")
    check("GET /api/changes/recent", r.status_code == 200, f"{len(r.json())} change(s)")
    r = c.get("/api/changes/recent", params={"field": "price_min_aed"})
    check("changes filtered by field", r.status_code == 200, f"{len(r.json())} price move(s)")

    print("\n--- admin auth ---")
    r = c.get("/api/admin/runs")
    check("admin without key -> 401", r.status_code == 401, str(r.status_code))
    r = c.get("/api/admin/runs", headers={"X-API-Key": "wrong"})
    check("admin with wrong key -> 401", r.status_code == 401)
    r = c.get("/api/admin/runs", headers=KEY)
    runs = r.json() if r.status_code == 200 else []
    check("GET /api/admin/runs with key", r.status_code == 200, f"{len(runs)} run(s)")

    if runs:
        rid = runs[0]["id"]
        r = c.get(f"/api/admin/runs/{rid}", headers=KEY)
        check(f"GET /api/admin/runs/{rid}", r.status_code == 200,
              f"status={r.json()['status']}")
    r = c.get("/api/admin/runs/999999", headers=KEY)
    check("unknown run -> 404", r.status_code == 404)

    r = c.post("/api/admin/scrape", headers=KEY, params={"only": ["bogus-entity"]})
    check("bad entity type -> 422", r.status_code == 422, str(r.status_code))

    print("\n--- openapi ---")
    r = c.get("/openapi.json")
    spec = r.json() if r.status_code == 200 else {}
    check("GET /openapi.json", r.status_code == 200,
          f"{len(spec.get('paths', {}))} documented paths")

print()
if failures:
    print(f"{len(failures)} check(s) FAILED: {', '.join(failures)}")
    sys.exit(1)
print("all API checks passed")
