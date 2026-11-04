"""Coverage report over a scrape output directory."""
import json, sys, os
from collections import Counter

d = sys.argv[1] if len(sys.argv) > 1 else "output"
load = lambda n: json.load(open(os.path.join(d, n), encoding="utf-8"))

P = load("projects.json")
D = load("developers.json")
A = load("areas.json")
n = len(P)
print(f"projects: {n}   parse_errors: {sum(1 for p in P if p.get('parse_error'))}")

def pct(f):
    c = sum(1 for p in P if f(p))
    return f"{c:>5}/{n}  {c*100//n:>3}%"

print("\n-- project field coverage --")
checks = {
    "name":              lambda p: bool(p.get("name")),
    "developer.slug":    lambda p: bool((p.get("developer") or {}).get("slug")),
    "area.slug":         lambda p: bool((p.get("area") or {}).get("slug")),
    "status":            lambda p: bool(p.get("status")),
    "price_aed.min":     lambda p: (p.get("price_aed") or {}).get("min") is not None,
    "bedrooms_raw":      lambda p: bool(p.get("bedrooms_raw")),
    "handover":          lambda p: bool(p.get("handover")),
    "size_sqft.min":     lambda p: (p.get("size_sqft") or {}).get("min") is not None,
    "aed_per_sqft.min":  lambda p: (p.get("aed_per_sqft") or {}).get("min") is not None,
    "units_total":       lambda p: p.get("units_total") is not None,
    "construction":      lambda p: bool(p.get("construction_status")),
    "readiness_percent": lambda p: p.get("readiness_percent") is not None,
    "service_charge":    lambda p: bool(p.get("service_charge")),
    "unit_types":        lambda p: bool(p.get("unit_types")),
    "payment_plans":     lambda p: bool(p.get("payment_plans")),
    "amenities":         lambda p: bool(p.get("amenities")),
    "gallery":           lambda p: bool(p.get("gallery")),
    "faqs":              lambda p: bool(p.get("faqs")),
    "sections":          lambda p: bool(p.get("sections")),
}
for k, f in checks.items():
    print(f"  {k:<20} {pct(f)}")

print("\n-- sanity --")
bad_price = [p["slug"] for p in P if (p.get("price_aed") or {}).get("min") is not None
             and (p["price_aed"]["min"] < 0 or p["price_aed"]["min"] > 5e9)]
bad_rng = [p["slug"] for p in P
           if (p.get("size_sqft") or {}).get("min") is not None
           and p["size_sqft"]["min"] > (p["size_sqft"]["max"] or 0)]
print(f"  implausible prices: {len(bad_price)} {bad_price[:4]}")
print(f"  inverted ranges:    {len(bad_rng)} {bad_rng[:4]}")
print(f"  status values:      {dict(Counter(p.get('status') for p in P))}")
print(f"  dup slugs:          {n - len(set(p['slug'] for p in P))}")

prices = sorted(p["price_aed"]["min"] for p in P if (p.get("price_aed") or {}).get("min"))
if prices:
    print(f"  price min/median/max AED: {prices[0]:,} / {prices[len(prices)//2]:,} / {prices[-1]:,}")

print("\n-- developers --")
print(f"  with snapshot projects_tracked: {sum(1 for x in D if (x.get('snapshot') or {}).get('projects_tracked') is not None)}/{len(D)}")
print(f"  with website:                   {sum(1 for x in D if x.get('website'))}/{len(D)}")
print(f"  with about:                     {sum(1 for x in D if x.get('about'))}/{len(D)}")
print(f"  with project list:              {sum(1 for x in D if x.get('projects'))}/{len(D)}")

print("\n-- areas --")
print(f"  with snapshot projects_tracked: {sum(1 for x in A if (x.get('snapshot') or {}).get('projects_tracked') is not None)}/{len(A)}")
print(f"  with developers_active:         {sum(1 for x in A if x.get('developers_active'))}/{len(A)}")
print(f"  truncated chip lists:           {sum(1 for x in A if any('_truncated_more' in c for c in x.get('developers_active') or []))}/{len(A)}")

print("\n-- top developers by project count (from cross_reference) --")
xr = load("cross_reference.json")["projects_by_developer"]
for k, v in sorted(xr.items(), key=lambda kv: -len(kv[1]))[:10]:
    print(f"  {k:<34} {len(v)}")
