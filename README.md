# api-scraper

FastAPI backend over scraped [maisondubai.com](https://maisondubai.com) (brand: *Casadior*) off-plan real-estate data, with a weekly refresh that writes **only what changed**.

**No AI at runtime.** The scraper is plain Python — `requests` + `BeautifulSoup` + regex. No Claude CLI on the server, no API key, no inference cost. Claude *wrote* this code; running it needs only `python`. That matters beyond cost: change detection hashes each record, so a non-deterministic step in the pipeline would make every record look changed on every run.

---

## What's in the dataset

| Entity | Count | Notes |
|---|---:|---|
| Projects | ~2,021 | Off-plan developments — the main dataset |
| Developers | ~450 | Profiles + market snapshot |
| Areas | ~134 | Area guides + developer breakdown |
| Brokers | ~18 | Contact profiles |
| Articles | ~27 | `/learn` guides |
| Pages | ~13 | Static marketing pages |

Each project carries price range, bedrooms, handover, size, AED/sqft, inventory, construction status + readiness %, service charge, furnishing, **unit types with per-bedroom price bands**, **payment-plan milestones**, amenities, gallery, FAQs and long-form descriptions.

Two things worth knowing about the source:

- **"Listings" are genuinely empty.** The site has two property concepts: `/projects/*` (2,021 off-plan developments — the real data) and `/listings` (owner-mandated resale/rental), which renders *"0 listings"* with an empty sitemap. The scraper handles that section and will pick listings up automatically if they ever publish any.
- **The data is Reelly-sourced.** Amenity icons serve from `reelly-backend.s3.amazonaws.com`, so this is effectively Reelly's Dubai off-plan dataset as Casadior republishes it.

---

## Quick start (local)

```bash
cd api-scraper
python -m venv .venv && .venv/Scripts/activate      # Windows
# python3 -m venv .venv && source .venv/bin/activate  # Linux

pip install -r requirements.txt
cp .env.example .env            # then edit it

# Local dev without Postgres: set DATABASE_URL=sqlite:///./maisondubai.db
alembic upgrade head

python -m app.cli scrape --limit 5   # smoke test: 5 pages per entity type
uvicorn app.main:app --reload        # http://localhost:8000/docs
```

On Windows the launcher is usually `py` rather than `python`.

---

## How the weekly refresh works

```
POST /api/admin/scrape                 (or: python -m app.cli scrape)
        │
        ├─ 1. open a scrape_runs row (status=running), return 202 + run_id
        ├─ 2. scrape ~2,650 pages  — ETag/If-None-Match, so unchanged pages
        │       cost a round-trip instead of ~280KB each
        ├─ 3. SAFETY RAILS — compare against the last good run; abort if wrong
        ├─ 4. ingest: hash each record, write only what changed
        └─ 5. close the run row with per-entity counts
```

### Only-what-changed, concretely

Each record is hashed (`sha256` over the canonical record, ignoring `scraped_at`):

| Condition | Action |
|---|---|
| slug not in DB | INSERT → `created` |
| slug in DB, hash identical | touch `last_seen_at` only → `unchanged` |
| slug in DB, hash differs | diff watched fields → write `record_changes` rows → UPDATE → `updated` |
| slug in DB, absent from scrape | `is_active=False`, `delisted_at=now` → `delisted` |

Records are **never hard-deleted**, so a project that goes out of stock and later returns keeps its history, and a partial scrape can't wipe rows.

Verified behaviour: running the identical scrape twice reports every record `unchanged` and writes **zero** change rows. Tamper with one price and the next run reports exactly `updated=1` plus one `record_changes` row with the old and new value.

### The safety rails — what replaces "put Claude on the server"

The parsers depend on maisondubai.com's HTML. When that changes, selectors stop matching and a scrape can "succeed" while returning mostly empty records. Writing that would quietly destroy the dataset. So **before any write**, the run compares itself to current DB state and refuses to proceed if it looks wrong:

| Rail | Trips when | Env var |
|---|---|---|
| `project_count_ratio` | scraped count < 80% of active rows | `GUARD_MIN_COUNT_RATIO` |
| `price_coverage_drop` | price coverage falls > 30 percentage points | `GUARD_MAX_COVERAGE_DROP` |
| `delist_ratio` | one run would delist > 20% of records | `GUARD_MAX_DELIST_RATIO` |
| `parse_errors` | any record failed to parse | — |

A tripped run is marked `aborted`, the reason is stored in `scrape_runs.validation`, and **the data is untouched**. It's threshold arithmetic over counts — no model involved.

**When a rail trips:** read `GET /api/admin/runs/{id}`, open a couple of the affected pages, and fix the selectors in `scraper/maisondubai.py` (that's a developer task — Claude Code on your laptop is great for it; it does not belong on the cron). Partial runs (`--limit` / `--only`) skip the count and delist rails so a smoke test can't damage real data.

---

## API

Interactive docs at `/docs`. 20 routes.

### Public reads

```
GET /health                          liveness + DB ping + last successful run
GET /api/stats                       counts, price distribution, top devs/areas
GET /api/changes/recent              what moved across the whole dataset
                                     ?entity= &field= &run_id= &limit=

GET /api/projects                    ?q= &developer= &area= &status=
                                     &price_min= &price_max= &bedrooms=
                                     &handover_year= &handover_before=
                                     &has_price= &construction_status=
                                     &sort=name|price|price_desc|handover|updated|sqft
                                     &page= &page_size=
GET /api/projects/{slug}             the full record
GET /api/projects/{slug}/changes     price/status history for one project

GET /api/developers    /{slug}   /{slug}/projects
GET /api/areas         /{slug}   /{slug}/projects
GET /api/brokers       /{slug}
GET /api/articles      /{slug}
GET /api/pages
```

List routes return `{items, total, page, page_size, pages}` and hide soft-deleted rows; add `?include_delisted=true` to see them. `bedrooms=0` means studio. Price filters match on **range overlap**, so `price_max=2000000` returns anything whose advertised range starts at or below 2M.

### Admin — requires `X-API-Key`

```
POST /api/admin/scrape      -> 202 {run_id, poll}   409 if one is already running
                               ?only=projects&only=areas   ?limit=5
GET  /api/admin/runs        -> history              ?status= &limit=
GET  /api/admin/runs/{id}   -> poll this after the 202
```

```bash
curl -X POST -H "X-API-Key: $ADMIN_API_KEY" https://api.example.com/api/admin/scrape
# {"run_id":12,"status":"running","poll":"/api/admin/runs/12"}
curl -H "X-API-Key: $ADMIN_API_KEY" https://api.example.com/api/admin/runs/12
```

The POST returns immediately rather than holding the connection for ~21 minutes.

**Why the key:** an anonymous trigger would let anyone launch a 2,662-page crawl at will, which also hammers maisondubai.com and risks your VPS IP being blocked. A single-flight lock means a second trigger during a run gets `409` with the in-flight `run_id` instead of starting a parallel crawl. `deploy/nginx.conf` also rate-limits that one path.

---

## CLI

```bash
python -m app.cli scrape                      # full scrape + ingest
python -m app.cli scrape --limit 5            # smoke test
python -m app.cli scrape --only projects      # one entity type (never delists)
python -m app.cli ingest                      # re-ingest output/*.json, no re-scrape
python -m app.cli ingest --force              # ...ignoring the safety rails
python -m app.cli runs                        # recent run history
python -m app.cli stats                       # row counts
python -m app.cli initdb                      # create tables without alembic
```

The scraper also still runs standalone and writes JSON only:

```bash
python scraper/maisondubai.py --revalidate            # full run
python scraper/maisondubai.py --only projects --limit 25
```

---

## Deploying to the VPS

```bash
sudo apt install python3-venv postgresql nginx
sudo -u postgres createuser scraper -P
sudo -u postgres createdb maisondubai -O scraper

sudo git clone <your-repo> /opt/api-scraper
cd /opt/api-scraper
python3 -m venv .venv && .venv/bin/pip install -r requirements.txt

cp .env.example .env
python3 -c "import secrets; print(secrets.token_urlsafe(32))"   # ADMIN_API_KEY
# set DATABASE_URL, ADMIN_API_KEY, CORS_ORIGINS (your frontend's origin)

.venv/bin/alembic upgrade head
.venv/bin/python -m app.cli scrape        # first full run, ~21 min

sudo cp deploy/maisondubai-api.service /etc/systemd/system/
sudo systemctl enable --now maisondubai-api
# nginx: see deploy/nginx.conf      cron: see deploy/crontab.example
```

**Run one uvicorn worker.** The scrape executes as a background task inside the API process behind an in-process lock; a second worker would hold its own lock and could start a parallel crawl. If you need more read throughput, put the scrape on the CLI/cron path (`deploy/crontab.example` option B) and then scale workers freely.

Disk: the HTML cache grows to ~576MB and `output/` to ~136MB. Both are gitignored. Delete `.cache_maisondubai/` any time — it only costs bandwidth on the next run.

---

## Layout

```
api-scraper/
├── app/
│   ├── main.py          FastAPI app, CORS, routers
│   ├── config.py        settings from .env
│   ├── db.py            engine + session
│   ├── models.py        SQLAlchemy ORM (8 tables)
│   ├── schemas.py       Pydantic response models
│   ├── ingest.py        hashing, field diffing, soft-delete
│   ├── jobs.py          scrape+ingest job, safety rails, lock
│   ├── cli.py           python -m app.cli
│   └── routers/         projects, catalog, meta, admin, common
├── scraper/
│   ├── maisondubai.py   the scraper (importable: run_scrape())
│   └── validate.py      standalone coverage report over output/*.json
├── migrations/          alembic
├── deploy/              systemd, nginx, crontab
├── tests_smoke.py       parsers, hashing, safety rails
└── tests_api.py         every route, in-process
```

### Schema notes

`projects` promotes the filterable fields into real columns — including `bedrooms_min/max` parsed out of `"Studio, 1, 2, 3"` and `handover_year/quarter` parsed out of `"Q4 2027"`, so those are actual indexed queries rather than string matching. Nested data (unit types, payment plans, amenities, gallery, FAQs) lives in JSONB.

`record_changes` is the field-level diff log — it makes *"which projects dropped their price this week"* a real query:

```sql
SELECT slug, old_value, new_value, changed_at
FROM record_changes
WHERE entity = 'projects' AND field = 'price_min_aed'
  AND CAST(new_value AS BIGINT) < CAST(old_value AS BIGINT)
ORDER BY changed_at DESC;
```

---

## Tests

```bash
python tests_smoke.py     # parsers, hash stability, every safety rail
python tests_api.py       # all 20 routes + auth + filtering, in-process
```

Both run against the configured `DATABASE_URL`. `tests_smoke.py` is read-only; `tests_api.py` only reads.

---

## Known limits

- **Price coverage is ~73%** — the missing ones are `out of stock` projects the site itself shows as "Price on request". `price_on_request` is the authoritative flag; upstream uses a literal `1` as its placeholder, which the ingest refuses to promote into `price_min_aed`.
- **Area coverage is ~92%** — some projects genuinely have no area link on the site (e.g. `241-waterside` is only labelled "Dubai").
- **No geo coordinates** — the pages don't render lat/long anywhere. `address` carries locality/region/country from JSON-LD only.
- **Developer/area chip lists are UI-truncated** (`+24 more`). The complete mapping is in `output/cross_reference.json`, or just query `projects` grouped by `developer_slug` / `area_slug`.
- **Prices change daily** — the site refreshes from live developer inventory, so treat any snapshot as of its run date.

## On reuse

This pulls publicly served, indexable pages at a polite rate and honours `robots.txt` (`/api/`, `/admin/`, `/dashboard/`, `/portal/`, `/auth/`, `/login` are never touched — enforced in `Fetcher.get`). The content is still Casadior's/Reelly's copyright, and Reelly licenses this data commercially. Internal research is one thing; republishing is another — check their terms, and if you need this as a product input, licensing Reelly directly will be cheaper than a legal problem.
