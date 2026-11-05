#!/usr/bin/env python3
"""
maisondubai.com (Casadior) scraper -> JSON

The site is a Next.js App Router app on Vercel. There is no public JSON API
(robots.txt disallows /api/), so this scraper works entirely from the public,
indexable surface the site itself advertises:

  * /sitemap.xml  -> per-entity sitemaps (projects, developers, areas, brokers, learn, pages)
  * server-rendered HTML of each page
  * embedded JSON-LD blocks (<script type="application/ld+json">)
  * the React Server Component "flight" payload (self.__next_f.push([1,"..."]))
    which carries unit types, payment-plan milestones, amenities and gallery media

Entities produced:
  projects    ~2,021  off-plan developments (price, beds, handover, size, sqft rate,
                      inventory, construction status, payment plans, unit types,
                      amenities, gallery, FAQ, descriptions, developer + area links)
  developers  ~450    developer profiles + market snapshot + their project list
  areas       ~134    area guides + market snapshot + developer breakdown
  brokers     ~18     broker profiles
  learn       ~27     guide/blog articles
  listings    live    direct owner-mandate listings (empty at time of writing)

Usage
-----
  pip install requests beautifulsoup4 lxml

  python maisondubai_scraper.py                      # everything
  python maisondubai_scraper.py --only projects      # one entity type
  python maisondubai_scraper.py --limit 25           # smoke test (25 per type)
  python maisondubai_scraper.py --workers 4 --delay 0.5
  python maisondubai_scraper.py --out ./data --no-cache

Output (in --out, default ./output):
  projects.json  developers.json  areas.json  brokers.json  learn.json
  listings.json  pages.json  maisondubai_all.json  _run_meta.json

Be considerate: this hits ~2,700 pages. The defaults (6 workers, 0.3s jitter,
on-disk cache, retries with backoff) are deliberately gentle. Scraped content
remains the copyright of its owner -- check their terms before redistributing.
"""

from __future__ import annotations

import argparse
import concurrent.futures as cf
import hashlib
import json
import os
import random
import re
import sys
import threading
import time
from datetime import datetime, timezone
from typing import Any, Callable, Iterable
from urllib.parse import urljoin, urlparse
from xml.etree import ElementTree as ET

import requests
from bs4 import BeautifulSoup

BASE = "https://maisondubai.com"
SITEMAP_INDEX = f"{BASE}/sitemap.xml"

UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
)

# Chrome-ish headers; the site is a plain SSR Next.js app and does not gate on these,
# but sending a coherent set avoids tripping generic bot heuristics on Vercel's edge.
HEADERS = {
    "User-Agent": UA,
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Cache-Control": "no-cache",
}

# robots.txt Disallow list -- we honour it.
ROBOTS_DISALLOW = ("/dashboard/", "/admin/", "/api/", "/login", "/r/", "/portal/", "/auth/")

NAV_NOISE = {
    "Properties", "Listings", "Developers", "Areas", "FAQ", "Learn", "Team",
    "Careers", "Browse properties", "Book a call", "Home", "Share", "Inquire",
    "Brochure", "All projects",
}


# ---------------------------------------------------------------------------
# fetching
# ---------------------------------------------------------------------------
class Fetcher:
    """Polite, cached, retrying HTTP getter shared across worker threads."""

    def __init__(self, cache_dir: str | None, delay: float = 0.3,
                 retries: int = 3, timeout: int = 30, revalidate: bool = False):
        self.cache_dir = cache_dir
        self.delay = delay
        self.retries = retries
        self.timeout = timeout
        # revalidate: still use the cache, but ask the origin whether it changed
        # (If-None-Match). A 304 costs a round-trip instead of ~280KB of HTML.
        self.revalidate = revalidate
        self._local = threading.local()
        self.stats = {"cache": 0, "net": 0, "fail": 0, "not_modified": 0}
        self._lock = threading.Lock()
        if cache_dir:
            os.makedirs(cache_dir, exist_ok=True)

    @property
    def session(self) -> requests.Session:
        # one Session per thread: connection pooling without cross-thread sharing
        s = getattr(self._local, "session", None)
        if s is None:
            s = requests.Session()
            s.headers.update(HEADERS)
            self._local.session = s
        return s

    def _cache_path(self, url: str) -> str | None:
        if not self.cache_dir:
            return None
        key = hashlib.sha1(url.encode()).hexdigest()[:20]
        slug = re.sub(r"[^a-zA-Z0-9]+", "_", urlparse(url).path).strip("_")[:70] or "root"
        return os.path.join(self.cache_dir, f"{slug}.{key}.html")

    def get(self, url: str) -> str | None:
        if any(urlparse(url).path.startswith(p) for p in ROBOTS_DISALLOW):
            return None

        cp = self._cache_path(url)
        cached = None
        if cp and os.path.exists(cp) and os.path.getsize(cp) > 500:
            with open(cp, encoding="utf-8") as fh:
                cached = fh.read()
            if not self.revalidate:
                with self._lock:
                    self.stats["cache"] += 1
                return cached

        validators = self._load_validators(cp) if cached else {}

        for attempt in range(self.retries):
            try:
                if self.delay:
                    time.sleep(self.delay * random.uniform(0.6, 1.5))
                headers = {}
                if cached and validators.get("etag"):
                    headers["If-None-Match"] = validators["etag"]
                if cached and validators.get("last_modified"):
                    headers["If-Modified-Since"] = validators["last_modified"]

                r = self.session.get(url, timeout=self.timeout,
                                     headers=headers or None)

                if r.status_code == 304 and cached:
                    with self._lock:
                        self.stats["not_modified"] += 1
                    return cached

                if r.status_code == 200:
                    r.encoding = r.encoding or "utf-8"
                    html = r.text
                    if cp:
                        with open(cp, "w", encoding="utf-8") as fh:
                            fh.write(html)
                        self._save_validators(cp, r.headers)
                    with self._lock:
                        self.stats["net"] += 1
                    return html

                if r.status_code in (404, 410):
                    break
                if r.status_code in (429, 500, 502, 503, 504):
                    time.sleep(2 ** attempt + random.random() * 2)
                    continue
                break
            except requests.RequestException:
                time.sleep(1.5 * (attempt + 1))

        if cached:  # a failed revalidation should not lose data we already hold
            with self._lock:
                self.stats["cache"] += 1
            return cached

        with self._lock:
            self.stats["fail"] += 1
        return None

    @staticmethod
    def _load_validators(cache_path: str | None) -> dict:
        if not cache_path:
            return {}
        try:
            with open(cache_path + ".meta.json", encoding="utf-8") as fh:
                return json.load(fh)
        except (OSError, json.JSONDecodeError):
            return {}

    @staticmethod
    def _save_validators(cache_path: str, headers) -> None:
        meta = {"etag": headers.get("ETag"),
                "last_modified": headers.get("Last-Modified"),
                "fetched_at": datetime.now(timezone.utc).isoformat()}
        if meta["etag"] or meta["last_modified"]:
            try:
                with open(cache_path + ".meta.json", "w", encoding="utf-8") as fh:
                    json.dump(meta, fh)
            except OSError:
                pass


# ---------------------------------------------------------------------------
# generic HTML / payload helpers
# ---------------------------------------------------------------------------
_TAG_RE = re.compile(r"<[^>]+>")
_WS_RE = re.compile(r"[ \t\r\f\v]+")
_NBSP = "\xa0"

# matches the JS string literal inside self.__next_f.push([1,"...."])
FLIGHT_RE = re.compile(r'self\.__next_f\.push\(\[1,\s*("(?:\\.|[^"\\])*")')

# No leading '-': ranges render as "391-1743" and a signed match would read -1743.
MONEY_RE = re.compile(r"\d[\d,]*\.?\d*")

STATUS_WORDS = {"on sale", "presale", "out of stock", "sold out", "completed", "announced"}


def soup_of(html: str) -> BeautifulSoup:
    return BeautifulSoup(html, "lxml")


def clean(s: Any) -> str:
    """Strip tags/entities/comments from a fragment and normalise whitespace."""
    if s is None:
        return ""
    if not isinstance(s, str):
        s = str(s)
    s = s.replace("<!-- -->", " ")
    s = _TAG_RE.sub(" ", s)
    s = BeautifulSoup(s, "lxml").get_text(" ")
    s = s.replace(_NBSP, " ")
    return _WS_RE.sub(" ", s).strip()


def num(s: Any) -> float | int | None:
    """First number in a string -> int/float. '658,589-2,148,899' -> 658589."""
    if s is None:
        return None
    m = MONEY_RE.search(str(s).replace(_NBSP, " "))
    if not m:
        return None
    try:
        v = float(m.group(0).replace(",", ""))
    except ValueError:
        return None
    return int(v) if v.is_integer() else v


_SUFFIX = {"k": 1_000, "m": 1_000_000, "b": 1_000_000_000}


def money(s: Any) -> float | int | None:
    """Expand abbreviated money: 'AED 1.19M' -> 1190000, 'AED 728K' -> 728000."""
    if s is None:
        return None
    txt = str(s).replace(_NBSP, " ")
    m = re.search(r"(\d[\d,]*\.?\d*)\s*([KkMmBb])\b", txt)
    if m:
        base = num(m.group(1))
        if base is not None:
            v = base * _SUFFIX[m.group(2).lower()]
            return int(v) if float(v).is_integer() else v
    return num(txt)


def num_range(s: Any) -> dict[str, float | int | None]:
    """'391-1743' / 'AED 1,192,904-AED 2,877,948' -> {'min':…, 'max':…, 'raw':…}."""
    raw = clean(s)
    if not raw:
        return {"min": None, "max": None, "raw": None}
    nums = [n for n in (num(x) for x in MONEY_RE.findall(raw.replace(_NBSP, " "))) if n is not None]
    if not nums:
        return {"min": None, "max": None, "raw": raw}
    return {"min": min(nums), "max": max(nums), "raw": raw}


def json_ld(html: str) -> list[dict]:
    """All JSON-LD objects, @graph flattened."""
    out: list[dict] = []
    for block in re.findall(
        r'<script[^>]*type=["\']application/ld\+json["\'][^>]*>(.*?)</script>', html, re.S
    ):
        try:
            data = json.loads(block.strip())
        except json.JSONDecodeError:
            continue
        for item in data if isinstance(data, list) else [data]:
            if not isinstance(item, dict):
                continue
            graph = item.get("@graph")
            out.extend(g for g in graph if isinstance(g, dict)) if isinstance(graph, list) else out.append(item)
    return out


def ld_of_type(blocks: Iterable[dict], *types: str) -> dict | None:
    want = {t.lower() for t in types}
    for b in blocks:
        t = b.get("@type")
        tl = {x.lower() for x in (t if isinstance(t, list) else [t]) if isinstance(x, str)}
        if tl & want:
            return b
    return None


def flight_payload(html: str) -> str:
    """Re-assemble the RSC flight stream from its chunked script pushes."""
    parts = []
    for lit in FLIGHT_RE.findall(html):
        try:
            parts.append(json.loads(lit))
        except json.JSONDecodeError:
            continue
    return "".join(parts)


def flight_objects(payload: str, needle: str, limit: int = 12) -> list[dict | list]:
    """
    Pull balanced JSON values out of the flight stream by key name.

    The flight stream is not valid JSON as a whole (it is `id:value` rows of
    React element trees), so we locate `needle` and expand outward to the
    nearest enclosing balanced {...} / [...] that parses.
    """
    found: list[dict | list] = []
    seen: set[str] = set()
    pos = 0
    while len(found) < limit:
        k = payload.find(needle, pos)
        if k == -1:
            break
        pos = k + len(needle)
        i = k
        for _ in range(80):  # widen the candidate start a bounded number of times
            i = max(payload.rfind("{", 0, i), payload.rfind("[", 0, i))
            if i <= 0:
                break
            end = _match_bracket(payload, i)
            if end and end > k:
                frag = payload[i:end + 1]
                try:
                    val = json.loads(frag)
                except json.JSONDecodeError:
                    continue
                h = hashlib.sha1(frag.encode()).hexdigest()
                if h not in seen:
                    seen.add(h)
                    found.append(val)
                break
    return found


def flight_array(payload: str, key: str, require: str | None = None) -> list:
    """
    Parse the array literal assigned to `"key":[ ... ]` in the flight stream.

    More reliable than expanding outward from a nested needle, which lands on the
    innermost object rather than the collection. `require` filters to the
    occurrence whose text contains that substring.
    """
    needle = f'"{key}":['
    pos = 0
    while True:
        k = payload.find(needle, pos)
        if k == -1:
            return []
        start = k + len(needle) - 1  # index of '['
        pos = k + len(needle)
        end = _match_bracket(payload, start)
        if not end:
            continue
        frag = payload[start:end + 1]
        if require and require not in frag:
            continue
        try:
            val = json.loads(frag)
        except json.JSONDecodeError:
            continue
        if isinstance(val, list) and val:
            return val


def _match_bracket(s: str, start: int, cap: int = 600_000) -> int | None:
    """Index of the bracket closing the one at `start`, respecting strings."""
    open_c = s[start]
    close_c = "}" if open_c == "{" else "]"
    depth = 0
    in_str = False
    esc = False
    for j in range(start, min(len(s), start + cap)):
        c = s[j]
        if esc:
            esc = False
            continue
        if c == "\\":
            esc = True
            continue
        if c == '"':
            in_str = not in_str
            continue
        if in_str:
            continue
        if c == open_c:
            depth += 1
        elif c == close_c:
            depth -= 1
            if depth == 0:
                return j
    return None


def _label_text(p) -> str:
    """
    Label text only, dropping the nested glossary-tooltip <a>/<svg>/<span>.
    Regex across this markup mis-joins adjacent cells, so walk the DOM instead.
    """
    direct = "".join(s for s in p.find_all(string=True, recursive=False))
    txt = clean(direct) or clean(p.get_text(" "))
    return txt.strip(" :·")


def _is_control(node) -> bool:
    """Currency <select> widget and sr-only labels are chrome, not values."""
    if node.name in ("select", "option", "label", "svg"):
        return True
    if node.find(["select", "option"]):
        return True
    cls = " ".join(node.get("class") or [])
    return "sr-only" in cls


def facts(soup: BeautifulSoup) -> dict[str, str]:
    """
    The uppercase label / value spec grid on project, developer and area pages:
      <div><p class="...uppercase tracking-wider...">LABEL</p><p>VALUE</p></div>
    The price cell nests its value one level deeper beside the currency switcher,
    so fall back to scanning the enclosing cell, preferring numeric-styled nodes.
    """
    out: dict[str, str] = {}

    # the market-snapshot strips use a <dl><dt>label</dt><dd>value</dd></dl>
    for dt in soup.select("dl dt"):
        label = _label_text(dt)
        dd = dt.find_next_sibling("dd")
        if label and dd and len(label) <= 60:
            val = clean(dd.get_text(" "))
            if val:
                out.setdefault(label, val)

    for p in soup.select('p[class*="uppercase"][class*="tracking-wider"]'):
        label = _label_text(p)
        if not label or label in NAV_NOISE or len(label) > 60:
            continue

        value = ""
        sib = p.find_next_sibling("p")
        if sib and not _is_control(sib):
            value = clean(sib.get_text(" "))

        if not value:
            holder = p.parent
            for _ in range(3):  # label cell -> flex row -> grid cell
                if holder is None:
                    break
                cands = []
                for n in holder.find_all(["p", "span", "div"]):
                    if n is p or p in n.parents or _is_control(n):
                        continue
                    t = clean(n.get_text(" "))
                    if not t or t == label or t.lower() == "currency":
                        continue
                    cls = " ".join(n.get("class") or [])
                    rank = 0 if ("tabular-nums" in cls or "font-medium" in cls) else 1
                    cands.append((rank, len(t), t))
                if cands:
                    cands.sort(key=lambda c: (c[0], c[1]))
                    value = cands[0][2]
                    break
                holder = holder.parent

        if value:
            out.setdefault(label, value)
    return out


def breadcrumbs(blocks: Iterable[dict]) -> list[str]:
    bc = ld_of_type(blocks, "BreadcrumbList")
    if not bc:
        return []
    items = bc.get("itemListElement") or []
    names = []
    for it in items:
        if isinstance(it, dict):
            n = it.get("name") or (it.get("item") or {}).get("name") if isinstance(it.get("item"), dict) else it.get("name")
            if n:
                names.append(clean(n))
    return names


def faqs(blocks: Iterable[dict], html: str) -> list[dict]:
    out = []
    page = ld_of_type(blocks, "FAQPage")
    for q in (page or {}).get("mainEntity", []) or []:
        if not isinstance(q, dict):
            continue
        ans = q.get("acceptedAnswer") or {}
        out.append({
            "question": clean(q.get("name")),
            "answer": clean(ans.get("text") if isinstance(ans, dict) else ans),
        })
    if out:
        return out
    # fallback: <details><summary>Q</summary><p>A</p></details>
    for d in soup_of(html).select("details"):
        s = d.find("summary")
        if not s:
            continue
        body = " ".join(clean(p.get_text(" ")) for p in d.find_all(["p", "div"], recursive=True))
        q = clean(s.get_text(" ")).rstrip("+").strip()
        if q:
            out.append({"question": q, "answer": body.strip()})
    return out


# Page chrome that rides along with every <img> scrape, not property photos:
# the site's own logo/brand assets, and tracking pixels from third-party
# analytics. Blocklisted by domain rather than allowlisted, since the real
# photo hosts (reelly-backend.s3.amazonaws.com, api.reelly.io, and possibly
# others not yet seen) shouldn't need to be enumerated up front.
_GALLERY_SKIP_DOMAINS = (
    "facebook.com", "facebook.net", "google-analytics.com", "googletagmanager.com",
    "doubleclick.net", "linkedin.com", "tiktok.com", "bing.com", "clarity.ms",
    "hotjar.com", "snapchat.com", "pinterest.com", "twitter.com", "x.com",
)


def gallery(html: str, soup: BeautifulSoup) -> list[dict]:
    """Project/area imagery, de-duplicated by URL, with page chrome filtered out."""
    seen, imgs = set(), []
    for img in soup.select("img[src], img[srcset]"):
        src = img.get("src") or ""
        if "/_next/image" in src:  # unwrap Next.js optimiser to the origin asset
            m = re.search(r"url=([^&]+)", src)
            if m:
                from urllib.parse import unquote
                src = unquote(m.group(1))
        if not src or src.startswith("data:"):
            continue
        src = urljoin(BASE, src)
        if src in seen:
            continue
        seen.add(src)

        host = urlparse(src).netloc.lower()
        if any(host == d or host.endswith("." + d) for d in _GALLERY_SKIP_DOMAINS):
            continue
        if host == urlparse(BASE).netloc.lower():
            # own-domain images on a project/area page are nav/brand chrome
            # (logo, favicon, OG placeholder) -- never a property photo
            continue

        width, height = num(img.get("width")), num(img.get("height"))
        if width is not None and height is not None and width <= 2 and height <= 2:
            continue  # 1x1-style tracking pixel sized via HTML attrs

        imgs.append({
            "url": src,
            "alt": clean(img.get("alt")) or None,
            "width": width,
            "height": height,
        })
    return imgs


def project_documents(soup: BeautifulSoup) -> dict[str, str | None]:
    """
    Brochure + floor-plan PDF links.

    Every project renders both as plain <a href="....pdf"> links: a compact
    header "Brochure" button (icon + the word "Brochure"), a larger
    "Download brochure (PDF)" button further down, and a "Floor plan -
    <name>.pdf" link in the facts panel. The brochure link is duplicated
    (same URL, two buttons) -- the "brochure" label text is what tells the
    two document types apart; neither project publishes both without that
    distinguishing text. Either can be absent on a project that hasn't
    published that asset yet.
    """
    brochure_url = None
    floor_plan_url = None
    for a in soup.select('a[href*=".pdf"]'):
        href = urljoin(BASE, a["href"])
        label = a.get_text(" ", strip=True)
        if re.search(r"brochure", label, re.I):
            brochure_url = brochure_url or href
        elif floor_plan_url is None:
            floor_plan_url = href
    return {"brochure_url": brochure_url, "floor_plan_url": floor_plan_url}


def sections(soup: BeautifulSoup) -> list[dict]:
    """Heading -> prose, for the long-form About / location / infrastructure copy."""
    out = []
    for h in soup.select("h2, h3"):
        title = clean(h.get_text(" "))
        if not title or title in NAV_NOISE:
            continue
        body = []
        for sib in h.find_all_next():
            if sib.name in ("h2", "h3"):
                break
            if sib.name == "p":
                t = clean(sib.get_text(" "))
                if t and len(t) > 40:
                    body.append(t)
            if len(body) >= 8:
                break
        if body:
            out.append({"heading": title, "body": "\n\n".join(body)})
    return out


def counted_chips(soup: BeautifulSoup, heading_re: str) -> list[dict]:
    """
    'Where LMD builds: Jebel Ali Village 2 ...' / 'Developers building in X: ALDAR 13 ...'
    Rendered as a link + count chip; pull the pairs that follow the matching heading.
    """
    pat = re.compile(heading_re, re.I)
    for h in soup.select("h2, h3"):
        if not pat.search(clean(h.get_text(" "))):
            continue
        out, seen = [], set()
        for a in h.find_all_next("a", href=True, limit=80):
            href = a["href"]
            if not re.match(r"^/(areas|developers)/", href):
                continue
            label = clean(a.get_text(" "))
            cnt = num(re.sub(r"^.*?(\d+)\s*$", r"\1", label)) if re.search(r"\d\s*$", label) else None
            name = re.sub(r"\s*\d+\s*$", "", label).strip()
            if not name or name in seen:
                continue
            seen.add(name)
            out.append({"name": name, "slug": href.rstrip("/").split("/")[-1],
                        "url": urljoin(BASE, href), "project_count": cnt})
            if len(out) >= 60:
                break
        if out:
            # the UI caps the chip list and renders a "+24 more" toggle; record the
            # overflow so consumers know the list is partial. The authoritative
            # mapping is derivable from projects.json (each project names both).
            tail = clean(h.parent.get_text(" ") if h.parent else "")
            m = re.search(r"\+\s*(\d+)\s*more", tail)
            if m:
                out.append({"_truncated_more": int(m.group(1))})
            return out
    return []


def linked_projects(soup: BeautifulSoup) -> list[dict]:
    """Project cards linked from a developer / area page."""
    out, seen = [], set()
    for a in soup.select('a[href^="/projects/"]'):
        href = a["href"].split("?")[0].rstrip("/")
        slug = href.split("/")[-1]
        if not slug or slug in seen:
            continue
        txt = clean(a.get_text(" "))
        if not txt:
            continue
        seen.add(slug)
        price = next((p for p in re.findall(r"AED[\s\xa0]*[\d,]+(?:\s*-\s*AED[\s\xa0]*[\d,]+)?", txt)), None)
        out.append({
            "slug": slug,
            "url": urljoin(BASE, href),
            "card_text": txt,
            "price_raw": clean(price) if price else ("Price on request" if "Price on request" in txt else None),
        })
    return out


def meta_tags(soup: BeautifulSoup) -> dict:
    g = lambda sel, at: (soup.select_one(sel) or {}).get(at) if soup.select_one(sel) else None
    return {
        "title": clean(soup.title.get_text()) if soup.title else None,
        "description": g('meta[name="description"]', "content"),
        "canonical": g('link[rel="canonical"]', "href"),
        "og_title": g('meta[property="og:title"]', "content"),
        "og_description": g('meta[property="og:description"]', "content"),
        "og_image": g('meta[property="og:image"]', "content"),
    }


# ---------------------------------------------------------------------------
# entity parsers
# ---------------------------------------------------------------------------
def parse_project(url: str, html: str) -> dict:
    soup = soup_of(html)
    ld = json_ld(html)
    fx = facts(soup)
    pay = flight_payload(html)
    slug = url.rstrip("/").split("/")[-1]

    listing = ld_of_type(ld, "RealEstateListing", "Product", "Residence") or {}
    offers = listing.get("offers") or {}
    if isinstance(offers, list):
        offers = offers[0] if offers else {}
    addr = listing.get("address") or {}

    crumbs = breadcrumbs(ld)

    # status badge sits inside the h1 block ("09 Life Residences" + "on sale")
    status = next(
        (clean(s.get_text(" ")) for s in soup.select("span, div, p")
         if clean(s.get_text(" ")).lower() in STATUS_WORDS),
        None,
    )

    h1 = soup.select_one("h1")
    name = clean(h1.get_text(" ")) if h1 else ""
    if status and name.lower().endswith(status.lower()):
        name = name[: -len(status)].strip(" ·-–,")
    name = name or clean(listing.get("name")) or (crumbs[-1] if crumbs else slug)

    # developer + area come from the only /developers/ and /areas/ links in the header block
    dev = soup.select_one('a[href^="/developers/"]')
    area = soup.select_one('a[href^="/areas/"]')

    # --- flight-embedded structures -------------------------------------
    unit_types = flight_array(pay, "unitTypes", require="fromPriceAED")
    payment_plans = flight_array(pay, "plans", require="stage_type")

    # amenities arrive as [{"id":…,"amenity":{"name":"Swimming Pool","icon":{…}}}]
    amenities: list[dict] = []
    for row in flight_array(pay, "amenities", require='"amenity"'):
        if not isinstance(row, dict):
            continue
        a = row.get("amenity") if isinstance(row.get("amenity"), dict) else row
        nm = clean(a.get("name"))
        if not nm:
            continue
        icon = a.get("icon") or {}
        amenities.append({
            "name": nm,
            "id": a.get("id"),
            "icon": icon.get("url") if isinstance(icon, dict) else None,
        })

    price_raw = fx.get("Price (AED)") or fx.get("Price") or ""
    on_request = bool(re.search(r"on request", price_raw, re.I))

    price = num_range(price_raw)
    # Unit types back-fill the price when the grid cell is unparsed, but upstream
    # (Reelly) uses a literal 1 as its "on request" placeholder -- never promote that.
    if price["min"] is None and unit_types and not on_request:
        plausible = lambda v: isinstance(v, (int, float)) and v > 1
        lo = [u["fromPriceAED"] for u in unit_types if plausible(u.get("fromPriceAED"))]
        hi = [u["toPriceAED"] for u in unit_types if plausible(u.get("toPriceAED"))]
        if lo or hi:
            price = {"min": min(lo) if lo else None,
                     "max": max(hi) if hi else None,
                     "raw": price.get("raw") or clean(offers.get("price")) or None}

    def drop_zero(r: dict) -> dict:
        """'Size (sqft)': '0' means unpublished, not a zero-sized unit."""
        if r.get("min") in (0, None) and r.get("max") in (0, None):
            return {"min": None, "max": None, "raw": r.get("raw")}
        return r

    size = drop_zero(num_range(fx.get("Size (sqft)")))
    sqft_rate = drop_zero(num_range(fx.get("AED / sqft")))

    return {
        "entity": "project",
        "slug": slug,
        "url": url,
        "name": name,
        "status": status,
        "developer": {
            "name": clean(dev.get_text(" ")) if dev else None,
            "slug": dev["href"].rstrip("/").split("/")[-1] if dev else None,
            "url": urljoin(BASE, dev["href"]) if dev else None,
        },
        "area": {
            "name": clean(area.get_text(" ")) if area else None,
            "slug": area["href"].rstrip("/").split("/")[-1] if area else None,
            "url": urljoin(BASE, area["href"]) if area else None,
        },
        "breadcrumbs": crumbs,
        "price_aed": price,
        "price_on_request": on_request,
        "price_usd_approx": clean(
            (soup.find(string=re.compile(r"USD\s*[\d,]+")) or "")
        ) or None,
        "bedrooms_raw": fx.get("Bedrooms"),
        "handover": fx.get("Handover"),
        "size_sqft": size,
        "aed_per_sqft": sqft_rate,
        "inventory_raw": fx.get("Inventory"),
        "units_total": num(fx.get("Inventory")),
        "construction_status": fx.get("Construction"),
        "readiness_percent": num(fx.get("Readiness")),
        "service_charge": fx.get("Service charge"),
        "furnishing": fx.get("Furnishing"),
        "post_handover_plan": fx.get("Post-handover plan"),
        "all_facts": fx,
        "unit_types": unit_types,
        "payment_plans": payment_plans,
        "amenities": amenities,
        "sections": sections(soup),
        "faqs": faqs(ld, html),
        "gallery": gallery(html, soup),
        **project_documents(soup),
        "address": {
            "locality": clean(addr.get("addressLocality")) or None,
            "region": clean(addr.get("addressRegion")) or None,
            "country": clean(addr.get("addressCountry")) or None,
        } if addr else {},
        "meta": meta_tags(soup),
        "json_ld": ld,
        "scraped_at": datetime.now(timezone.utc).isoformat(),
    }


def _snapshot(soup: BeautifulSoup) -> dict:
    """The shared 'market snapshot' stat strip on developer & area pages."""
    fx = facts(soup)
    snap = next(
        (clean(p.get_text(" ")) for p in soup.select("p")
         if "Casadior tracks" in clean(p.get_text(" "))),
        None,
    )
    hand = next(
        (clean(p.get_text(" ")) for p in soup.select("p")
         if re.search(r"Handovers (by|in) ", clean(p.get_text(" ")))),
        None,
    )
    return {
        "projects_tracked": num(fx.get("Off-plan projects")),
        "price_from_raw": fx.get("From"),
        "price_from_aed": money(fx.get("From")),
        "median_per_sqft_raw": fx.get("Median / sqft"),
        "median_per_sqft_aed": money(fx.get("Median / sqft")),
        "bedrooms_raw": fx.get("Bedrooms"),
        "handover_raw": fx.get("Handover"),
        "summary": snap,
        "handover_summary": hand,
    }


def parse_developer(url: str, html: str) -> dict:
    soup = soup_of(html)
    ld = json_ld(html)
    slug = url.rstrip("/").split("/")[-1]
    h1 = soup.select_one("h1")

    website = next(
        (a["href"] for a in soup.select("a[href^=http]")
         if "maisondubai.com" not in a["href"]
         and re.search(r"\.(com|ae|io|net|org)", a["href"])
         and len(clean(a.get_text(" "))) > 3
         and not re.search(r"(facebook|instagram|linkedin|twitter|x\.com|youtube|tiktok|wa\.me|whatsapp|mailto|tel:)", a["href"], re.I)),
        None,
    )

    # the site footer carries "Dubai, UAE · RERA ORN 41941" -- that is Casadior's
    # own registration, not the developer's office, so exclude it.
    offices = []
    for node in soup.select("p, li, address"):
        t = clean(node.get_text(" "))
        if not (25 < len(t) < 300):
            continue
        if re.search(r"RERA ORN|All rights reserved", t, re.I):
            continue
        if re.search(r"(Office|Offices|Floor|floor|Tower|P\.?O\.? Box)", t) and "Dubai" in t:
            offices.append(t)

    about = next(
        (s["body"] for s in sections(soup) if re.search(r"^About", s["heading"], re.I)),
        None,
    )

    # The developer's own logo is the first non-chrome image on the page --
    # it renders in the profile header, ahead of any project photo cards.
    _imgs = gallery(html, soup)
    logo_url = _imgs[0]["url"] if _imgs else None

    return {
        "entity": "developer",
        "slug": slug,
        "url": url,
        "name": clean(h1.get_text(" ")) if h1 else slug,
        "website": website,
        "logo_url": logo_url,
        "snapshot": _snapshot(soup),
        "areas_active_in": counted_chips(soup, r"where .* build|areas"),
        "about": about,
        "offices": sorted(set(offices))[:6],
        "projects": linked_projects(soup),
        "sections": sections(soup),
        "faqs": faqs(ld, html),
        "meta": meta_tags(soup),
        "json_ld": ld,
        "scraped_at": datetime.now(timezone.utc).isoformat(),
    }


def parse_area(url: str, html: str) -> dict:
    soup = soup_of(html)
    ld = json_ld(html)
    slug = url.rstrip("/").split("/")[-1]
    h1 = soup.select_one("h1")

    intro = None
    if h1:
        p = h1.find_next("p")
        if p:
            t = clean(p.get_text(" "))
            intro = t if len(t) > 30 else None

    return {
        "entity": "area",
        "slug": slug,
        "url": url,
        "name": clean(h1.get_text(" ")) if h1 else slug,
        "intro": intro,
        "snapshot": _snapshot(soup),
        "developers_active": counted_chips(soup, r"developers building"),
        "sections": sections(soup),
        "projects": linked_projects(soup),
        "faqs": faqs(ld, html),
        "gallery": gallery(html, soup),
        "meta": meta_tags(soup),
        "json_ld": ld,
        "scraped_at": datetime.now(timezone.utc).isoformat(),
    }


def parse_broker(url: str, html: str) -> dict:
    soup = soup_of(html)
    ld = json_ld(html)
    slug = url.rstrip("/").split("/")[-1]
    h1 = soup.select_one("h1")
    person = ld_of_type(ld, "Person", "RealEstateAgent") or {}

    emails = sorted({a["href"][7:] for a in soup.select('a[href^="mailto:"]')})
    phones = sorted({a["href"][4:] for a in soup.select('a[href^="tel:"]')})
    # every page carries the agency's own header/footer contacts; split them out
    SITE_EMAIL, SITE_PHONES = "info@maisondubai.com", {"+15146598892", "+971585524299"}
    own_emails = [e for e in emails if e.lower() != SITE_EMAIL]
    own_phones = [p for p in phones if p.replace(" ", "") not in SITE_PHONES]
    socials = sorted({
        a["href"] for a in soup.select("a[href^=http]")
        if re.search(r"(linkedin|instagram|facebook|twitter|x\.com|wa\.me|whatsapp)", a["href"], re.I)
    })

    role = None
    if h1:
        nxt = h1.find_next(["p", "span"])
        if nxt:
            t = clean(nxt.get_text(" "))
            role = t if 0 < len(t) < 90 else None

    # Not every broker has uploaded a headshot -- first non-chrome image when
    # present, None otherwise (confirmed against real pages, not a bug).
    _imgs = gallery(html, soup)
    photo_url = _imgs[0]["url"] if _imgs else None

    return {
        "entity": "broker",
        "slug": slug,
        "url": url,
        "name": clean(h1.get_text(" ")) if h1 else clean(person.get("name")) or slug,
        "role": role or clean(person.get("jobTitle")) or None,
        "photo_url": photo_url,
        "email": own_emails[0] if own_emails else None,
        "phone": own_phones[0] if own_phones else None,
        "emails": own_emails,
        "phones": own_phones,
        "agency_contacts": {"emails": [e for e in emails if e not in own_emails],
                            "phones": [p for p in phones if p not in own_phones]},
        "socials": socials,
        "languages": person.get("knowsLanguage"),
        "bio": next((s["body"] for s in sections(soup) if re.search(r"about|bio", s["heading"], re.I)), None),
        "listings": linked_projects(soup),
        "sections": sections(soup),
        "meta": meta_tags(soup),
        "json_ld": ld,
        "scraped_at": datetime.now(timezone.utc).isoformat(),
    }


def parse_learn(url: str, html: str) -> dict:
    soup = soup_of(html)
    ld = json_ld(html)
    slug = url.rstrip("/").split("/")[-1]
    h1 = soup.select_one("h1")
    art = ld_of_type(ld, "Article", "BlogPosting", "NewsArticle") or {}
    body = "\n\n".join(
        clean(p.get_text(" ")) for p in soup.select("article p, main p")
        if len(clean(p.get_text(" "))) > 60
    )
    return {
        "entity": "learn",
        "slug": slug,
        "url": url,
        "title": clean(h1.get_text(" ")) if h1 else clean(art.get("headline")) or slug,
        "published": art.get("datePublished"),
        "modified": art.get("dateModified"),
        "author": (art.get("author") or {}).get("name") if isinstance(art.get("author"), dict) else art.get("author"),
        "body": body or None,
        "sections": sections(soup),
        "faqs": faqs(ld, html),
        "meta": meta_tags(soup),
        "json_ld": ld,
        "scraped_at": datetime.now(timezone.utc).isoformat(),
    }


def parse_listing_index(url: str, html: str) -> list[dict]:
    """
    /listings holds direct owner mandates. It renders '<n> listings' and cards.
    Returns [] when the board is empty (which it is at time of writing).
    """
    soup = soup_of(html)
    out, seen = [], set()
    for a in soup.select('a[href^="/listings/"]'):
        href = a["href"].split("?")[0].rstrip("/")
        slug = href.split("/")[-1]
        if not slug or slug == "listings" or slug in seen:
            continue
        seen.add(slug)
        txt = clean(a.get_text(" "))
        out.append({
            "entity": "listing",
            "slug": slug,
            "url": urljoin(BASE, href),
            "card_text": txt,
            "price_raw": next((clean(p) for p in re.findall(r"AED[\s\xa0]*[\d,]+", txt)), None),
            "scraped_at": datetime.now(timezone.utc).isoformat(),
        })
    return out


def parse_page(url: str, html: str) -> dict:
    soup = soup_of(html)
    h1 = soup.select_one("h1")
    return {
        "entity": "page",
        "path": urlparse(url).path or "/",
        "url": url,
        "title": clean(h1.get_text(" ")) if h1 else None,
        "sections": sections(soup),
        "faqs": faqs(json_ld(html), html),
        "meta": meta_tags(soup),
        "scraped_at": datetime.now(timezone.utc).isoformat(),
    }


# ---------------------------------------------------------------------------
# sitemap discovery
# ---------------------------------------------------------------------------
def sitemap_urls(fetcher: Fetcher) -> dict[str, list[str]]:
    """{'projects': [...], 'developers': [...], ...} from the sitemap index."""
    idx = fetcher.get(SITEMAP_INDEX)
    groups: dict[str, list[str]] = {}
    if not idx:
        print("  ! sitemap index unreachable", file=sys.stderr)
        return groups

    ns = {"s": "http://www.sitemaps.org/schemas/sitemap/0.9"}
    try:
        root = ET.fromstring(idx.encode("utf-8"))
    except ET.ParseError as e:
        print(f"  ! bad sitemap index: {e}", file=sys.stderr)
        return groups

    for loc in root.findall(".//s:sitemap/s:loc", ns):
        sm_url = (loc.text or "").strip()
        kind = re.sub(r"\.xml$", "", sm_url.rstrip("/").split("/")[-1])
        xml = fetcher.get(sm_url)
        if not xml:
            continue
        try:
            sub = ET.fromstring(xml.encode("utf-8"))
        except ET.ParseError:
            continue
        urls = [(u.text or "").strip() for u in sub.findall(".//s:url/s:loc", ns)]
        groups[kind] = [u for u in urls if u]
        print(f"  {kind:<12} {len(groups[kind]):>5} urls")
    return groups


# ---------------------------------------------------------------------------
# driver
# ---------------------------------------------------------------------------
PARSERS = {
    "projects": parse_project,
    "developers": parse_developer,
    "areas": parse_area,
    "brokers": parse_broker,
    "learn": parse_learn,
    "pages": parse_page,
}


class ScrapePaused(Exception):
    """Raised to unwind cleanly when should_pause() turns true mid-run."""
    def __init__(self, partial: dict[str, list[dict]]):
        self.partial = partial
        super().__init__("scrape paused")


def scrape_group(kind: str, urls: list[str], fetcher: Fetcher, workers: int,
                  on_progress: Callable[[str, int, int], None] | None = None,
                  should_pause: Callable[[], bool] | None = None) -> list[dict]:
    """
    on_progress(kind, done, total) fires every 25 pages (same cadence as the
    console print) -- the job runner uses this to persist live progress to
    the database so a polling UI can show real numbers, not just a spinner.

    should_pause(), if given, is checked after every page. Returning True
    stops the loop from consuming further completions (already-dispatched
    in-flight requests still finish, but nothing new is queued) and raises
    ScrapePaused with whatever was collected so far -- nothing is lost, the
    next run just re-fetches the remaining URLs (fast, via the HTML cache).
    """
    parser = PARSERS[kind]
    results: list[dict] = []
    done = 0
    total = len(urls)
    lock = threading.Lock()

    def work(u: str) -> dict | None:
        html = fetcher.get(u)
        if not html:
            return None
        try:
            return parser(u, html)
        except Exception as e:  # one bad page must not kill the run
            return {"entity": kind.rstrip("s"), "url": u, "parse_error": f"{type(e).__name__}: {e}"}

    with cf.ThreadPoolExecutor(max_workers=workers) as ex:
        for rec in ex.map(work, urls):
            with lock:
                done += 1
                if rec:
                    results.append(rec)
                if done % 25 == 0 or done == total:
                    pct = done * 100 // max(total, 1)
                    print(f"\r  {kind:<12} {done}/{total} ({pct}%)  ok={len(results)}",
                          end="", flush=True)
                    if on_progress:
                        on_progress(kind, done, total)
            if should_pause and should_pause():
                print(f"\n  {kind:<12} paused at {done}/{total}")
                raise ScrapePaused({kind: results})
    print()
    return results


def run_scrape(out: str = "output", only: Iterable[str] | None = None,
               limit: int | None = None, workers: int = 6, delay: float = 0.3,
               cache_dir: str | None = ".cache_maisondubai",
               revalidate: bool = False, write_json: bool = True,
               on_progress: Callable[[dict], None] | None = None,
               should_pause: Callable[[], bool] | None = None) -> dict[str, Any]:
    """
    Scrape and return {"meta":…, "cross_reference":…, <entity>: [records]}.

    This is the importable entry point -- the backend's scheduled job calls it
    directly instead of shelling out, so it can ingest the records in-process.

    on_progress, if given, is called every ~25 pages with a dict:
        {"stage": "projects", "stage_done": 1245, "stage_total": 2021,
         "overall_done": 1245, "overall_total": 2662}
    The job runner persists this to the ScrapeRun row so a polling UI shows
    real numbers instead of just an elapsed-time spinner.

    should_pause, if given and it returns True, stops after the current
    in-flight batch and returns whatever was fetched so far, with
    meta["paused"] = True. Nothing is discarded or ingested as a false
    "everything's gone" state -- the caller decides what a paused result
    means. Re-running later is fast: already-fetched pages hit the HTML
    cache (or return 304 with --revalidate).
    """
    os.makedirs(out, exist_ok=True)
    fetcher = Fetcher(cache_dir, delay=delay, revalidate=revalidate)

    t0 = time.time()
    print(f"maisondubai.com -> {os.path.abspath(out)}")
    print("\n[1/3] reading sitemaps")
    groups = sitemap_urls(fetcher)
    if not groups:
        raise RuntimeError("sitemap index unreachable; aborting")

    wanted = set(only) if only else set(PARSERS) | {"listings"}
    bundle: dict[str, Any] = {}
    order = ("projects", "developers", "areas", "brokers", "learn", "pages")

    # Resolve every kind's URL list up front (post --limit) so the overall
    # total is known before scraping starts, not guessed mid-run.
    planned: dict[str, list[str]] = {}
    for kind in order:
        if kind not in wanted:
            continue
        urls = groups.get(kind, [])
        if kind == "pages":
            urls = [u for u in urls if urlparse(u).path not in ("", "/")] or urls
        if limit:
            urls = urls[:limit]
        planned[kind] = urls
    overall_total = sum(len(u) for u in planned.values())
    overall_done = 0

    print("\n[2/3] scraping")
    paused = False
    for kind, urls in planned.items():
        if not urls:
            print(f"  {kind:<12} (none)")
            continue

        def _cb(k: str, done: int, total: int, _prior: int = overall_done) -> None:
            if on_progress:
                on_progress({
                    "stage": k, "stage_done": done, "stage_total": total,
                    "overall_done": _prior + done, "overall_total": overall_total,
                })

        try:
            bundle[kind] = scrape_group(kind, urls, fetcher, workers,
                                         on_progress=_cb, should_pause=should_pause)
        except ScrapePaused as exc:
            bundle[kind] = exc.partial[kind]
            paused = True
            break
        overall_done += len(urls)

    if "listings" in wanted and not paused:
        html = fetcher.get(f"{BASE}/listings")
        items = parse_listing_index(f"{BASE}/listings", html) if html else []
        detail = []
        for it in (items[:limit] if limit else items):
            dh = fetcher.get(it["url"])
            if dh:
                rec = parse_project(it["url"], dh)
                rec["entity"] = "listing"
                rec["card_text"] = it.get("card_text")
                detail.append(rec)
            else:
                detail.append(it)
        bundle["listings"] = detail
        note = "" if detail else "  (board is empty - owner mandates are added as taken on)"
        print(f"  listings     {len(detail)} found{note}")

    # Developer/area pages cap their chip lists in the UI, so derive the complete
    # project -> developer/area mapping from the project records themselves.
    xref: dict[str, Any] = {}
    if bundle.get("projects"):
        by_dev: dict[str, list[str]] = {}
        by_area: dict[str, list[str]] = {}
        for p in bundle["projects"]:
            if p.get("parse_error"):
                continue
            d = (p.get("developer") or {}).get("slug")
            a = (p.get("area") or {}).get("slug")
            if d:
                by_dev.setdefault(d, []).append(p["slug"])
            if a:
                by_area.setdefault(a, []).append(p["slug"])
        xref = {
            "projects_by_developer": {k: sorted(v) for k, v in sorted(by_dev.items())},
            "projects_by_area": {k: sorted(v) for k, v in sorted(by_area.items())},
        }
        print(f"\n  cross-reference: {len(by_dev)} developers, {len(by_area)} areas "
              f"mapped from {len(bundle['projects'])} projects")

    meta = {
        "source": BASE,
        "scraped_at": datetime.now(timezone.utc).isoformat(),
        "duration_seconds": round(time.time() - t0, 1),
        "counts": {k: len(v) for k, v in bundle.items()},
        "http": dict(fetcher.stats),
        "parse_errors": {k: sum(1 for r in v if r.get("parse_error")) for k, v in bundle.items()},
        "paused": paused,
    }

    if write_json:
        print("\n[3/3] writing JSON")
        if xref:
            with open(os.path.join(out, "cross_reference.json"), "w", encoding="utf-8") as fh:
                json.dump(xref, fh, indent=2, ensure_ascii=False)
        for kind, recs in bundle.items():
            fp = os.path.join(out, f"{kind}.json")
            with open(fp, "w", encoding="utf-8") as fh:
                json.dump(recs, fh, indent=2, ensure_ascii=False)
            print(f"  {os.path.basename(fp):<22} {len(recs):>5} records  "
                  f"{os.path.getsize(fp)/1e6:.2f} MB")

        combined = os.path.join(out, "maisondubai_all.json")
        with open(combined, "w", encoding="utf-8") as fh:
            json.dump({"meta": meta, **bundle, "cross_reference": xref},
                      fh, indent=2, ensure_ascii=False)
        with open(os.path.join(out, "_run_meta.json"), "w", encoding="utf-8") as fh:
            json.dump(meta, fh, indent=2, ensure_ascii=False)
        print(f"  {'maisondubai_all.json':<22} {os.path.getsize(combined)/1e6:.2f} MB")

    print(f"\ndone in {meta['duration_seconds']}s  "
          f"(net={fetcher.stats['net']} not-modified={fetcher.stats['not_modified']} "
          f"cache={fetcher.stats['cache']} fail={fetcher.stats['fail']})")

    # The backend ingests this return value directly rather than re-reading the
    # ~136MB of JSON back off disk.
    return {"meta": meta, "cross_reference": xref, **bundle}


def main() -> int:
    ap = argparse.ArgumentParser(description="Scrape maisondubai.com into JSON")
    ap.add_argument("--out", default="output", help="output directory (default: output)")
    ap.add_argument("--only", nargs="+", choices=list(PARSERS) + ["listings"],
                    help="scrape only these entity types")
    ap.add_argument("--limit", type=int, help="max pages per entity type (smoke test)")
    ap.add_argument("--workers", type=int, default=6, help="concurrent requests (default 6)")
    ap.add_argument("--delay", type=float, default=0.3,
                    help="per-request jittered delay (default 0.3s)")
    ap.add_argument("--no-cache", action="store_true", help="skip the on-disk HTML cache")
    ap.add_argument("--cache-dir", default=".cache_maisondubai")
    ap.add_argument("--revalidate", action="store_true",
                    help="use the cache but revalidate via ETag/If-None-Match "
                         "(use this for scheduled re-scrapes)")
    args = ap.parse_args()

    try:
        run_scrape(out=args.out, only=args.only, limit=args.limit,
                   workers=args.workers, delay=args.delay,
                   cache_dir=None if args.no_cache else args.cache_dir,
                   revalidate=args.revalidate)
    except RuntimeError as exc:
        print(exc, file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except KeyboardInterrupt:
        print("\ninterrupted", file=sys.stderr)
        sys.exit(130)
