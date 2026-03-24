"""
main.py — Rajasthan Dashboard API v3
Every field served to the frontend comes directly from the scrapers.
No hardcoded data anywhere in this file.
"""
import asyncio, re, logging
import json
from datetime import datetime
from pathlib import Path
from typing import Optional
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware

from scrapers.igod_scraper       import scrape_igod
from scrapers.rajras_scraper     import scrape_rajras
from scrapers.jansoochna_full_scraper import (
    OUTPUT_PATH as JANSOOCHNA_OUTPUT_PATH,
    run_scraper as scrape_jansoochna_full,
    save_json as save_jansoochna_json,
)
from scrapers.jansoochna_scraper import scrape_jansoochna as scrape_jansoochna_basic
from scrapers.myscheme_scraper   import scrape_myscheme
from scrapers.budget_scraper     import scrape_budget
from scrapers.jjm_scraper        import scrape_jjm
from scrapers.pmksy_scraper      import scrape_pmksy
from scrapers.scheme_dashboard_scraper import scrape_scheme_dashboards

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s — %(message)s")
log = logging.getLogger("api")

from contextlib import asynccontextmanager

@asynccontextmanager
async def lifespan(app_: FastAPI):
    """Auto-scrape all sources + district dashboards on startup."""
    log.info("🚀 Startup: kicking off background scrape of all sources + district dashboards...")
    async def _scrape_all(): await asyncio.gather(*[_run(sid, fn) for sid, fn in SCRAPERS.items()])
    asyncio.create_task(_scrape_all())

    async def _fetch_jjm_startup():
        data = await asyncio.to_thread(scrape_jjm)
        _cache[JJM_CACHE_KEY] = {
            "data": data,
            "live": any(d.get("live") for d in data),
            "scraped_at": data[0].get("scraped_at") if data else None,
        }
        log.info("✅ JJM startup: %d districts", len(data))

    async def _fetch_pmksy_startup():
        data = await asyncio.to_thread(scrape_pmksy)
        _cache[PMKSY_CACHE_KEY] = {
            "data": data,
            "live": any(d.get("live") for d in data),
            "scraped_at": data[0].get("scraped_at") if data else None,
        }
        log.info("✅ PMKSY startup: %d districts", len(data))

    async def _fetch_scheme_dashboards_startup():
        data = await asyncio.to_thread(scrape_scheme_dashboards)
        _cache[SCHEME_DASHBOARD_CACHE_KEY] = {
            "data": data,
            "live": any(item.get("live") for item in data),
            "scraped_at": datetime.utcnow().isoformat() + "Z",
        }
        log.info("✅ Scheme dashboards startup: %d scheme cards", len(data))

    asyncio.create_task(_fetch_jjm_startup())
    asyncio.create_task(_fetch_pmksy_startup())
    asyncio.create_task(_fetch_scheme_dashboards_startup())
    yield

app = FastAPI(title="Rajasthan Dashboard API v3", lifespan=lifespan)
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

_cache: dict = {}
def scrape_jansoochna():
    """Prefer full dataset scraping, but never let an empty run wipe out usable data."""
    try:
        data = scrape_jansoochna_full()
        if data:
            return data
        log.warning("Jan Soochna full scraper returned 0 items; falling back to basic scraper.")
    except Exception as exc:
        log.warning("Jan Soochna full scraper failed: %s; falling back to basic scraper.", exc)

    data = scrape_jansoochna_basic()
    if data:
        try:
            save_jansoochna_json(data, JANSOOCHNA_OUTPUT_PATH)
        except Exception as exc:
            log.warning("Could not persist fallback Jan Soochna dataset: %s", exc)
    return data

SCRAPERS = {
    "igod":       scrape_igod,
    "rajras":     scrape_rajras,
    "jansoochna": scrape_jansoochna,
    "myscheme":   scrape_myscheme,
}
BUDGET_CACHE_KEY = "budget"
JJM_CACHE_KEY    = "jjm"
PMKSY_CACHE_KEY  = "pmksy"
SCHEME_DASHBOARD_CACHE_KEY = "scheme_dashboards"

# ── scrape helpers ─────────────────────────────────────────────────────────────
def _store(sid, data, status="ok", error=""):
    _cache[sid] = {
        "source_id": sid,
        "data": data,
        "status": status,
        "error": error,
        "count": len(data) if isinstance(data, list) else 0,
        "scraped_at": datetime.utcnow().isoformat() + "Z",
    }

async def _run(sid, fn):
    try:
        data = await asyncio.to_thread(fn)
        _store(sid, data, "ok")
        log.info("✅ %s — %d items", sid, len(data))
    except Exception as e:
        log.error("❌ %s: %s", sid, e)
        _store(sid, [], "error", str(e))
    return _cache[sid]

# ── routes ─────────────────────────────────────────────────────────────────────
@app.get("/")
def root():
    return {"status": "ok", "version": "3.0"}

@app.get("/status")
def status():
    return {
        "sources": {
            sid: {
                "status":     _cache.get(sid, {}).get("status", "not_scraped"),
                "count":      _cache.get(sid, {}).get("count", 0),
                "scraped_at": _cache.get(sid, {}).get("scraped_at"),
            }
            for sid in SCRAPERS
        }
    }

@app.post("/scrape/all")
async def scrape_all():
    results = await asyncio.gather(*[_run(sid, fn) for sid, fn in SCRAPERS.items()])
    try:
        jjm = await asyncio.to_thread(scrape_jjm)
        _cache[JJM_CACHE_KEY] = {
            "data": jjm,
            "live": any(d.get("live") for d in jjm),
            "scraped_at": jjm[0].get("scraped_at") if jjm else None,
        }
        pmksy = await asyncio.to_thread(scrape_pmksy)
        _cache[PMKSY_CACHE_KEY] = {
            "data": pmksy,
            "live": any(d.get("live") for d in pmksy),
            "scraped_at": pmksy[0].get("scraped_at") if pmksy else None,
        }
        dashboards = await asyncio.to_thread(scrape_scheme_dashboards)
        _cache[SCHEME_DASHBOARD_CACHE_KEY] = {
            "data": dashboards,
            "live": any(item.get("live") for item in dashboards),
            "scraped_at": datetime.utcnow().isoformat() + "Z",
        }
    except Exception as exc:
        log.warning("District dashboard refresh failed during scrape/all: %s", exc)
    return {"results": {r["source_id"]: {"status": r["status"], "count": r["count"]} for r in results}}

@app.post("/scrape/{source_id}")
async def scrape_one(source_id: str):
    if source_id not in SCRAPERS:
        raise HTTPException(404, f"Unknown source: {source_id}")
    return await _run(source_id, SCRAPERS[source_id])

@app.get("/data/rajras")
def get_rajras_schemes():
    data_path = Path(__file__).resolve().parent / "data" / "rajras_schemes.json"
    if not data_path.exists():
        cached = _cache.get("rajras", {}).get("data")
        if cached:
            return [
                _enrich_scheme({**item, "_src": "rajras", "_src_label": "RajRAS", "_src_url": "rajras.in"})
                for item in cached
            ]
        raise HTTPException(404, "RajRAS dataset not found and no cached RajRAS data is available.")
    with data_path.open("r", encoding="utf-8") as f:
        return json.load(f)

@app.get("/data/jansoochna")
def get_jansoochna_schemes():
    data_path = Path(__file__).resolve().parent / "data" / "jansoochna_schemes.json"
    if data_path.exists():
        with data_path.open("r", encoding="utf-8") as f:
            file_data = json.load(f)
        if isinstance(file_data, list) and file_data:
            return file_data
        log.warning("Jan Soochna dataset file is empty; falling back to cached data.")

    cached = _cache.get("jansoochna", {}).get("data")
    if cached:
        return [
            _enrich_scheme({**item, "_src": "jansoochna", "_src_label": "Jan Soochna", "_src_url": "jansoochna.rajasthan.gov.in"})
            for item in cached
        ]
    raise HTTPException(404, "Jan Soochna dataset not found and no cached Jan Soochna data is available.")

@app.get("/data/{source_id}")
def get_data(source_id: str, limit: Optional[int] = None):
    if source_id not in SCRAPERS:
        raise HTTPException(404)
    if source_id not in _cache:
        raise HTTPException(404, f"No data yet — POST /scrape/{source_id} first")
    entry = _cache[source_id]
    data = entry["data"][:limit] if limit else entry["data"]
    return {**entry, "data": data}

@app.get("/data")
def get_all():
    return {sid: _cache.get(sid, {"status": "not_scraped", "data": [], "count": 0}) for sid in SCRAPERS}


@app.get("/scheme-dashboards")
async def get_scheme_dashboards(refresh: bool = False):
    """
    Returns normalized scheme dashboard data for the district dashboard tab.
    Live public metrics are returned when source pages expose them; otherwise the
    response includes truthful limited-source states for the frontend to render.
    """
    if not refresh and SCHEME_DASHBOARD_CACHE_KEY in _cache:
        return _cache[SCHEME_DASHBOARD_CACHE_KEY]
    data = await asyncio.to_thread(scrape_scheme_dashboards)
    _cache[SCHEME_DASHBOARD_CACHE_KEY] = {
        "data": data,
        "live": any(item.get("live") for item in data),
        "scraped_at": datetime.utcnow().isoformat() + "Z",
    }
    return _cache[SCHEME_DASHBOARD_CACHE_KEY]

# ── Scheme enrichment helpers ──────────────────────────────────────────────────

def _extract_budget_amount(benefit_text, description=""):
    """
    Parse a concise budget/benefit amount from scraped benefit or description text.
    Returns strings like '₹2,500/mo', '₹25.0 L/yr', '100 days/yr', 'Free Medicines'.
    Returns None if no meaningful amount found.
    """
    text = str(benefit_text or description or "").strip()
    if not text:
        return None

    # ₹ / Rs. amount
    m = re.search(
        r'(?:₹|Rs\.?|INR)\s*([\d,]+(?:\.\d+)?)\s*(lakh\s*crore|lakh|crore|cr\.?)?',
        text, re.I
    )
    if m:
        raw  = m.group(1).replace(",", "")
        unit = (m.group(2) or "").strip().lower()
        try:
            val = float(raw)
        except ValueError:
            return None

        if "lakh crore" in unit:  display = f"₹{val} L Cr"
        elif unit == "lakh":       display = f"₹{val} L"
        elif "crore" in unit or unit == "cr": display = f"₹{val} Cr"
        else:
            if val >= 10_000_000:  display = f"₹{val/10_000_000:.1f} Cr"
            elif val >= 1_00_000:  display = f"₹{val/1_00_000:.1f} L"
            else:                  display = f"₹{int(val):,}"

        end = m.end()
        ctx = text[end:end + 25].lower()
        if re.search(r'per\s*year|/year|/yr|per\s*annum|annually', ctx):
            display += "/yr"
        elif re.search(r'per\s*month|/month|/mo|monthly', ctx):
            display += "/mo"
        return display

    # "X days/year" (MGNREGA-style)
    m2 = re.search(r'(\d+)\s*days?\s*/?\s*(?:year|yr)', text, re.I)
    if m2:
        return f"{m2.group(1)} days/yr"

    # "free <specific thing>" — strict whitelist to avoid "tax-free" etc.
    m3 = re.search(
        r'(?<![a-zA-Z\-])free\s+'
        r'(medicine|medicines|lpg|gas\s*connection|electricity|meals?|food|coaching|treatment|health\s*care)',
        text, re.I
    )
    if m3:
        return f"Free {m3.group(1).title()}"

    return None


def _format_beneficiaries(beneficiary_count, eligibility="", description=""):
    """
    Return a short, human-readable beneficiary string from scraped fields.
    e.g. "12.0 L", "SC/ST students", "All Rajasthan families"
    """
    # Jan Soochna: beneficiary_count is a raw integer
    if beneficiary_count:
        s = str(beneficiary_count).strip().replace(",", "")
        try:
            n = int(float(s))
            if n >= 10_000_000: return f"{n/10_000_000:.1f} Cr"
            if n >= 1_00_000:   return f"{n/1_00_000:.1f} L"
            if n >= 1_000:      return f"{n/1_000:.0f}K"
            return str(n)
        except ValueError:
            if s:
                return s[:30]

    # Try to extract a short phrase from eligibility
    for src in [eligibility, description]:
        if not src:
            continue
        src = str(src).strip()
        # Take up to first sentence break if short enough
        first = re.split(r'[.;\n]', src)[0].strip()
        if 4 <= len(first) <= 50:
            return first[:50]

    return None


def _enrich_scheme(s):
    """Add budget_amount and beneficiary_display to a scheme dict."""
    budget_amount = _extract_budget_amount(
        s.get("benefit", "") or s.get("benefits", ""),
        s.get("description", "")
    )
    beneficiary_display = _format_beneficiaries(
        s.get("beneficiary_count") or s.get("beneficiaries"),
        s.get("eligibility", ""),
        s.get("description", ""),
    )
    return {
        **s,
        "budget_amount":        budget_amount,
        "beneficiary_display":  beneficiary_display,
    }


def _normalize_portal(p):
    """Return a stable portal object for IGOD records."""
    name = p.get("name") or p.get("organization_name") or p.get("portal_title") or "Government Portal"
    url = p.get("url") or p.get("website_url") or ""
    description = p.get("description") or p.get("meta_description") or p.get("summary") or ""
    return {
        **p,
        "name": name,
        "organization_name": p.get("organization_name") or name,
        "portal_title": p.get("portal_title") or p.get("page_title") or name,
        "url": url,
        "website_url": p.get("website_url") or url,
        "description": description,
        "summary": p.get("summary") or description,
        "category": p.get("category") or "Government Services",
        "status": p.get("status") or "Active",
    }


# ── aggregate ──────────────────────────────────────────────────────────────────
@app.get("/aggregate")
def aggregate():
    """
    Single endpoint consumed by the entire frontend.
    Merges all 4 sources into structured sections.
    ZERO hardcoded data — everything comes from scraper output.
    """
    igod_raw  = _cache.get("igod",       {}).get("data", [])
    rr_raw    = _cache.get("rajras",      {}).get("data", [])
    jsp_raw   = _cache.get("jansoochna",  {}).get("data", [])
    ms_raw    = _cache.get("myscheme",    {}).get("data", [])

    # ── 1. Schemes — tag source then enrich with parsed budget/beneficiary fields
    schemes = [
        _enrich_scheme({**s, "_src": "rajras",     "_src_label": "RajRAS",      "_src_url": "rajras.in"})
        for s in rr_raw
    ] + [
        _enrich_scheme({**s, "_src": "jansoochna", "_src_label": "Jan Soochna", "_src_url": "jansoochna.rajasthan.gov.in"})
        for s in jsp_raw
    ] + [
        _enrich_scheme({**s, "_src": "myscheme",   "_src_label": "MyScheme",    "_src_url": "myscheme.gov.in"})
        for s in ms_raw
    ]


    # ── 2. Category breakdown (derived entirely from scheme data) ──────────────
    cat_map: dict = {}
    for s in schemes:
        c = s.get("category") or "General"
        if c not in cat_map:
            cat_map[c] = {"name": c, "count": 0, "sources": set()}
        cat_map[c]["count"] += 1
        cat_map[c]["sources"].add(s.get("_src_label", ""))
    categories = sorted(
        [{"name": v["name"], "count": v["count"], "sources": list(v["sources"])} for v in cat_map.values()],
        key=lambda x: -x["count"]
    )

    # ── 3. Source counts for charts ────────────────────────────────────────────
    source_counts = [
        {"source": "RajRAS",      "count": len(rr_raw),  "color": "#3b82f6"},
        {"source": "Jan Soochna", "count": len(jsp_raw), "color": "#10b981"},
        {"source": "MyScheme",    "count": len(ms_raw),  "color": "#8b5cf6"},
        {"source": "IGOD Directory","count": len(igod_raw),"color": "#f97316"},
    ]

    # ── 4. Portals (igod only) ─────────────────────────────────────────────────
    portals = [_normalize_portal(p) for p in igod_raw]

    # ── 5. KPIs ────────────────────────────────────────────────────────────────
    sources_live = sum(1 for sid in SCRAPERS if _cache.get(sid, {}).get("status") == "ok")
    kpis = {
        "total_schemes":    len(schemes),
        "total_portals":    len(portals),
        "unique_categories":len(categories),
        "sources_live":     sources_live,
        "rajras_count":     len(rr_raw),
        "jansoochna_count": len(jsp_raw),
        "myscheme_count":   len(ms_raw),
        "igod_count":       len(igod_raw),
    }

    # ── 6. Alerts — built from real scraper data patterns ─────────────────────
    # ── 7. Source metadata ─────────────────────────────────────────────────────
    source_status = {
        sid: {
            "status":     _cache.get(sid, {}).get("status", "not_scraped"),
            "count":      _cache.get(sid, {}).get("count", 0),
            "scraped_at": _cache.get(sid, {}).get("scraped_at"),
            "error":      _cache.get(sid, {}).get("error", ""),
        }
        for sid in SCRAPERS
    }

    # Attach live JJM districts if available in cache
    jjm_cache     = _cache.get(JJM_CACHE_KEY, {})
    jjm_districts = jjm_cache.get("data", [])
    pmksy_cache   = _cache.get(PMKSY_CACHE_KEY, {})
    pmksy_districts = pmksy_cache.get("data", [])
    alerts = _build_alerts(schemes, portals, igod_raw, jjm_districts, source_status)

    return {
        "scraped_at":     datetime.utcnow().isoformat() + "Z",
        "kpis":           kpis,
        "schemes":        schemes,
        "portals":        portals,
        "categories":     categories,
        "source_counts":  source_counts,
        "alerts":         alerts,
        "source_status":  source_status,
        "jjm_districts":  jjm_districts,
        "pmksy_districts": pmksy_districts,
    }


def _build_alerts(schemes, portals, igod_raw, jjm_districts=None, source_status=None):
    """
    Generate intelligence alerts entirely from scraped data.
    Every alert title/body references actual counts and names from scrapers.
    """
    alerts = []
    jjm_districts = jjm_districts or []
    source_status = source_status or {}

    # ── Health schemes
    health = [s for s in schemes if re.search(r"health|medical|ayush|chiranjeevi|dawa|hospital", s.get("category", ""), re.I)]
    if health:
        names = ", ".join(s["name"] for s in health[:3])
        alerts.append({
            "id": "alert_health", "type": "ACTION", "severity": "Action", "icon": "🏥",
            "title": f"{len(health)} Health Schemes Active — Rajasthan",
            "date": _latest_scraped(health),
            "body": f"{len(health)} health-related schemes scraped from official sources. Key schemes: {names}{'…' if len(health)>3 else ''}.",
            "tags": [f"🏥 {len(health)} health schemes", f"Top schemes: {min(len(health), 3)} highlighted", "📍 State-wide"],
            "source": f"Source: {', '.join(set(s.get('_src_label','') for s in health))}",
            "borderColor": "#10b981", "bgColor": "#f0fdf4", "tagColor": "#10b981",
        })

    # ── Agriculture
    agri = [s for s in schemes if re.search(r"agri|kisan|farm|crop|horticulture", s.get("category", ""), re.I)]
    if agri:
        names = ", ".join(s["name"] for s in agri[:3])
        alerts.append({
            "id": "alert_agri", "type": "INSIGHT", "severity": "Insight", "icon": "🌾",
            "title": f"{len(agri)} Agriculture Schemes Found",
            "date": _latest_scraped(agri),
            "body": f"{len(agri)} agriculture and farmer welfare schemes scraped. Top schemes: {names}.",
            "tags": [f"🌾 {len(agri)} agri schemes", f"Top 3: {min(len(agri), 3)} named", "📍 State-wide"],
            "source": f"Source: {', '.join(set(s.get('_src_label','') for s in agri))}",
            "borderColor": "#10b981", "bgColor": "#f0fdf4", "tagColor": "#10b981",
        })

    # ── Social welfare
    social = [s for s in schemes if re.search(r"social|pension|welfare|palanhar", s.get("category", ""), re.I)]
    if social:
        names = ", ".join(s["name"] for s in social[:3])
        alerts.append({
            "id": "alert_social", "type": "ACTION", "severity": "Action", "icon": "🛡️",
            "title": f"{len(social)} Social Welfare Schemes — Beneficiary Verification Needed",
            "date": _latest_scraped(social),
            "body": f"{len(social)} social welfare schemes active. Includes: {names}. Recommend verifying beneficiary lists for accuracy.",
            "tags": [f"🛡️ {len(social)} welfare schemes", f"Named schemes: {min(len(social), 3)}", "📍 Beneficiary-facing schemes"],
            "source": f"Source: {', '.join(set(s.get('_src_label','') for s in social))}",
            "borderColor": "#8b5cf6", "bgColor": "#f5f3ff", "tagColor": "#8b5cf6",
        })

    # ── IGOD portals
    if portals:
        cats = list(set(p.get("category", "") for p in portals if p.get("category")))[:4]
        alerts.append({
            "id": "alert_portals", "type": "INSIGHT", "severity": "Insight", "icon": "🏛️",
            "title": f"{len(portals)} Official Portals — IGOD Rajasthan Directory",
            "date": _latest_scraped(portals),
            "body": f"IGOD directory lists {len(portals)} active Rajasthan government portals. Categories include: {', '.join(cats)}.",
            "tags": [f"🏛️ {len(portals)} portals listed", f"{len(cats)} portal categories sampled", "📍 IGOD directory"],
            "source": "Source: igod.gov.in/sg/RJ/SPMA/organizations",
            "borderColor": "#3b82f6", "bgColor": "#eff6ff", "tagColor": "#3b82f6",
        })

    # ── Water & Sanitation schemes
    water = [s for s in schemes if re.search(r"water|jal|sanitation|swachh", s.get("category", ""), re.I)]
    if water:
        names = ", ".join(s["name"] for s in water[:2])
        live_jjm_rows = [
            row for row in jjm_districts
            if isinstance(row.get("coverage"), (int, float))
        ]
        lowest_rows = sorted(live_jjm_rows, key=lambda row: row.get("coverage", 0))[:2]
        lagging_text = ", ".join(
            f"{row.get('name')} ({row.get('coverage'):.1f}%)"
            for row in lowest_rows
            if row.get("name")
        )
        avg_coverage = (
            sum(row.get("coverage", 0) for row in live_jjm_rows) / len(live_jjm_rows)
            if live_jjm_rows else None
        )
        coverage_text = f"Current JJM district average is {avg_coverage:.1f}%." if avg_coverage is not None else ""
        lagging_note = f" Lowest live-coverage districts: {lagging_text}." if lagging_text else ""
        alerts.append({
            "id": "alert_water", "type": "CRITICAL", "severity": "Critical", "icon": "🚨",
            "title": f"JJM Coverage Gap — {len(water)} Water Schemes Tracked",
            "date": _latest_scraped(live_jjm_rows) or _latest_scraped(water),
            "body": f"{len(water)} water & sanitation schemes found in official sources including {names}.{coverage_text}{lagging_note}",
            "tags": [f"💧 {len(water)} water schemes", f"{len(live_jjm_rows)} JJM district rows", "📍 Low-coverage districts highlighted" if lagging_text else "📍 District coverage pending"],
            "source": f"Source: {', '.join(set(s.get('_src_label','') for s in water))} / JJM MIS",
            "borderColor": "#ef4444", "bgColor": "#fff5f5", "tagColor": "#ef4444",
        })

    # ── Error warnings for failed scrapes
    for sid, entry in source_status.items():
        if entry.get("status") == "error":
            alerts.append({
                "id": f"alert_err_{sid}", "type": "WARNING", "severity": "Warning", "icon": "⚠️",
                "title": f"Scrape Warning — {sid.upper()} fetch failed",
                "date": entry.get("scraped_at", ""),
                "body": f"Live scrape of {sid} failed. Showing cached/fallback data. Error: {str(entry.get('error',''))[:120]}. Click ↺ to retry.",
                "tags": [f"⚠️ {sid} offline", f"Cached rows: {entry.get('count', 0)}", "📍 Retry scraper"],
                "source": f"Source: Dashboard system monitor",
                "borderColor": "#f97316", "bgColor": "#fff7ed", "tagColor": "#f97316",
            })

    return alerts


@app.post("/insights")
async def generate_insights():
    """
    Calls Claude API with all scraped data and returns structured
    executive intelligence for the CM's office.
    Requires ANTHROPIC_API_KEY environment variable on Render.
    """
    import os, httpx

    api_key = os.environ.get("ANTHROPIC_API_KEY", "")
    if not api_key:
        raise HTTPException(500, "ANTHROPIC_API_KEY not set on server")

    igod_raw  = _cache.get("igod",       {}).get("data", [])
    rr_raw    = _cache.get("rajras",      {}).get("data", [])
    jsp_raw   = _cache.get("jansoochna",  {}).get("data", [])
    ms_raw    = _cache.get("myscheme",    {}).get("data", [])
    schemes   = rr_raw + jsp_raw + ms_raw
    portals   = [_normalize_portal(p) for p in igod_raw]

    if not schemes:
        raise HTTPException(400, "No scraped data found. Run /scrape/all first.")

    scheme_list = [
        {
            "name":        s.get("name", ""),
            "category":    s.get("category", "General"),
            "benefit":     s.get("benefit") or s.get("description") or "",
            "eligibility": s.get("eligibility", ""),
            "dept":        s.get("department") or s.get("ministry") or "",
            "source":      s.get("_src_label") or s.get("source") or "",
        }
        for s in schemes[:60]
    ]

    cat_counts = {}
    for s in schemes:
        c = s.get("category", "General")
        cat_counts[c] = cat_counts.get(c, 0) + 1

    prompt = f"""You are a senior policy analyst briefing the Chief Minister of Rajasthan, India.

REAL DATA scraped live from 4 official government websites:
- Jan Soochna Portal, MyScheme.gov.in, RajRAS, IGOD Portal Directory

SCHEMES ({len(schemes)} total):
{__import__('json').dumps(scheme_list, indent=1)}

PORTALS ({len(portals)} total):
{chr(10).join(f'  {p.get("name")} ({p.get("category")}) — {p.get("domain")}' for p in portals)}

CATEGORY BREAKDOWN:
{chr(10).join(f'  {c}: {n} schemes' for c,n in sorted(cat_counts.items(), key=lambda x:-x[1]))}

Analyse and respond ONLY with a valid JSON object — no markdown, no text outside JSON:

{{
  "executive_summary": {{
    "headline": "one powerful sentence summarizing the welfare ecosystem",
    "strongest_sector": "category with most schemes",
    "weakest_sector": "category most critically under-served",
    "overall_health": "GOOD|FAIR|NEEDS_ATTENTION",
    "key_stat": "one striking statistic from the data",
    "cm_note": "one urgent personal note to CM about immediate attention needed"
  }},
  "coverage_gaps": [
    {{
      "segment": "specific underserved citizen group",
      "gap_description": "what gap exists in current scheme coverage",
      "schemes_addressing": ["actual scheme names from data that partially help"],
      "schemes_missing": "what type of scheme is absent",
      "priority": "CRITICAL|HIGH|MEDIUM|LOW",
      "recommendation": "specific actionable recommendation referencing real data"
    }}
  ],
  "category_analysis": [
    {{
      "category": "category name",
      "scheme_count": 0,
      "assessment": "OVER_SERVED|WELL_SERVED|UNDER_SERVED|CRITICALLY_UNDER_SERVED",
      "rationale": "why — name actual schemes",
      "gap": "what is missing or null",
      "opportunity": "specific opportunity for the CM"
    }}
  ],
  "overlaps": [
    {{
      "title": "short cluster name",
      "schemes": ["Actual Scheme A", "Actual Scheme B"],
      "overlap_type": "BENEFIT_OVERLAP|ELIGIBILITY_OVERLAP|OBJECTIVE_OVERLAP",
      "overlap_description": "exactly how these schemes overlap",
      "impact": "waste or citizen confusion caused",
      "recommendation": "merge/consolidate/differentiate with specific steps"
    }}
  ],
  "priority_actions": [
    {{
      "rank": 1,
      "action": "specific action for CM",
      "rationale": "why — reference actual scheme names and data",
      "timeline": "This week|This month|This quarter",
      "expected_impact": "measurable concrete outcome",
      "schemes_involved": ["actual scheme names"],
      "priority": "CRITICAL|HIGH|MEDIUM"
    }}
  ],
  "data_quality_note": "brief note on data completeness"
}}

Rules: only reference actual scheme names from the data. Provide 4-5 coverage_gaps, 6-8 category_analysis, 3-4 overlaps, exactly 5 priority_actions ranked 1-5."""

    async with httpx.AsyncClient(timeout=60) as client:
        resp = await client.post(
            "https://api.anthropic.com/v1/messages",
            headers={
                "x-api-key": api_key,
                "anthropic-version": "2023-06-01",
                "content-type": "application/json",
            },
            json={
                "model": "claude-sonnet-4-20250514",
                "max_tokens": 4000,
                "messages": [{"role": "user", "content": prompt}],
            },
        )

    if resp.status_code != 200:
        raise HTTPException(502, f"Claude API error: {resp.status_code} — {resp.text[:200]}")

    raw = resp.json()["content"][0]["text"]
    clean = raw.replace("```json", "").replace("```", "").strip()

    try:
        insights = __import__('json').loads(clean)
    except Exception as e:
        raise HTTPException(500, f"Failed to parse Claude response: {e}\n\nRaw: {clean[:300]}")

    return {
        "insights": insights,
        "meta": {
            "schemes_analysed": len(schemes),
            "portals_analysed": len(portals),
            "generated_at": datetime.utcnow().isoformat() + "Z",
        }
    }


@app.get("/budget")
async def get_budget(refresh: bool = False):
    """Returns scraped budget + financial data. Cached for 1 hour."""
    if not refresh and BUDGET_CACHE_KEY in _cache:
        cached = _cache[BUDGET_CACHE_KEY]
        # Use cache if less than 1 hour old
        from datetime import timezone
        cached_at = cached.get("scraped_at","")
        if cached_at:
            try:
                from datetime import datetime
                dt = datetime.fromisoformat(cached_at.replace("Z","+00:00"))
                age_s = (datetime.now(timezone.utc) - dt).total_seconds()
                if age_s < 3600:
                    return cached
            except:
                pass
    data = await asyncio.to_thread(scrape_budget)
    _cache[BUDGET_CACHE_KEY] = data
    return data


@app.post("/scrape/budget")
async def scrape_budget_endpoint():
    """Force-refresh budget data (includes sparklines + JJM districts)."""
    data = await asyncio.to_thread(scrape_budget)
    _cache[BUDGET_CACHE_KEY] = data
    # Also update JJM cache from budget result
    if "jjm_districts" in data:
        _cache[JJM_CACHE_KEY] = {
            "data": data["jjm_districts"],
            "live": data.get("jjm_districts_live", False),
            "scraped_at": data.get("scraped_at"),
        }
    return {"status": "ok", "fields": len(data)}

@app.get("/jjm")
async def get_jjm(refresh: bool = False):
    """
    Returns live JJM district coverage data for all 33 Rajasthan districts.
    Scraped from ejalshakti.gov.in. Cached for 6 hours.
    """
    if not refresh and JJM_CACHE_KEY in _cache:
        cached = _cache[JJM_CACHE_KEY]
        return cached
    data = await asyncio.to_thread(scrape_jjm)
    _cache[JJM_CACHE_KEY] = {
        "data": data,
        "live": any(d.get("live") for d in data),
        "scraped_at": data[0].get("scraped_at") if data else None,
    }
    return _cache[JJM_CACHE_KEY]


@app.get("/pmksy")
async def get_pmksy(refresh: bool = False):
    """
    Returns live PMKSY district irrigation data for Rajasthan.
    Scraped from wdcpmksy.dolr.gov.in and cached for reuse.
    """
    if not refresh and PMKSY_CACHE_KEY in _cache:
        return _cache[PMKSY_CACHE_KEY]
    data = await asyncio.to_thread(scrape_pmksy)
    _cache[PMKSY_CACHE_KEY] = {
        "data": data,
        "live": any(d.get("live") for d in data),
        "scraped_at": data[0].get("scraped_at") if data else None,
    }
    return _cache[PMKSY_CACHE_KEY]


def _latest_scraped(items):
    ts = max((s.get("scraped_at", "") for s in items if s.get("scraped_at")), default="")
    if not ts:
        return "Scraped Live"
    try:
        dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
        return dt.strftime("%d %b %Y, %H:%M UTC")
    except:
        return "Scraped Live"
