"""
scheme_dashboard_scraper.py
===========================
Normalizes district / state dashboard data for scheme cards used by the frontend.

This module prefers public dashboard data when it is available. For sources that
are publicly reachable but do not expose scrapeable metrics at runtime, it
returns a truthful "limited" record so the UI can show the live source link and
current availability state instead of inventing numbers.
"""

from __future__ import annotations

import logging
import re
from datetime import datetime, timezone

import requests
from bs4 import BeautifulSoup

from .jjm_scraper import scrape_jjm
from .pmksy_scraper import scrape_pmksy
from .pmkisan_scraper import scrape_pmkisan
from .scholarship_scraper import scrape_scholarship
from .pmjdy_scraper import scrape_pmjdy
from .pmgdisha_scraper import scrape_pmgdisha
from .saubhagya_scraper import scrape_saubhagya
from .mgnrega_scraper import scrape_mgnrega
from .pmfby_scraper import scrape_pmfby
from .pmayg_scraper import scrape_pmayg
from .sbmg_scraper import scrape_sbmg

log = logging.getLogger("scraper.scheme_dashboards")

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "en-IN,en;q=0.9",
}


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _clean_text(value: str | None) -> str:
    return re.sub(r"\s+", " ", str(value or "")).strip()


def _to_float(value) -> float | None:
    if value is None:
        return None
    try:
        return float(str(value).replace(",", "").replace("%", "").strip())
    except Exception:
        return None


def _time_state(url: str, source_name: str, reason: str, scheme_id: str, label: str, description: str) -> dict:
    return {
        "id": scheme_id,
        "label": label,
        "icon": "•",
        "source": source_name,
        "source_url": url,
        "description": description,
        "live": False,
        "status": "limited",
        "status_label": "Source limited",
        "scraped_at": _now_iso(),
        "verified_label": "Official source",
        "report_label": None,
        "note": reason,
        "summary": {
            "primary": "—",
            "primaryLabel": "Metric unavailable",
            "good": 0,
            "goodLabel": "Live rows",
            "watch": 0,
            "watchLabel": "Awaiting parse",
            "critical": 0,
            "criticalLabel": "Blocked",
        },
        "columns": [],
        "rows": [],
        "row_count": 0,
    }


def _jjm_dashboard() -> dict:
    rows = scrape_jjm()
    live_rows = [row for row in rows if isinstance(row.get("coverage"), (int, float))]
    state_avg = round(sum(row["coverage"] for row in live_rows) / len(live_rows), 1) if live_rows else None

    normalized_rows = []
    for row in rows:
        coverage = _to_float(row.get("coverage")) or 0.0
        tone = "good" if coverage >= 70 else "watch" if coverage >= 50 else "critical"
        status = "On track" if tone == "good" else "Needs push" if tone == "watch" else "Critical"
        normalized_rows.append({
            "district": row.get("name"),
            "population": row.get("pop") or "—",
            "coverage_pct": round(coverage, 1),
            "status": status,
            "status_tone": tone,
            "source_url": row.get("source"),
        })

    return {
        "id": "jal_shakti",
        "label": "Jal Shakti",
        "icon": "💧",
        "source": "ejalshakti.gov.in",
        "source_url": "https://ejalshakti.gov.in/jjmreport/JJMIndia.aspx",
        "description": "JJM tap water coverage by district",
        "live": any(row.get("live") for row in rows),
        "status": "ok" if rows else "limited",
        "status_label": "Live data" if rows else "Source limited",
        "scraped_at": rows[0].get("scraped_at") if rows else _now_iso(),
        "verified_label": "Official source",
        "report_label": None,
        "note": "Functional tap water coverage from the Jal Jeevan Mission public dashboard.",
        "summary": {
            "primary": f"{state_avg:.1f}%" if state_avg is not None else "—",
            "primaryLabel": "State Average Coverage",
            "good": sum(1 for row in normalized_rows if row["coverage_pct"] >= 70),
            "goodLabel": "Districts >70%",
            "watch": sum(1 for row in normalized_rows if 50 <= row["coverage_pct"] < 70),
            "watchLabel": "Districts 50–70%",
            "critical": sum(1 for row in normalized_rows if row["coverage_pct"] < 50),
            "criticalLabel": "Districts <50% (critical)",
        },
        "columns": [
            {"key": "district", "label": "District", "type": "text"},
            {"key": "population", "label": "Population", "type": "text"},
            {"key": "coverage_pct", "label": "Tap Water Coverage", "type": "progress"},
            {"key": "status", "label": "Status", "type": "status"},
        ],
        "rows": normalized_rows,
        "row_count": len(normalized_rows),
    }


def _pmksy_dashboard() -> dict:
    rows = scrape_pmksy()
    normalized_rows = []
    for row in rows:
        normalized_rows.append({
            "district": row.get("name"),
            "net_area_sown": row.get("net_area_sown_display"),
            "net_irrigated_area": row.get("net_irrigated_area_display"),
            "coverage_pct": _to_float(row.get("coverage_pct")) or 0.0,
            "status": row.get("status") or "—",
            "status_tone": row.get("status_tone") or "watch",
            "source_url": row.get("source_url"),
        })

    state_avg = _to_float(rows[0].get("state_average")) if rows else None

    return {
        "id": "pmksy",
        "label": "PMKSY",
        "icon": "🌾",
        "source": "rajas.rajasthan.gov.in",
        "source_url": rows[0].get("source_url") if rows else "https://rajas.rajasthan.gov.in/",
        "description": "District irrigation coverage from Rajasthan Agriculture Statistics",
        "live": any(row.get("live") for row in rows),
        "status": "ok" if rows else "limited",
        "status_label": "Live data" if rows else "Source limited",
        "scraped_at": rows[0].get("scraped_at") if rows else _now_iso(),
        "verified_label": "Verified Official Data",
        "report_label": rows[0].get("report_label") if rows else "2022-23 Annual Report",
        "note": (
            "Coverage is computed as net irrigated area divided by net area sown "
            "for each district."
        ),
        "summary": {
            "primary": f"{state_avg:.1f}%" if state_avg is not None else "—",
            "primaryLabel": "State Average Irrigation Coverage",
            "good": sum(1 for row in normalized_rows if row["coverage_pct"] >= 65),
            "goodLabel": "Districts >65%",
            "watch": sum(1 for row in normalized_rows if 40 <= row["coverage_pct"] < 65),
            "watchLabel": "Districts 40–65%",
            "critical": sum(1 for row in normalized_rows if row["coverage_pct"] < 40),
            "criticalLabel": "Districts <40% (critical)",
        },
        "columns": [
            {"key": "district", "label": "District", "type": "text"},
            {"key": "net_area_sown", "label": "Net Area Sown (Lakh Ha)", "type": "text"},
            {"key": "net_irrigated_area", "label": "Net Irrigated Area (Lakh Ha)", "type": "text"},
            {"key": "coverage_pct", "label": "Irrigation Coverage", "type": "progress"},
            {"key": "status", "label": "Status", "type": "status"},
        ],
        "rows": normalized_rows,
        "row_count": len(normalized_rows),
    }


def _parse_js_objects(html: str, required_key: str) -> list[dict]:
    objects = []
    for match in re.finditer(r"\{[^{}]*" + re.escape(required_key) + r"[^{}]*\}", html, re.I | re.S):
        raw = match.group(0)
        pairs = re.findall(r"['\"]?([A-Za-z0-9_]+)['\"]?\s*:\s*'([^']*)'", raw)
        if pairs:
            objects.append({key: value for key, value in pairs})
    return objects


def _sbmg_dashboard() -> dict:
    url = "https://sbm.gov.in/sbmgdashboard/StatesDashboard.aspx"
    try:
        response = requests.get(url, timeout=30, verify=False, headers=HEADERS)
        response.raise_for_status()
        html = response.text
    except Exception as exc:
        log.warning("SBM-G scrape failed: %s", exc)
        return _time_state(
            url,
            "sbm.gov.in",
            "The SBM-G dashboard did not respond cleanly during this refresh.",
            "sbmg",
            "SBM-G",
            "ODF Plus star-village dashboard for Rajasthan districts",
        )

    objects = _parse_js_objects(html, "STCODE11")
    state_row = next(
        (
            item for item in objects
            if item.get("STCODE11") == "08" and not item.get("dtname")
        ),
        None,
    )
    district_rows = [
        item for item in objects
        if item.get("STCODE11") == "08" and item.get("dtname")
    ]

    if not state_row:
        return _time_state(
            url,
            "sbm.gov.in",
            "The public dashboard loaded but the Rajasthan state row could not be parsed.",
            "sbmg",
            "SBM-G",
            "ODF Plus star-village dashboard for Rajasthan districts",
        )

    normalized_rows = []
    for item in district_rows:
        coverage = _to_float(item.get("TotalStarVillagePer")) or 0.0
        tone = "good" if coverage >= 5 else "watch" if coverage >= 1 else "critical"
        status = "On track" if tone == "good" else "Needs push" if tone == "watch" else "Critical"
        normalized_rows.append({
            "district": (item.get("dtname") or "").title(),
            "villages": item.get("TotalVillages") or "—",
            "star_villages": item.get("TotalStarVillage") or "—",
            "coverage_pct": round(coverage, 2),
            "status": status,
            "status_tone": tone,
            "source_url": url,
        })

    state_pct = _to_float(state_row.get("TotalStarVillagePer"))
    districts_total = int(_to_float(state_row.get("TotalNoDistrict")) or len(normalized_rows))
    total_blocks = int(_to_float(state_row.get("TotalNoOfBlocks")) or 0)
    total_villages = int(_to_float(state_row.get("TotalVillages")) or 0)

    return {
        "id": "sbmg",
        "label": "SBM-G",
        "icon": "🚰",
        "source": "sbm.gov.in",
        "source_url": url,
        "description": "ODF Plus star-village progress by district",
        "live": True,
        "status": "ok",
        "status_label": "Live data",
        "scraped_at": _now_iso(),
        "verified_label": "Official source",
        "report_label": None,
        "note": "Star-village coverage comes from the SBM-G public district dashboard.",
        "summary": {
            "primary": f"{state_pct:.2f}%" if state_pct is not None else "—",
            "primaryLabel": "Star-Village Coverage",
            "good": sum(1 for row in normalized_rows if row["coverage_pct"] >= 5),
            "goodLabel": "Districts >5%",
            "watch": sum(1 for row in normalized_rows if 1 <= row["coverage_pct"] < 5),
            "watchLabel": "Districts 1–5%",
            "critical": sum(1 for row in normalized_rows if row["coverage_pct"] < 1),
            "criticalLabel": "Districts <1%",
            "meta": {
                "districts": districts_total,
                "blocks": total_blocks,
                "villages": total_villages,
            },
        },
        "columns": [
            {"key": "district", "label": "District", "type": "text"},
            {"key": "villages", "label": "Total Villages", "type": "text"},
            {"key": "star_villages", "label": "Star Villages", "type": "text"},
            {"key": "coverage_pct", "label": "Star Coverage", "type": "progress"},
            {"key": "status", "label": "Status", "type": "status"},
        ],
        "rows": normalized_rows,
        "row_count": len(normalized_rows),
    }



def scrape_scheme_dashboards() -> list[dict]:
    dashboards = [
        _jjm_dashboard(),
        _pmksy_dashboard(),
        scrape_pmkisan(),
        scrape_sbmg(),
        scrape_scholarship(),
        scrape_pmjdy(),
        scrape_pmgdisha(),
        scrape_saubhagya(),
        scrape_mgnrega(),
        scrape_pmfby(),
        scrape_pmayg(),
    ]

    for dashboard in dashboards:
        dashboard["row_count"] = len(dashboard.get("rows") or [])
    return dashboards
