"""
igod_scraper.py
Scrapes igod.gov.in for the Rajasthan government portal directory and enriches
each listed portal with lightweight homepage metadata.
"""
import logging
import re
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from urllib.parse import urlparse

import requests
import urllib3
from bs4 import BeautifulSoup

urllib3.disable_warnings()

log = logging.getLogger("scraper.igod")
BASE_URL = "https://igod.gov.in"
IGOD_URL = "https://igod.gov.in/sg/RJ/SPMA/organizations"
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/122.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-IN,en;q=0.9,hi;q=0.8",
}

CAT_MAP = {
    r"jan soochna|jansoochna|soochna": "Transparency & RTI",
    r"labour|ldms|worker": "Labour & Employment",
    r"pregnancy|child|pcts|health|medical": "Health & Family Welfare",
    r"pushkar|fair|mela": "Tourism & Culture",
    r"invest|nivesh|rising": "Industry & Investment",
    r"civil registration|pehchan|birth|death": "Civil Registration",
    r"farmer|agri|kisan|rjfr|rjfrc": "Agriculture & Farmers",
    r"recruitment|job": "Recruitment",
    r"wam|accounts|work account": "Finance & Accounts",
}


def _clean_text(text):
    return re.sub(r"\s+", " ", str(text or "")).strip()


def _cat(*parts):
    text = " ".join(_clean_text(part) for part in parts if part).lower()
    for pat, category in CAT_MAP.items():
        if re.search(pat, text, re.I):
            return category
    return "Government Services"


def _extract_directory_last_updated(soup):
    text = _clean_text(soup.get_text(" ", strip=True))
    match = re.search(
        r"(last\s+updated|updated\s+on|last\s+modified)\s*[:\-]?\s*([A-Za-z0-9,\-/ ]{6,40})",
        text,
        re.I,
    )
    return _clean_text(match.group(2)) if match else ""


def _normalize_portal(name, href, category, ts, position, source="igod.gov.in"):
    domain = urlparse(href).netloc.lower()
    summary = f"Official Rajasthan government portal listed in the IGOD directory under {category}."
    return {
        "id": f"igod_{position}",
        "position": position,
        "name": name,
        "organization_name": name,
        "portal_title": name,
        "department": "Government of Rajasthan",
        "ministry": "Government of Rajasthan",
        "category": category,
        "url": href,
        "website_url": href,
        "domain": domain,
        "description": summary,
        "summary": summary,
        "portal_type": "Government Portal",
        "content_type": "",
        "page_title": "",
        "meta_description": "",
        "redirect_url": href,
        "status_code": None,
        "response_time_ms": None,
        "is_https": href.lower().startswith("https://"),
        "status": "Active",
        "source": source,
        "directory_url": IGOD_URL,
        "directory_last_updated": "",
        "total_portals_listed": 0,
        "last_checked_at": ts,
        "scraped_at": ts,
    }


def _extract_portal_meta(portal):
    checked_at = datetime.now(timezone.utc).isoformat()
    started = time.perf_counter()
    try:
        response = requests.get(
            portal["url"],
            headers={**HEADERS, "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8"},
            timeout=10,
            verify=False,
            allow_redirects=True,
        )
        elapsed_ms = int((time.perf_counter() - started) * 1000)
        final_url = response.url or portal["url"]
        content_type = response.headers.get("Content-Type", "").split(";")[0].strip().lower()
        soup = BeautifulSoup(response.text, "html.parser") if "html" in content_type else None
        page_title = _clean_text(soup.title.get_text(" ", strip=True)) if soup and soup.title else ""
        meta_description = ""
        if soup:
            meta = soup.find("meta", attrs={"name": re.compile(r"description", re.I)})
            if meta:
                meta_description = _clean_text(meta.get("content"))

        description = meta_description or page_title or portal.get("summary") or portal.get("description") or ""
        category = portal.get("category") or _cat(portal.get("name"), portal.get("domain"), page_title, meta_description)

        return {
            **portal,
            "category": category,
            "content_type": content_type,
            "page_title": page_title,
            "portal_title": page_title or portal.get("portal_title") or portal.get("name"),
            "meta_description": meta_description,
            "description": description,
            "summary": description,
            "redirect_url": final_url,
            "status_code": response.status_code,
            "response_time_ms": elapsed_ms,
            "is_https": final_url.lower().startswith("https://"),
            "status": "Active" if response.ok else "Inactive",
            "domain": urlparse(final_url).netloc.lower() or portal.get("domain", ""),
            "last_checked_at": checked_at,
        }
    except Exception as exc:
        log.debug("IGOD portal enrichment failed for %s: %s", portal.get("url"), exc)
        return {
            **portal,
            "status": "Unreachable",
            "last_checked_at": checked_at,
        }


def _enrich_portals(portals):
    if not portals:
        return portals

    enriched = [None] * len(portals)
    max_workers = min(6, len(portals))
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {
            executor.submit(_extract_portal_meta, portal): idx
            for idx, portal in enumerate(portals)
        }
        for future in as_completed(futures):
            idx = futures[future]
            try:
                enriched[idx] = future.result()
            except Exception as exc:
                log.debug("IGOD enrichment future failed for row %s: %s", idx, exc)
                enriched[idx] = portals[idx]

    return [portal for portal in enriched if portal]


def scrape_igod():
    ts = datetime.now(timezone.utc).isoformat()
    session = requests.Session()
    log.info("Fetching IGOD Organizations: %s", IGOD_URL)

    portals = []
    seen = set()
    position = 1
    current_url = IGOD_URL
    directory_last_updated = ""

    while current_url:
        try:
            log.info("Fetching %s...", current_url)
            response = session.get(current_url, headers=HEADERS, timeout=15, verify=False)
            response.raise_for_status()
            soup = BeautifulSoup(response.text, "html.parser")
            directory_last_updated = directory_last_updated or _extract_directory_last_updated(soup)
        except Exception as exc:
            log.error("IGOD fetch failed on %s: %s", current_url, exc)
            data = _fallback(ts)
            for portal in data:
                portal["directory_last_updated"] = directory_last_updated
                portal["total_portals_listed"] = len(data)
            return data if not portals else portals

        search_rows = soup.find_all("div", class_="search-result-row")
        for row in search_rows:
            link_tag = row.find("a", class_="search-title")
            if not link_tag:
                continue

            href = _clean_text(link_tag.get("href", ""))
            name = _clean_text(
                link_tag.get_text(" ", strip=True).replace("External link that opens in a new window", "")
            )

            if not href or not name or not href.startswith("http"):
                continue

            norm = href.rstrip("/").split("#")[0].lower()
            if norm in seen:
                continue
            seen.add(norm)

            portals.append(
                _normalize_portal(
                    name=name,
                    href=href,
                    category=_cat(name, urlparse(href).netloc.lower()),
                    ts=ts,
                    position=position,
                )
            )
            position += 1

        next_link_tag = soup.find("a", string=re.compile(r"Next", re.I))
        if next_link_tag and next_link_tag.get("href"):
            next_url = next_link_tag.get("href")
            if next_url.startswith("/"):
                current_url = BASE_URL + next_url
            elif next_url.startswith("http"):
                current_url = next_url
            else:
                current_url = None
            time.sleep(1)
            continue

        pagination = soup.find("ul", class_="pagination")
        next_li = pagination.find("li", class_="next") if pagination else None
        next_anchor = next_li.find("a") if next_li else None
        if next_anchor and next_anchor.get("href"):
            next_url = next_anchor.get("href")
            if next_url.startswith("/"):
                current_url = BASE_URL + next_url
            elif next_url.startswith("http"):
                current_url = next_url
            else:
                current_url = None
            time.sleep(1)
            continue

        current_url = None

    if not portals:
        return _fallback(ts)

    total_listed = len(portals)
    portals = _enrich_portals(portals)
    for portal in portals:
        portal["directory_last_updated"] = directory_last_updated
        portal["total_portals_listed"] = total_listed

    log.info("IGOD: %d organizations found", len(portals))
    return portals


def _fallback(ts):
    known = [
        ("Jan Soochna Portal", "https://jansoochna.rajasthan.gov.in", "Transparency & RTI"),
        ("Labour Department Management System (LDMS), Rajasthan", "https://ldms.rajasthan.gov.in", "Labour & Employment"),
        ("Pregnancy, Child Tracking & Health Services Management System (PCTS), Rajasthan", "https://pctsrajmedical.rajasthan.gov.in", "Health & Family Welfare"),
        ("Pushkar Fair, Rajasthan", "https://pushkarmela.rajasthan.gov.in", "Tourism & Culture"),
        ("Raj Nivesh Portal, Rajasthan", "https://rajnivesh.rajasthan.gov.in", "Industry & Investment"),
        ("Rajasthan Civil Registration System", "https://pehchan.raj.nic.in", "Civil Registration"),
        ("Rajasthan Farmer Registry", "https://rjfr.agristack.gov.in/farmer-registry-rj/#", "Agriculture & Farmers"),
        ("Rajasthan Farmer Registry Camps Portal", "https://rjfrc.rajasthan.gov.in", "Agriculture & Farmers"),
        ("Rajasthan Recruitment Portal", "https://recruitment.rajasthan.gov.in", "Recruitment"),
        ("Rising Rajasthan Global Investment Summit, Rajasthan", "https://rising.rajasthan.gov.in", "Industry & Investment"),
        ("Work Accounts Management System (WAM), Rajasthan", "https://wam.rajasthan.gov.in", "Finance & Accounts"),
    ]
    data = [
        _normalize_portal(name, url, category, ts, i + 1, source="igod.gov.in (fallback)")
        for i, (name, url, category) in enumerate(known)
    ]
    for portal in data:
        portal["total_portals_listed"] = len(data)
    return data
