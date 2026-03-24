"""
Production-ready Jan Soochna scheme scraper.

Pipeline:
1. Fetch scheme records from Jan Soochna API endpoints or HTML fallback
2. Normalize and deduplicate scheme rows
3. Optionally visit individual scheme/detail pages when available
4. Extract structured fields and save dataset to backend/data/jansoochna_schemes.json
"""

from __future__ import annotations

import json
import logging
import re
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple
from urllib.parse import parse_qs, urljoin, urlparse

import requests
from bs4 import BeautifulSoup, Tag
from requests.adapters import HTTPAdapter
from tqdm import tqdm
from urllib3.util.retry import Retry
import urllib3


BASE_URL = "https://jansoochna.rajasthan.gov.in"
INDEX_URL = f"{BASE_URL}/Scheme"
OUTPUT_PATH = Path(__file__).resolve().parents[1] / "data" / "jansoochna_schemes.json"
REQUEST_TIMEOUT = 20
REQUEST_DELAY_SECONDS = 0.8

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/123.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "en-IN,en;q=0.9",
    "Referer": BASE_URL,
    "Origin": BASE_URL,
    "Connection": "keep-alive",
}

LIST_ENDPOINTS = [
    f"{BASE_URL}/api/Scheme/getAllScheme",
    f"{BASE_URL}/api/Scheme/getSchemeList",
    f"{BASE_URL}/api/scheme/list",
    f"{BASE_URL}/api/schemes",
]

DETAIL_ENDPOINT_PATTERNS = [
    f"{BASE_URL}/api/Scheme/getSchemeById/{{scheme_id}}",
    f"{BASE_URL}/api/Scheme/getSchemeDetails/{{scheme_id}}",
    f"{BASE_URL}/api/scheme/details/{{scheme_id}}",
]

SECTION_KEYWORDS = {
    "benefits": ["benefit", "benefits", "लाभ", "assistance", "subsidy", "incentive"],
    "eligibility": ["eligibility", "eligible", "criteria", "पात्रता", "beneficiary"],
    "documents_required": ["document", "documents", "दस्तावेज", "certificate", "proof"],
}

CATEGORY_MAP = [
    (r"social justice|palanhar|pension|widow|disabled|specially", "Social Welfare"),
    (r"health|medical|ayushman|chiranjeevi|dawa|hospital", "Health"),
    (r"agriculture|kisan|fasal|crop|farmer", "Agriculture"),
    (r"food|ration|pds|fps|nfsa|rasoi", "Food Security"),
    (r"labou?r|worker|rozgar|employment", "Labour & Employment"),
    (r"education|school|scholarship|student", "Education"),
    (r"mgnrega|rural|panchayat", "Rural Development"),
    (r"doit|e-mitra|emitra|technology|digital", "Digital Services"),
    (r"mining|dmft|mineral", "Mining"),
    (r"electricity|vidyut|power|energy|solar", "Energy"),
    (r"water|jal|jjm|sanitation|swachh", "Water & Sanitation"),
    (r"jan aadhaar|bhamashah|identity", "Identity & Social Security"),
    (r"urban|municipal|housing|awas", "Urban Development"),
    (r"women|mahila|beti|ladli|maternity", "Women & Child"),
]

PROGRESS_KEYWORDS = [
    "implementation",
    "progress",
    "coverage",
    "covered",
    "completion",
    "achieved",
    "target",
    "beneficiaries covered",
    "registered",
]

BENEFICIARY_KEYWORDS = [
    "beneficiary",
    "beneficiaries",
    "families",
    "family",
    "households",
    "household",
    "students",
    "women",
    "children",
    "workers",
    "citizens",
    "people",
    "farmers",
    "patients",
    "coverage",
    "covered",
    "distributed",
    "status",
]

BUDGET_KEYWORDS = [
    "budget",
    "allocation",
    "outlay",
    "fund",
    "funds",
    "grant",
    "subsidy",
    "financial assistance",
    "assistance",
    "scholarship",
    "stipend",
    "loan",
    "insurance",
    "cover",
    "per month",
    "per year",
    "per annum",
]


log = logging.getLogger("scraper.jansoochna_full")


@dataclass
class ScraperConfig:
    timeout: int = REQUEST_TIMEOUT
    polite_delay: float = REQUEST_DELAY_SECONDS


def _get_session() -> requests.Session:
    session = requests.Session()
    retry = Retry(
        total=4,
        backoff_factor=0.8,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=frozenset(["GET"]),
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry, pool_connections=20, pool_maxsize=20)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    session.headers.update(HEADERS)
    return session


def _clean_text(text: Optional[str]) -> str:
    if not text:
        return ""
    return re.sub(r"\s+", " ", str(text)).strip()


def _is_valid_detail_url(url: str) -> bool:
    if not url:
        return False
    lowered = url.lower()
    if lowered.startswith(("javascript:", "mailto:", "tel:", "#")):
        return False
    return lowered.startswith("http") and "jansoochna.rajasthan.gov.in" in lowered


def _detect_category(*values: Optional[str]) -> Optional[str]:
    text = " ".join(_clean_text(v) for v in values if v).lower()
    if not text:
        return None
    for pattern, label in CATEGORY_MAP:
        if re.search(pattern, text, re.IGNORECASE):
            return label
    return None


def _extract_scheme_id(raw: Dict[str, object], url: str) -> Optional[str]:
    for key in ("SchemeId", "scheme_id", "id", "ServiceId", "service_id"):
        value = raw.get(key)
        if value not in (None, ""):
            return str(value)
    if url:
        parsed = urlparse(url)
        qs = parse_qs(parsed.query)
        for key in ("q", "id", "schemeId", "serviceId"):
            values = qs.get(key)
            if values:
                return str(values[0])
        tail = parsed.path.rstrip("/").split("/")[-1]
        if tail and tail.lower() not in {"scheme", "services"}:
            return tail
    return None


def _coerce_list(value: object) -> List[str]:
    if isinstance(value, list):
        return [_clean_text(item) for item in value if _clean_text(item)]
    if isinstance(value, str):
        text = _clean_text(value)
        if not text:
            return []
        parts = re.split(r"[;\n]|(?:\s{2,})", text)
        return [_clean_text(part) for part in parts if _clean_text(part)]
    return []


def _iter_sentences(text: str) -> List[str]:
    return [segment.strip() for segment in re.split(r"(?<=[.!?])\s+|\s*•\s*", _clean_text(text)) if segment.strip()]


def _extract_budget(lines: List[str]) -> Optional[str]:
    amount_pattern = (
        r"((?:₹|Rs\.?|INR)\s*[\d,]+(?:\.\d+)?\s*(?:lakh\s*crore|crore|cr|lakh)?"
        r"(?:\s*(?:per month|per year|per annum|monthly|annually|/month|/year|/yr|/mo))?)"
    )
    best_match: Optional[str] = None
    best_score = -1
    for raw_line in lines:
        for sentence in _iter_sentences(raw_line):
            lowered = sentence.lower()
            match = re.search(amount_pattern, sentence, re.IGNORECASE)
            if not match:
                continue
            kw_hits = sum(1 for keyword in BUDGET_KEYWORDS if keyword in lowered)
            if kw_hits == 0 and not re.search(r"(?:₹|rs\.?|inr)", sentence, re.IGNORECASE):
                continue
            score = kw_hits
            if re.search(r"(?:benefit|coverage|insurance|assistance|subsidy|scholarship|stipend|loan)", lowered):
                score += 2
            if score > best_score:
                best_score = score
                best_match = _clean_text(match.group(1)).rstrip(" .,:;")
    return best_match


def _extract_beneficiaries(lines: List[str]) -> Optional[str]:
    count_patterns = [
        r"((?:around\s+|about\s+|over\s+|more than\s+|nearly\s+|upto\s+|up to\s+)?\d+(?:\.\d+)?\s*(?:crore|lakh|lakhs)\s+(?:families|family|households|people|citizens|students|women|children|farmers|workers|patients|beneficiaries))",
        r"((?:around\s+|about\s+|over\s+|more than\s+|nearly\s+|upto\s+|up to\s+)?\d[\d,]*(?:\.\d+)?\s+(?:families|family|households|people|citizens|students|women|children|farmers|workers|patients|beneficiaries))",
        r"((?:benefits?|assistance|services?)\s+distributed\s+to\s+(?:around\s+|about\s+|over\s+|more than\s+|nearly\s+)?\d+(?:\.\d+)?\s*(?:crore|lakh|lakhs)?\s*(?:families|family|households|people|citizens|beneficiaries))",
    ]
    best_match: Optional[str] = None
    best_score = -1
    for raw_line in lines:
        for sentence in _iter_sentences(raw_line):
            lowered = sentence.lower()
            kw_hits = sum(1 for keyword in BENEFICIARY_KEYWORDS if keyword in lowered)
            for pattern in count_patterns:
                match = re.search(pattern, sentence, re.IGNORECASE)
                if not match:
                    continue
                score = kw_hits
                if re.search(r"(?:covered|registered|distributed|eligible|approved)", lowered):
                    score += 2
                if score > best_score:
                    best_score = score
                    best_match = _clean_text(match.group(1)).rstrip(" .,:;")
    return best_match


def _parse_beneficiary_count(raw_value: object) -> Optional[int]:
    text = _clean_text(str(raw_value)) if raw_value not in (None, "") else ""
    if not text:
        return None
    numeric = re.sub(r"[^\d.]", "", text)
    if not numeric:
        return None
    try:
        value = float(numeric)
    except ValueError:
        return None
    lowered = text.lower()
    if "crore" in lowered:
        value *= 10_000_000
    elif "lakh" in lowered or "lac" in lowered:
        value *= 100_000
    return int(value) if value > 0 else None


def _extract_progress_signal(lines: List[str]) -> Tuple[Optional[float], Optional[str]]:
    best_score = -1
    best_pct: Optional[float] = None
    best_source: Optional[str] = None
    for line in lines:
        line_lower = line.lower()
        pct_matches = re.findall(r"(\d{1,3}(?:\.\d+)?)\s*%", line)
        if not pct_matches:
            continue
        kw_hits = sum(1 for kw in PROGRESS_KEYWORDS if kw in line_lower)
        if kw_hits == 0:
            continue
        for raw in pct_matches:
            pct = float(raw)
            if 0 <= pct <= 100 and kw_hits > best_score:
                best_score = kw_hits
                best_pct = round(pct, 2)
                best_source = line[:240]
    return best_pct, best_source


def _extract_main_content(soup: BeautifulSoup) -> Tag:
    content = (
        soup.select_one(".container")
        or soup.select_one("main")
        or soup.select_one("article")
        or soup.body
    )
    return content if isinstance(content, Tag) else soup


def _iter_section_chunks(content: Tag) -> List[Tuple[str, List[str]]]:
    chunks: List[Tuple[str, List[str]]] = []
    current_heading = "General"
    current_lines: List[str] = []

    for node in content.descendants:
        if not isinstance(node, Tag):
            continue
        if node.name in {"h1", "h2", "h3", "h4", "strong"}:
            heading_text = _clean_text(node.get_text(" ", strip=True))
            if heading_text and len(heading_text) <= 100:
                if current_lines:
                    chunks.append((current_heading, current_lines))
                    current_lines = []
                current_heading = heading_text
                continue
        if node.name == "p":
            text = _clean_text(node.get_text(" ", strip=True))
            if len(text) > 20:
                current_lines.append(text)
        elif node.name in {"ul", "ol"}:
            for li in node.find_all("li", recursive=False):
                li_text = _clean_text(li.get_text(" ", strip=True))
                if li_text:
                    current_lines.append(li_text)
    if current_lines:
        chunks.append((current_heading, current_lines))
    return chunks


def extract_sections(soup: BeautifulSoup) -> Dict[str, Optional[object]]:
    content = _extract_main_content(soup)
    chunks = _iter_section_chunks(content)
    headings: List[str] = []
    description_parts: List[str] = []
    benefits: List[str] = []
    eligibility: List[str] = []
    documents_required: List[str] = []
    all_lines: List[str] = []

    for heading, lines in chunks:
        if heading and heading != "General":
            headings.append(heading)
        all_lines.extend(lines)
        if not description_parts and lines:
            description_parts.extend(lines[:2])
        heading_lower = heading.lower()
        if any(key in heading_lower for key in SECTION_KEYWORDS["benefits"]):
            benefits.extend(lines)
        if any(key in heading_lower for key in SECTION_KEYWORDS["eligibility"]):
            eligibility.extend(lines)
        if any(key in heading_lower for key in SECTION_KEYWORDS["documents_required"]):
            documents_required.extend(lines)

    if not benefits:
        benefits = [line for line in all_lines if any(k in line.lower() for k in SECTION_KEYWORDS["benefits"])]
    if not eligibility:
        eligibility = [line for line in all_lines if any(k in line.lower() for k in SECTION_KEYWORDS["eligibility"])]
    if not documents_required:
        documents_required = [line for line in all_lines if any(k in line.lower() for k in SECTION_KEYWORDS["documents_required"])]

    def uniq(items: List[str]) -> List[str]:
        seen: Set[str] = set()
        result: List[str] = []
        for item in items:
            cleaned = _clean_text(item)
            if cleaned and cleaned not in seen:
                seen.add(cleaned)
                result.append(cleaned)
        return result

    progress_pct, progress_source = _extract_progress_signal(all_lines)

    return {
        "description": _clean_text(" ".join(description_parts[:3])) or None,
        "headings": uniq(headings) or None,
        "benefits": uniq(benefits) or None,
        "eligibility": uniq(eligibility) or None,
        "documents_required": uniq(documents_required) or None,
        "beneficiaries": _extract_beneficiaries(all_lines),
        "budget": _extract_budget(all_lines),
        "progress_pct": progress_pct,
        "progress_source": progress_source,
    }


def _extract_items_from_payload(payload: object) -> List[Dict[str, object]]:
    if isinstance(payload, list):
        return [item for item in payload if isinstance(item, dict)]
    if isinstance(payload, dict):
        for key in ("data", "schemes", "result", "Schemes", "items"):
            value = payload.get(key)
            if isinstance(value, list):
                return [item for item in value if isinstance(item, dict)]
    return []


def get_scheme_records(
    session: Optional[requests.Session] = None,
    config: Optional[ScraperConfig] = None,
) -> List[Dict[str, object]]:
    config = config or ScraperConfig()
    own_session = session is None
    session = session or _get_session()
    try:
        for endpoint in LIST_ENDPOINTS:
            try:
                response = session.get(endpoint, timeout=config.timeout, verify=False)
                if response.status_code != 200:
                    continue
                items = _extract_items_from_payload(response.json())
                if items:
                    log.info("Collected %s Jan Soochna records from %s", len(items), endpoint)
                    return items
            except Exception as exc:
                log.debug("Jan Soochna endpoint %s failed: %s", endpoint, exc)

        try:
            response = session.get(INDEX_URL, timeout=config.timeout, verify=False)
            response.raise_for_status()
            soup = BeautifulSoup(response.text, "html.parser")
            items: List[Dict[str, object]] = []
            for anchor in soup.find_all("a", href=True):
                href = anchor.get("href", "").strip()
                title = _clean_text(anchor.get_text(" ", strip=True))
                if len(title) < 5:
                    continue
                absolute_url = urljoin(BASE_URL, href)
                if not _is_valid_detail_url(absolute_url):
                    continue
                if not any(token in absolute_url.lower() for token in ("/services", "q=", "/scheme/")):
                    continue
                if title.endswith(":"):
                    continue
                if any(token in title.lower() for token in ("home", "about", "contact", "login", "back", "information of schemes")):
                    continue
                items.append({"SchemeName": title, "SchemeURL": absolute_url})
            if items:
                log.info("Collected %s Jan Soochna records from HTML fallback", len(items))
                return items
        except Exception as exc:
            log.warning("Jan Soochna HTML fallback failed: %s", exc)

        return []
    finally:
        if own_session:
            session.close()


def _normalize_record(raw: Dict[str, object], index: int) -> Dict[str, object]:
    name = _clean_text(
        raw.get("SchemeName")
        or raw.get("scheme_name")
        or raw.get("name")
        or raw.get("SchemeTitle")
        or raw.get("title")
        or f"Scheme {index + 1}"
    )
    url = _clean_text(raw.get("SchemeURL") or raw.get("detail_url") or raw.get("url"))
    department = _clean_text(raw.get("DepartmentName") or raw.get("department") or raw.get("dept_name"))
    description = _clean_text(raw.get("description") or raw.get("SchemeDescription"))
    scheme_id = _extract_scheme_id(raw, url)
    if not url and scheme_id:
        url = f"{BASE_URL}/Services?q={scheme_id}"
    if not url:
        url = INDEX_URL
    if not _is_valid_detail_url(url) and url != INDEX_URL:
        url = INDEX_URL

    benefits = _coerce_list(raw.get("benefits") or raw.get("Benefit"))
    eligibility = _coerce_list(raw.get("eligibility") or raw.get("Eligibility"))
    documents_required = _coerce_list(raw.get("documents_required") or raw.get("DocumentsRequired"))
    progress_pct, progress_source = _extract_progress_signal(
        [_clean_text(description), *benefits, *eligibility, *documents_required]
    )
    beneficiary_count = (
        _parse_beneficiary_count(raw.get("beneficiary_count"))
        or _parse_beneficiary_count(raw.get("BeneficiaryCount"))
    )
    beneficiaries = _extract_beneficiaries(
        [_clean_text(description), *benefits, *eligibility, *documents_required]
    )
    budget = _extract_budget(
        [_clean_text(description), *benefits, *eligibility, *documents_required]
    )

    return {
        "id": f"jsp_full_{index + 1:03d}",
        "scheme_id": scheme_id,
        "name": name or f"Scheme {index + 1}",
        "category": _detect_category(name, department, description) or "General Services",
        "department": department or None,
        "description": description or None,
        "benefits": benefits or None,
        "eligibility": eligibility or None,
        "documents_required": documents_required or None,
        "beneficiary_count": beneficiary_count,
        "beneficiaries": beneficiaries,
        "budget": budget,
        "progress_pct": progress_pct,
        "progress": f"{progress_pct}%" if isinstance(progress_pct, (int, float)) else None,
        "progress_source": progress_source,
        "source": "Jan Soochna",
        "url": url,
    }


def _fetch_detail_payload(
    session: requests.Session,
    scheme_id: Optional[str],
    config: ScraperConfig,
) -> Optional[Dict[str, object]]:
    if not scheme_id:
        return None
    for endpoint in DETAIL_ENDPOINT_PATTERNS:
        url = endpoint.format(scheme_id=scheme_id)
        try:
            response = session.get(url, timeout=config.timeout, verify=False)
            if response.status_code != 200:
                continue
            payload = response.json()
            if isinstance(payload, dict):
                if isinstance(payload.get("data"), dict):
                    return payload["data"]
                return payload
        except Exception as exc:
            log.debug("Jan Soochna detail endpoint %s failed: %s", url, exc)
    return None


def scrape_scheme_page(
    record: Dict[str, object],
    session: Optional[requests.Session] = None,
    config: Optional[ScraperConfig] = None,
) -> Dict[str, object]:
    config = config or ScraperConfig()
    own_session = session is None
    session = session or _get_session()
    try:
        detail_payload = _fetch_detail_payload(session, record.get("scheme_id"), config)
        if detail_payload:
            merged = {**record}
            for key in ("description", "department"):
                if not merged.get(key):
                    merged[key] = _clean_text(detail_payload.get(key) or detail_payload.get(key.title()))
            if not merged.get("beneficiary_count"):
                merged["beneficiary_count"] = (
                    _parse_beneficiary_count(detail_payload.get("beneficiary_count"))
                    or _parse_beneficiary_count(detail_payload.get("BeneficiaryCount"))
                )
            if not merged.get("benefits"):
                merged["benefits"] = _coerce_list(detail_payload.get("benefits") or detail_payload.get("Benefit"))
            if not merged.get("eligibility"):
                merged["eligibility"] = _coerce_list(detail_payload.get("eligibility") or detail_payload.get("Eligibility"))
            if not merged.get("documents_required"):
                merged["documents_required"] = _coerce_list(
                    detail_payload.get("documents_required") or detail_payload.get("DocumentsRequired")
                )
            signal_lines = [
                _clean_text(merged.get("description")),
                *_coerce_list(detail_payload.get("description")),
                *_coerce_list(detail_payload.get("benefits")),
                *_coerce_list(detail_payload.get("Benefit")),
                *_coerce_list(detail_payload.get("eligibility")),
                *_coerce_list(detail_payload.get("Eligibility")),
            ]
            if not merged.get("beneficiaries"):
                merged["beneficiaries"] = _extract_beneficiaries(signal_lines)
            if not merged.get("budget"):
                merged["budget"] = _extract_budget(signal_lines)
            if merged.get("progress_pct") is None:
                merged["progress_pct"], merged["progress_source"] = _extract_progress_signal(signal_lines)
                if merged.get("progress_pct") is not None:
                    merged["progress"] = f"{merged['progress_pct']}%"
            return merged

        url = str(record.get("url") or "")
        if not url or url.rstrip("/") == INDEX_URL.rstrip("/"):
            return record

        response = session.get(url, timeout=config.timeout, verify=False)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, "html.parser")
        sections = extract_sections(soup)
        updated = {**record}
        for key in ("description", "benefits", "eligibility", "documents_required", "beneficiaries", "budget"):
            if not updated.get(key) and sections.get(key):
                updated[key] = sections[key]
        if not updated.get("category"):
            updated["category"] = _detect_category(
                str(updated.get("name") or ""),
                str(updated.get("department") or ""),
                str(updated.get("description") or ""),
            )
        if updated.get("progress_pct") is None and sections.get("progress_pct") is not None:
            updated["progress_pct"] = sections["progress_pct"]
            updated["progress"] = f"{sections['progress_pct']}%"
            updated["progress_source"] = sections["progress_source"]
        return updated
    except Exception as exc:
        log.debug("Failed to enrich Jan Soochna record %s: %s", record.get("name"), exc)
        return record
    finally:
        if own_session:
            session.close()


def save_json(data: List[Dict[str, object]], output_path: Path = OUTPUT_PATH) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)
    log.info("Saved %s Jan Soochna schemes to %s", len(data), output_path)


def run_scraper(
    output_path: Path = OUTPUT_PATH,
    config: Optional[ScraperConfig] = None,
) -> List[Dict[str, object]]:
    config = config or ScraperConfig()
    session = _get_session()
    run_ts = datetime.now(timezone.utc).isoformat()
    try:
        raw_records = get_scheme_records(session=session, config=config)
        if not raw_records:
            log.warning("No Jan Soochna schemes found; creating empty dataset.")
            save_json([], output_path)
            return []

        normalized = [_normalize_record(raw, idx) for idx, raw in enumerate(raw_records)]
        seen_keys: Set[Tuple[str, str]] = set()
        deduped: List[Dict[str, object]] = []
        for row in normalized:
            key = (
                _clean_text(str(row.get("name") or "")).lower(),
                _clean_text(str(row.get("url") or "")).lower(),
            )
            if key in seen_keys:
                continue
            seen_keys.add(key)
            deduped.append(row)

        results: List[Dict[str, object]] = []
        for row in tqdm(deduped, desc="Scraping Jan Soochna schemes"):
            enriched = scrape_scheme_page(row, session=session, config=config)
            enriched["progress_updated_at"] = run_ts if enriched.get("progress_pct") is not None else None
            enriched["scraped_at"] = run_ts
            results.append(enriched)
            time.sleep(config.polite_delay)

        save_json(results, output_path)
        return results
    finally:
        session.close()


def main() -> None:
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
    )
    run_scraper()


if __name__ == "__main__":
    main()
