"""
Production-ready RajRAS scheme scraper.

Pipeline:
1. Fetch all scheme links from RajRAS index page
2. Visit each scheme page with retry + timeout handling
3. Extract structured fields
4. Save dataset to backend/data/rajras_schemes.json
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
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup, Tag
from requests.adapters import HTTPAdapter
from tqdm import tqdm
from urllib3.util.retry import Retry


INDEX_URL = "https://rajras.in/ras/pre/rajasthan/adm/schemes/"
BASE_URL = "https://rajras.in"
OUTPUT_PATH = Path(__file__).resolve().parents[1] / "data" / "rajras_schemes.json"
REQUEST_TIMEOUT = 20
REQUEST_DELAY_SECONDS = 0.8

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/123.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-IN,en;q=0.9",
    "Connection": "keep-alive",
}

SKIP_PATH_PARTS = {
    "/ras/pre/rajasthan/adm/schemes/",
    "/category/",
    "/tag/",
    "/author/",
    "/feed/",
}

SECTION_KEYWORDS = {
    "benefits": [
        "benefit",
        "benefits",
        "लाभ",
        "advantage",
        "assistance",
        "subsidy",
        "incentive",
    ],
    "eligibility": [
        "eligibility",
        "eligible",
        "criteria",
        "who can apply",
        "qualification",
        "पात्रता",
        "beneficiary",
    ],
    "documents_required": [
        "document",
        "documents",
        "required documents",
        "necessary documents",
        "दस्तावेज",
        "proof",
        "certificate",
    ],
}

CATEGORY_MAP = [
    (r"health|medical|swasth|ayush", "Health"),
    (r"education|student|scholarship|school", "Education"),
    (r"agri|agriculture|kisan|farm|crop", "Agriculture"),
    (r"social|welfare|pension|widow|disabled|\bsc\b|\bst\b|\bobc\b", "Social Welfare"),
    (r"employment|labou?r|rozgar|skill", "Labour & Employment"),
    (r"housing|awas|urban", "Housing"),
    (r"water|jal|irrigation|sanitation", "Water & Irrigation"),
    (r"digital|it|e-mitra|technology", "Digital & IT"),
]

PROGRESS_KEYWORDS = [
    "implementation",
    "progress",
    "coverage",
    "covered",
    "completion",
    "completed",
    "achieved",
    "target",
    "beneficiaries covered",
    "enrolled",
    "registered",
    "saturation",
    "uptake",
]

BENEFICIARY_KEYWORDS = [
    "beneficiary",
    "beneficiaries",
    "families",
    "family",
    "households",
    "household",
    "farmers",
    "students",
    "women",
    "children",
    "workers",
    "citizens",
    "people",
    "patients",
    "labourers",
    "laborers",
    "eligible",
    "coverage",
    "covered",
]

BUDGET_KEYWORDS = [
    "budget",
    "outlay",
    "allocation",
    "cost to state",
    "fund",
    "funds",
    "corpus",
    "expenditure",
    "financial assistance",
    "assistance",
    "subsidy",
    "grant",
    "loan",
    "insurance cover",
    "cover of",
    "per month",
    "per year",
    "per annum",
    "stipend",
    "scholarship",
]

NUMBER_WORDS = {
    "one": 1,
    "two": 2,
    "three": 3,
    "four": 4,
    "five": 5,
    "six": 6,
    "seven": 7,
    "eight": 8,
    "nine": 9,
    "ten": 10,
    "eleven": 11,
    "twelve": 12,
    "thirteen": 13,
    "fourteen": 14,
    "fifteen": 15,
    "sixteen": 16,
    "seventeen": 17,
    "eighteen": 18,
    "nineteen": 19,
    "twenty": 20,
    "twenty one": 21,
    "twenty-two": 22,
    "twenty three": 23,
    "twenty-four": 24,
    "twenty five": 25,
    "twenty-six": 26,
    "twenty seven": 27,
    "twenty-eight": 28,
    "twenty nine": 29,
    "thirty": 30,
    "thirty one": 31,
    "thirty-two": 32,
    "thirty three": 33,
}


log = logging.getLogger("scraper.rajras_full")


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
    return re.sub(r"\s+", " ", text).strip()


def _is_probable_scheme_url(url: str) -> bool:
    parsed = urlparse(url)
    if parsed.netloc and "rajras.in" not in parsed.netloc:
        return False
    path = parsed.path.lower()
    if not path or path == "/":
        return False
    if any(part in path for part in SKIP_PATH_PARTS):
        return False
    if "/wp-" in path:
        return False
    return True


def _extract_main_content(soup: BeautifulSoup) -> Tag:
    content = (
        soup.select_one(".entry-content")
        or soup.select_one("article")
        or soup.select_one("main")
        or soup.body
    )
    return content if isinstance(content, Tag) else soup


def get_scheme_links(
    index_url: str = INDEX_URL,
    session: Optional[requests.Session] = None,
    config: Optional[ScraperConfig] = None,
) -> List[str]:
    config = config or ScraperConfig()
    own_session = session is None
    session = session or _get_session()

    try:
        response = session.get(index_url, timeout=config.timeout)
        response.raise_for_status()
    except requests.RequestException as exc:
        log.exception("Failed to fetch RajRAS index page: %s", exc)
        return []
    finally:
        if own_session:
            session.close()

    soup = BeautifulSoup(response.text, "html.parser")
    content = _extract_main_content(soup)

    links: Set[str] = set()
    for anchor in content.find_all("a", href=True):
        href = anchor.get("href", "").strip()
        if not href:
            continue
        absolute_url = urljoin(BASE_URL, href)
        absolute_url = absolute_url.split("#", 1)[0].rstrip("/")
        if _is_probable_scheme_url(absolute_url):
            links.add(absolute_url)

    cleaned_links = sorted(links)
    log.info("Collected %s unique RajRAS scheme links", len(cleaned_links))
    return cleaned_links


def _iter_section_chunks(content: Tag) -> List[Tuple[str, List[str]]]:
    chunks: List[Tuple[str, List[str]]] = []
    current_heading = "General"
    current_lines: List[str] = []

    for node in content.descendants:
        if not isinstance(node, Tag):
            continue

        if node.name in {"h1", "h2", "h3", "h4"}:
            if current_lines:
                chunks.append((current_heading, current_lines))
                current_lines = []
            current_heading = _clean_text(node.get_text(" ", strip=True)) or "General"
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


def _extract_progress_signal(chunks: List[Tuple[str, List[str]]]) -> Tuple[Optional[float], Optional[str]]:
    best_score = -1
    best_pct: Optional[float] = None
    best_source: Optional[str] = None

    for heading, lines in chunks:
        heading_lower = heading.lower()
        for line in lines:
            line_lower = line.lower()
            context = f"{heading_lower} {line_lower}"

            # Prefer percentage statements in progress/coverage context.
            pct_matches = re.findall(r"(\d{1,3}(?:\.\d+)?)\s*%", line)
            if pct_matches:
                for raw in pct_matches:
                    pct = float(raw)
                    if pct < 0 or pct > 100:
                        continue
                    kw_hits = sum(1 for kw in PROGRESS_KEYWORDS if kw in context)
                    if kw_hits == 0:
                        continue
                    score = 2 + kw_hits
                    if score > best_score:
                        best_score = score
                        best_pct = round(pct, 2)
                        best_source = _clean_text(line)[:240]

            # Fallback: derive percent from "x out of y" style counts.
            ratio = re.search(r"(\d{1,6})\s*(?:out of|of)\s*(\d{1,6})", line_lower)
            if ratio:
                num = float(ratio.group(1))
                den = float(ratio.group(2))
                if den > 0:
                    pct = (num / den) * 100.0
                    if 0 <= pct <= 100:
                        kw_hits = sum(1 for kw in PROGRESS_KEYWORDS if kw in context)
                        if kw_hits == 0:
                            continue
                        score = 1 + kw_hits
                        if score > best_score:
                            best_score = score
                            best_pct = round(pct, 2)
                            best_source = _clean_text(line)[:240]

    return best_pct, best_source


def _normalize_count_phrase(raw: str) -> str:
    text = _clean_text(raw)
    if not text:
        return text
    text = re.sub(r"\bLakhs\b", "lakh", text, flags=re.IGNORECASE)
    text = re.sub(r"\bCrores\b", "crore", text, flags=re.IGNORECASE)
    return text


def _iter_sentences(text: str) -> List[str]:
    return [segment.strip() for segment in re.split(r"(?<=[.!?])\s+|\s*•\s*", text) if segment.strip()]


def _extract_launch_year(text: str) -> Optional[int]:
    if not text:
        return None

    patterns = [
        r"(?:launched|started|introduced|implemented|announced|rolled out|came into force)[^.]{0,120}?\b(19\d{2}|20\d{2})\b",
        r"\bon\s+\d{1,2}(?:st|nd|rd|th)?\s+[A-Za-z]+\s*,?\s*(19\d{2}|20\d{2})\b",
        r"\bin\s+(19\d{2}|20\d{2})\b",
        r"\bduring\s+the\s+year\s+(19\d{2}|20\d{2})\b",
    ]
    for pattern in patterns:
        match = re.search(pattern, text, re.IGNORECASE)
        if match:
            year = int(match.group(1))
            if 1900 <= year <= 2100:
                return year
    return None


def _extract_budget(text: str) -> Optional[str]:
    if not text:
        return None

    amount_pattern = (
        r"((?:₹|Rs\.?|INR)\s*[\d,]+(?:\.\d+)?\s*(?:lakh\s*crore|crore|cr|lakh)?"
        r"(?:\s*(?:per month|per year|per annum|monthly|annually|/month|/year|/yr|/mo))?)"
    )
    best_match: Optional[str] = None
    best_score = -1
    for sentence in _iter_sentences(text):
        lowered = sentence.lower()
        match = re.search(amount_pattern, sentence, re.IGNORECASE)
        if not match:
            continue
        kw_hits = sum(1 for keyword in BUDGET_KEYWORDS if keyword in lowered)
        if kw_hits == 0 and not re.search(r"(?:₹|rs\.?|inr)", sentence, re.IGNORECASE):
            continue
        score = kw_hits
        if re.search(r"(?:subsidy|grant|assistance|loan|insurance|stipend|scholarship)", lowered):
            score += 2
        if score > best_score:
            best_score = score
            best_match = _clean_text(match.group(1)).rstrip(" .,:;")
    return best_match


def _extract_beneficiaries(text: str) -> Optional[str]:
    if not text:
        return None

    count_patterns = [
        r"((?:around\s+|about\s+|over\s+|more than\s+|nearly\s+|upto\s+|up to\s+)?\d+(?:\.\d+)?\s*(?:crore|lakh|lakhs)\s+(?:families|family|households|people|citizens|students|women|children|farmers|workers|patients|beneficiaries))",
        r"((?:around\s+|about\s+|over\s+|more than\s+|nearly\s+|upto\s+|up to\s+)?\d[\d,]*(?:\.\d+)?\s+(?:families|family|households|people|citizens|students|women|children|farmers|workers|patients|beneficiaries))",
        r"((?:all\s+)?\d+(?:\.\d+)?\s*(?:crore|lakh|lakhs)?\s*(?:ration shops|shops|villages|hospitals|schools|districts))",
    ]
    audience_pattern = r"((?:laborers|labourers|rickshaw pullers|auto drivers|students|working women|elders|women|girls|children|farmers|workers)[^.]{0,120}?(?:beneficiary|beneficiaries|main beneficiary))"

    best_match: Optional[str] = None
    best_score = -1
    for sentence in _iter_sentences(text):
        lowered = sentence.lower()
        kw_hits = sum(1 for keyword in BENEFICIARY_KEYWORDS if keyword in lowered)
        for pattern in count_patterns:
            match = re.search(pattern, sentence, re.IGNORECASE)
            if match:
                score = kw_hits
                if re.search(r"(?:covered|registered|enrolled|eligible|distributed)", lowered):
                    score += 2
                candidate = _normalize_count_phrase(match.group(1)).rstrip(" .,:;")
                if score > best_score:
                    best_score = score
                    best_match = candidate
        if "beneficiar" in lowered:
            match = re.search(audience_pattern, sentence, re.IGNORECASE)
            if match:
                candidate = _clean_text(match.group(1)).rstrip(" .,:;")
                if kw_hits > best_score:
                    best_score = kw_hits
                    best_match = candidate

    return best_match


def _extract_districts(text: str) -> Optional[str]:
    if not text:
        return None

    match = re.search(r"\ball\s+33\s+districts\b", text, re.IGNORECASE)
    if match:
        return "All 33"

    candidates: List[Tuple[int, int]] = []
    for sentence in _iter_sentences(text):
        lowered = sentence.lower()
        score = 0
        if any(token in lowered for token in ("rajasthan", "state", "implemented", "cover", "covered", "serves")):
            score += 2
        if "district" not in lowered:
            continue

        for match in re.finditer(r"\b(\d{1,2})\s+districts\b", sentence, re.IGNORECASE):
            candidates.append((int(match.group(1)), score))
        for phrase, value in NUMBER_WORDS.items():
            if re.search(rf"\b{re.escape(phrase)}\s+districts\b", lowered, re.IGNORECASE):
                candidates.append((value, score))

    if not candidates:
        return None

    best_score = max(score for _, score in candidates)
    best_values = sorted({value for value, score in candidates if score == best_score})
    if len(best_values) == 1:
        return f"{best_values[0]} districts"

    return None


def extract_sections(soup: BeautifulSoup) -> Dict[str, Optional[object]]:
    content = _extract_main_content(soup)
    chunks = _iter_section_chunks(content)

    headings: List[str] = []
    description_parts: List[str] = []
    benefits: List[str] = []
    eligibility: List[str] = []
    documents: List[str] = []

    for heading, lines in chunks:
        if heading and heading != "General":
            headings.append(heading)
        heading_lower = heading.lower()

        if not description_parts and lines:
            description_parts.extend(lines[:2])

        if any(key in heading_lower for key in SECTION_KEYWORDS["benefits"]):
            benefits.extend(lines)
        if any(key in heading_lower for key in SECTION_KEYWORDS["eligibility"]):
            eligibility.extend(lines)
        if any(key in heading_lower for key in SECTION_KEYWORDS["documents_required"]):
            documents.extend(lines)

    # Fallback keyword extraction from all lines if heading-based detection misses data.
    if not benefits or not eligibility or not documents:
        all_lines = [line for _, lines in chunks for line in lines]
        for line in all_lines:
            ll = line.lower()
            if not benefits and any(k in ll for k in SECTION_KEYWORDS["benefits"]):
                benefits.append(line)
            if not eligibility and any(k in ll for k in SECTION_KEYWORDS["eligibility"]):
                eligibility.append(line)
            if not documents and any(k in ll for k in SECTION_KEYWORDS["documents_required"]):
                documents.append(line)

    # De-duplicate while preserving order.
    def uniq(items: List[str]) -> List[str]:
        seen: Set[str] = set()
        out: List[str] = []
        for item in items:
            cleaned = _clean_text(item)
            if cleaned and cleaned not in seen:
                seen.add(cleaned)
                out.append(cleaned)
        return out

    headings = uniq(headings)
    benefits = uniq(benefits)
    eligibility = uniq(eligibility)
    documents = uniq(documents)
    description = _clean_text(" ".join(description_parts[:3])) or None
    progress_pct, progress_source = _extract_progress_signal(chunks)
    full_text = _clean_text(" ".join(line for _, lines in chunks for line in lines))

    return {
        "description": description,
        "headings": headings or None,
        "benefits": benefits or None,
        "eligibility": eligibility or None,
        "documents_required": documents or None,
        "beneficiaries": _extract_beneficiaries(full_text),
        "launch_year": _extract_launch_year(full_text),
        "budget": _extract_budget(full_text),
        "districts": _extract_districts(full_text),
        "progress_pct": progress_pct,
        "progress_source": progress_source,
    }


def _detect_category(name: str, headings: Optional[List[str]], description: Optional[str]) -> Optional[str]:
    text = f"{name} {' '.join(headings or [])} {description or ''}".lower()
    for pattern, label in CATEGORY_MAP:
        if re.search(pattern, text, re.IGNORECASE):
            return label
    return None


def scrape_scheme_page(
    url: str,
    session: Optional[requests.Session] = None,
    config: Optional[ScraperConfig] = None,
) -> Dict[str, Optional[object]]:
    config = config or ScraperConfig()
    own_session = session is None
    session = session or _get_session()

    try:
        response = session.get(url, timeout=config.timeout)
        response.raise_for_status()
    except requests.RequestException as exc:
        log.warning("Failed to fetch scheme page %s: %s", url, exc)
        return {
            "scheme_name": None,
            "description": None,
            "category": None,
            "headings": None,
            "benefits": None,
            "eligibility": None,
            "documents_required": None,
            "source": "RajRAS",
            "source_url": url,
        }
    finally:
        if own_session:
            session.close()

    soup = BeautifulSoup(response.text, "html.parser")
    title_node = soup.find("h1") or soup.find("title")
    scheme_name = _clean_text(title_node.get_text(" ", strip=True) if title_node else "")
    if scheme_name:
        scheme_name = re.sub(r"\s*[-|]\s*RajRAS.*$", "", scheme_name, flags=re.IGNORECASE).strip()

    sections = extract_sections(soup)
    category = _detect_category(
        scheme_name or "",
        sections.get("headings") if isinstance(sections.get("headings"), list) else None,
        sections.get("description") if isinstance(sections.get("description"), str) else None,
    )

    return {
        "scheme_name": scheme_name or None,
        "description": sections["description"],
        "category": category,
        "headings": sections["headings"],
        "benefits": sections["benefits"],
        "eligibility": sections["eligibility"],
        "documents_required": sections["documents_required"],
        "beneficiaries": sections["beneficiaries"],
        "launch_year": sections["launch_year"],
        "budget": sections["budget"],
        "districts": sections["districts"],
        "progress_pct": sections["progress_pct"],
        "progress_source": sections["progress_source"],
        "source": "RajRAS",
        "source_url": url,
    }


def save_json(data: List[Dict[str, object]], output_path: Path = OUTPUT_PATH) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)
    log.info("Saved %s RajRAS schemes to %s", len(data), output_path)


def run_scraper(
    index_url: str = INDEX_URL,
    output_path: Path = OUTPUT_PATH,
    config: Optional[ScraperConfig] = None,
) -> List[Dict[str, object]]:
    config = config or ScraperConfig()
    session = _get_session()
    run_ts = datetime.now(timezone.utc).isoformat()
    collected: List[Dict[str, object]] = []
    try:
        scheme_links = get_scheme_links(index_url=index_url, session=session, config=config)
        if not scheme_links:
            log.warning("No RajRAS scheme links found; creating empty dataset.")
            save_json([], output_path=output_path)
            return []

        for idx, url in enumerate(tqdm(scheme_links, desc="Scraping RajRAS schemes"), start=1):
            item = scrape_scheme_page(url, session=session, config=config)
            row = {
                "id": f"rajras_{idx:03d}",
                "name": item["scheme_name"],
                "category": item["category"],
                "description": item["description"],
                "headings": item["headings"],
                "benefits": item["benefits"],
                "eligibility": item["eligibility"],
                "documents_required": item["documents_required"],
                "beneficiaries": item["beneficiaries"],
                "launch_year": item["launch_year"],
                "budget": item["budget"],
                "districts": item["districts"],
                "progress_pct": item["progress_pct"],
                "progress": (
                    f"{item['progress_pct']}%"
                    if isinstance(item.get("progress_pct"), (int, float))
                    else None
                ),
                "progress_source": item["progress_source"],
                "progress_updated_at": run_ts if item["progress_pct"] is not None else None,
                "source": item["source"],
                "url": item["source_url"],
            }
            collected.append({k: v for k, v in row.items() if v is not None})
            time.sleep(config.polite_delay)

        # Deduplicate by URL; keep first seen.
        unique: List[Dict[str, object]] = []
        seen_urls: Set[str] = set()
        for row in collected:
            row_url = str(row.get("url") or "")
            if row_url in seen_urls:
                continue
            seen_urls.add(row_url)
            unique.append(row)

        save_json(unique, output_path=output_path)
        return unique
    finally:
        session.close()


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
    )
    run_scraper()


if __name__ == "__main__":
    main()
