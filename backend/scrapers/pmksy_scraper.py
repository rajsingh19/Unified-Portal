"""
pmksy_scraper.py
================
Builds the PMKSY district dashboard from the official Rajasthan
Agriculture Statistics 2022-23 publication.

Coverage formula:
    net irrigated area / net area sown * 100

Official source:
    Directorate of Economics & Statistics, Government of Rajasthan
    https://rajas.rajasthan.gov.in/PDF/11222024122534PMAgriculturalStatistics.pdf
"""

from __future__ import annotations

import logging
import re
import subprocess
import tempfile
from datetime import datetime, timezone
from pathlib import Path

import requests

log = logging.getLogger("scraper.pmksy")

PDF_URL = "https://rajas.rajasthan.gov.in/PDF/11222024122534PMAgriculturalStatistics.pdf"
SOURCE_LABEL = (
    "Agricultural Statistics of Rajasthan 2022-23, Directorate of Economics & Statistics, Govt. of Rajasthan"
)
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/122.0.0.0 Safari/537.36",
    "Accept": "application/pdf,text/html,*/*;q=0.9",
    "Accept-Language": "en-IN,en;q=0.9",
}


def _clean_text(value: str) -> str:
    return re.sub(r"\s+", " ", str(value or "")).strip()


def _extract_pdf_text(pdf_path: Path) -> str:
    try:
        result = subprocess.run(
            ["pdftotext", "-layout", str(pdf_path), "-"],
            check=True,
            capture_output=True,
            text=True,
        )
        return result.stdout
    except FileNotFoundError as exc:
        raise RuntimeError("pdftotext is required to parse the official PMKSY source PDF.") from exc


def _slice_table(text: str, start_marker: str, end_marker: str) -> list[str]:
    start = text.find(start_marker)
    end = text.find(end_marker, start if start >= 0 else 0)
    if start < 0 or end < 0 or end <= start:
        return []
    return [
        line.rstrip()
        for line in text[start:end].splitlines()
        if line.strip()
    ]


def _parse_numeric_triplets(lines: list[str]) -> dict[str, dict[str, int]]:
    pattern = re.compile(r"^\s*(\d+)\s+([A-Z.\-]+)\s+(\d+)\s+(\d+)\s+(\d+)\s*$")
    rows = {}
    for line in lines:
        match = pattern.match(line)
        if not match:
            continue
        _, raw_name, total_area, double_cropped, net_area_sown = match.groups()
        rows[raw_name] = {
            "total_area_under_all_crops": int(total_area),
            "double_cropped_area": int(double_cropped),
            "net_area_sown_ha": int(net_area_sown),
        }
    return rows


def _parse_irrigated_totals(lines: list[str]) -> dict[str, dict[str, int]]:
    pattern = re.compile(
        r"^\s*(\d+)\s+([A-Z.\-]+)\s+"
        r"(\d+)\s+(\d+)\s+(\d+)\s+(\d+)\s+(\d+)\s+(\d+)\s+(\d+)\s+(\d+)\s+(\d+)\s*$"
    )
    rows = {}
    for line in lines:
        match = pattern.match(line)
        if not match:
            continue
        (
            _,
            raw_name,
            tube_wells_electric,
            tube_wells_oil,
            tube_wells_total,
            open_wells_electric,
            open_wells_oil,
            open_wells_other,
            open_wells_total,
            other_sources,
            total_net_irrigated,
        ) = match.groups()
        rows[raw_name] = {
            "tube_wells_total_ha": int(tube_wells_total),
            "open_wells_total_ha": int(open_wells_total),
            "other_sources_ha": int(other_sources),
            "net_irrigated_area_ha": int(total_net_irrigated),
        }
    return rows


def _normalize_name(raw_name: str) -> str:
    mapping = {
        "BHRATPUR": "Bharatpur",
        "GANGANAGAR": "Sri Ganganagar",
        "S.MADHOPUR": "Sawai Madhopur",
    }
    if raw_name in mapping:
        return mapping[raw_name]
    return raw_name.replace(".", " ").title()


def _format_lakh_hectare(area_ha: int) -> str:
    return f"{area_ha / 100000:.2f} L ha"


def scrape_pmksy():
    scraped_at = datetime.now(timezone.utc).isoformat()
    session = requests.Session()
    session.headers.update(HEADERS)

    with tempfile.NamedTemporaryFile(suffix=".pdf", delete=False) as tmp_file:
        pdf_path = Path(tmp_file.name)

    try:
        response = session.get(PDF_URL, timeout=30, verify=False)
        response.raise_for_status()
        pdf_path.write_bytes(response.content)
        full_text = _extract_pdf_text(pdf_path)
    except Exception as exc:
        log.error("PMKSY agriculture-stats scrape failed: %s", exc)
        return []
    finally:
        session.close()
        try:
            pdf_path.unlink(missing_ok=True)
        except Exception:
            pass

    net_irrigated_lines = _slice_table(
        full_text,
        "SOURCE WISE NET IRRIGATED AREA (2022-23)",
        "RAJ. STATE    4493009   331168 4824177",
    )
    net_area_lines = _slice_table(
        full_text,
        "1        138                   139                     140",
        "RAJ. STATE            28171106              9748549                  18422557",
    )

    net_irrigated = _parse_irrigated_totals(net_irrigated_lines)
    net_area_sown = _parse_numeric_triplets(net_area_lines)

    rows = []
    for raw_name, area_row in net_area_sown.items():
        irrigated_row = net_irrigated.get(raw_name)
        if not irrigated_row:
            continue
        net_area_sown_ha = area_row["net_area_sown_ha"]
        net_irrigated_area_ha = irrigated_row["net_irrigated_area_ha"]
        if net_area_sown_ha <= 0:
            continue
        coverage_pct = round((net_irrigated_area_ha / net_area_sown_ha) * 100, 1)
        rows.append({
            "name": _normalize_name(raw_name),
            "net_area_sown_ha": net_area_sown_ha,
            "net_area_sown_lakh_ha": round(net_area_sown_ha / 100000, 2),
            "net_area_sown_display": _format_lakh_hectare(net_area_sown_ha),
            "net_irrigated_area_ha": net_irrigated_area_ha,
            "net_irrigated_area_lakh_ha": round(net_irrigated_area_ha / 100000, 2),
            "net_irrigated_area_display": _format_lakh_hectare(net_irrigated_area_ha),
            "coverage_pct": coverage_pct,
            "source": "rajas.rajasthan.gov.in",
            "source_title": SOURCE_LABEL,
            "source_url": PDF_URL,
            "report_label": "2022-23 Annual Report",
            "scraped_at": scraped_at,
            "live": True,
        })

    if not rows:
        log.warning("PMKSY agriculture-stats parser returned 0 districts")
        return []

    state_average = round(
        sum(row["coverage_pct"] for row in rows) / len(rows),
        1,
    )

    for row in rows:
        pct = row["coverage_pct"]
        if pct >= 65:
            tone = "good"
            status = "On track"
        elif pct >= 40:
            tone = "watch"
            status = "Needs push"
        else:
            tone = "critical"
            status = "Critical"
        row["state_average"] = state_average
        row["status"] = status
        row["status_tone"] = tone

    log.info("PMKSY agriculture stats: %d districts parsed from official Rajasthan PDF", len(rows))
    return rows
