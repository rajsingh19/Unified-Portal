"""
budget_scraper.py
=================
Scrapes real Rajasthan budget & financial data from:
  1. PRS India (prsindia.org) — budget analysis, state finances
  2. finance.rajasthan.gov.in — official budget documents
  3. JJM MIS (ejalshakti.gov.in) — Jal Jeevan Mission coverage
  4. RajRAS / official press releases — budget highlights

Returns a structured dict with:
  - budget_summary: headline figures (revenue, deficit, capital outlay etc.)
  - sector_allocations: per-sector budget amounts
  - jjm_coverage: district-level tap water data
  - fiscal_indicators: GSDP, deficit %, growth rate
  - scraped_at: ISO timestamp
"""

import re, logging, requests
from datetime import datetime, timezone
from bs4 import BeautifulSoup

log = logging.getLogger("scraper.budget")

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/122.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-IN,en;q=0.9",
}

SESS = requests.Session()
SESS.headers.update(HEADERS)


def _get(url, timeout=18):
    try:
        r = SESS.get(url, timeout=timeout, verify=False)
        r.raise_for_status()
        return r.text
    except Exception as e:
        log.warning("GET %s failed: %s", url, e)
        return ""


def _parse_crore(text):
    """Extract ₹ crore value from text like '₹28,865 Cr' or '28865 crore'."""
    if not text:
        return None
    t = text.replace(",", "")
    m = re.search(r"[\₹Rs\.]*\s*([\d]+(?:\.\d+)?)\s*(?:crore|cr\.?|lakh\s*crore)", t, re.I)
    if m:
        val = float(m.group(1))
        if "lakh" in t.lower():
            val = val * 100000
        return val
    return None


def _scrape_prs():
    """
    Scrape PRS India state budget page for Rajasthan 2025-26.
    URL: https://prsindia.org/budgets/states/rajasthan-budget-analysis-2025-26
    """
    url = "https://prsindia.org/budgets/states/rajasthan-budget-analysis-2025-26"
    html = _get(url)
    if not html:
        url2 = "https://prsindia.org/budgets/states"
        html = _get(url2)

    if not html:
        return {}

    soup = BeautifulSoup(html, "html.parser")
    text = soup.get_text(" ", strip=True)

    data = {}

    # Try to extract key figures from page text
    # Revenue expenditure
    m = re.search(r"revenue expenditure[^\d₹]*(?:of\s*)?(?:₹|Rs\.?)\s*([\d,]+(?:\.\d+)?)\s*(?:crore|cr)", text, re.I)
    if m:
        data["revenue_expenditure_cr"] = float(m.group(1).replace(",",""))

    # Capital outlay
    m = re.search(r"capital outlay[^\d₹]*(?:of\s*)?(?:₹|Rs\.?)\s*([\d,]+(?:\.\d+)?)\s*(?:crore|cr)", text, re.I)
    if m:
        data["capital_outlay_cr"] = float(m.group(1).replace(",",""))

    # Fiscal deficit
    m = re.search(r"fiscal deficit[^\d%]*?([\d.]+)\s*%\s*(?:of\s*)?GSDP", text, re.I)
    if m:
        data["fiscal_deficit_pct_gsdp"] = float(m.group(1))

    # Total expenditure
    m = re.search(r"total expenditure[^\d₹]*(?:₹|Rs\.?)\s*([\d,]+(?:\.\d+)?)\s*(?:crore|cr)", text, re.I)
    if m:
        data["total_expenditure_cr"] = float(m.group(1).replace(",",""))

    # Health
    m = re.search(r"health[^\d₹\n]*(?:₹|Rs\.?)\s*([\d,]+(?:\.\d+)?)\s*(?:crore|cr)", text, re.I)
    if m:
        data["health_cr"] = float(m.group(1).replace(",",""))

    # Education
    m = re.search(r"education[^\d₹\n]*(?:₹|Rs\.?)\s*([\d,]+(?:\.\d+)?)\s*(?:crore|cr)", text, re.I)
    if m:
        data["education_cr"] = float(m.group(1).replace(",",""))

    log.info("PRS scraped: %d fields", len(data))
    return data


def _scrape_finance_raj():
    """
    Scrape finance.rajasthan.gov.in for budget highlights.
    """
    urls = [
        "https://finance.rajasthan.gov.in/docs/budget/statebudgets/2025-2026/BudgetAtGlance2025-26.pdf",
        "https://finance.rajasthan.gov.in/budget.aspx",
        "https://finance.rajasthan.gov.in",
    ]
    for url in urls:
        html = _get(url)
        if html and len(html) > 500:
            soup = BeautifulSoup(html, "html.parser")
            text = soup.get_text(" ", strip=True)
            data = {}
            # Total budget size
            m = re.search(r"(?:total\s*)?budget\s*(?:size|estimate)[^\d₹]*(?:₹|Rs\.?)\s*([\d,]+(?:\.\d+)?)\s*(?:crore|cr|lakh\s*crore)", text, re.I)
            if m:
                val = float(m.group(1).replace(",",""))
                if "lakh" in m.group(0).lower():
                    val = val * 100000
                data["total_budget_cr"] = val
            if data:
                log.info("Finance.raj scraped: %d fields", len(data))
                return data
    return {}


def _scrape_jjm():
    """
    Scrape JJM MIS for Rajasthan overall coverage percentage.
    URL: https://ejalshakti.gov.in/jjmreport/JJMIndia.aspx
    Also tries the API endpoint.
    """
    urls = [
        "https://ejalshakti.gov.in/jjmreport/JJMIndia.aspx",
        "https://jjm.gov.in/",
        "https://ejalshakti.gov.in/jjmreport/",
    ]
    for url in urls:
        html = _get(url)
        if not html:
            continue
        soup = BeautifulSoup(html, "html.parser")
        text = soup.get_text(" ", strip=True)

        # Look for Rajasthan row
        raj_match = re.search(
            r"Rajasthan[^\n%]*?([\d.]+)\s*%",
            text, re.I
        )
        if raj_match:
            pct = float(raj_match.group(1))
            log.info("JJM scraped Rajasthan coverage: %.2f%%", pct)
            return {"rajasthan_coverage_pct": pct, "source_url": url}

        # Try finding coverage number near Rajasthan
        lines = text.split("\n")
        for i, line in enumerate(lines):
            if "rajasthan" in line.lower():
                nearby = " ".join(lines[max(0,i-2):i+5])
                m = re.search(r"([\d.]+)\s*%", nearby)
                if m:
                    pct = float(m.group(1))
                    if 10 < pct < 100:
                        log.info("JJM coverage from context: %.2f%%", pct)
                        return {"rajasthan_coverage_pct": pct, "source_url": url}

    log.warning("JJM scrape failed, using last known")
    return {}


def _scrape_rajras_budget():
    """
    Scrape RajRAS for budget-related articles.
    """
    urls = [
        "https://rajras.in/rajasthan-budget-2025-26/",
        "https://rajras.in/rajasthan-budget/",
        "https://rajras.in/?s=budget+2025",
    ]
    for url in urls:
        html = _get(url)
        if not html:
            continue
        soup = BeautifulSoup(html, "html.parser")
        text = soup.get_text(" ", strip=True)
        data = {}

        # Total revenue expenditure
        m = re.search(r"revenue\s+expenditure[^\d₹\n]*(?:₹|Rs\.?)\s*([\d,]+(?:\.\d+)?)\s*(?:crore|cr)", text, re.I)
        if m:
            data["revenue_expenditure_cr"] = float(m.group(1).replace(",",""))

        # GSDP
        m = re.search(r"GSDP[^\d₹\n]*(?:₹|Rs\.?)\s*([\d,]+(?:\.\d+)?)\s*(?:lakh\s*crore|crore|cr)", text, re.I)
        if m:
            val = float(m.group(1).replace(",",""))
            if "lakh" in m.group(0).lower():
                val = val * 100000
            data["gsdp_cr"] = val

        # Social security / pension
        m = re.search(r"(?:social\s*security|pension)[^\d₹\n]*(?:₹|Rs\.?)\s*([\d,]+(?:\.\d+)?)\s*(?:crore|cr)", text, re.I)
        if m:
            data["social_security_cr"] = float(m.group(1).replace(",",""))

        if data:
            log.info("RajRAS budget scraped: %s", data)
            return data
    return {}


# ── Fallback: verified public data (cited from Budget 2025-26 documents) ──────
# These are the REAL figures from Rajasthan Budget 2025-26 as tabled in the
# State Legislature and published by PRS India. Used only when scraping fails.
BUDGET_FALLBACK = {
    "year":                     "2025-26",
    "total_expenditure_cr":     325546,   # Revenue expenditure ₹3,25,546 Cr (Budget 2025-26)
    "capital_outlay_cr":        53686,    # Capital outlay +40% over 2024-25 RE
    "fiscal_deficit_cr":        34543,    # Fiscal deficit ₹34,543 Cr (2025-26 BE)
    "fiscal_deficit_pct_gsdp":  4.25,     # 4.25% of GSDP
    "gsdp_cr":                  1350000,  # Estimated GSDP ~₹13.5 lakh crore
    "health_cr":                28865,    # Health budget ₹28,865 Cr (8.4% of total)
    "education_cr":             None,     # 18% share derived
    "education_pct":            18.0,     # 18% of total budget, above 15% national avg
    "social_security_cr":       14000,    # Social Security ₹14,000+ Cr
    "agriculture_cr":           None,
    "jjm_coverage_pct":         55.36,    # JJM MIS as of early 2025
    "economy_target_bn_usd":    350,      # $350 Bn economy by 2030
    "green_budget":             True,     # First Green Budget of Rajasthan
    "source":                   "Budget 2025-26 (Rajasthan Legislature) · PRS India · JJM MIS",
    "source_url":               "https://prsindia.org/budgets/states/rajasthan-budget-analysis-2025-26",
}

# ── Sparklines and JJM districts are now LIVE-scraped ─────────────────────────
# SPARKLINE_DATA and JJM district arrays were previously hardcoded here.
# They are now fetched by sparkline_scraper.py and jjm_scraper.py respectively.
# This file keeps only the budget figure fallbacks.


def scrape_budget():
    """
    Main entry point. Scrapes budget figures, then calls sparkline_scraper
    and jjm_scraper for live trend data and district coverage.
    Falls back to verified data for any field that fails to scrape.
    """
    ts = datetime.now(timezone.utc).isoformat()
    log.info("Starting budget scrape (figures + sparklines + JJM districts)...")

    # ── 1. Scrape budget figures ──────────────────────────────────────────────
    prs_data    = _scrape_prs()
    fin_data    = _scrape_finance_raj()
    rajras_data = _scrape_rajras_budget()

    # ── 2. Scrape JJM districts (live from ejalshakti.gov.in) ────────────────
    from scrapers.jjm_scraper import scrape_jjm
    jjm_districts = scrape_jjm()
    # Extract state-level coverage from districts
    live_districts = [d for d in jjm_districts if d.get("coverage") is not None]
    jjm_state_pct  = (sum(d["coverage"] for d in live_districts) / len(live_districts)
                      if live_districts else None)

    # ── 3. Scrape sparklines (live from PRS India year-by-year) ──────────────
    from scrapers.sparkline_scraper import scrape_sparklines
    sparkline_result = scrape_sparklines(current_jjm_pct=jjm_state_pct)

    # ── 4. Merge budget figures ───────────────────────────────────────────────
    merged = dict(BUDGET_FALLBACK)
    merged["scraped_at"] = ts

    scraped_fields = 0
    for src in [prs_data, fin_data, rajras_data]:
        for k, v in src.items():
            if v is not None and k in merged:
                merged[k] = v
                scraped_fields += 1

    # Use live JJM state average if available
    if jjm_state_pct is not None:
        merged["jjm_coverage_pct"] = round(jjm_state_pct, 2)
        scraped_fields += 1

    # ── 5. Attach live sparklines ─────────────────────────────────────────────
    merged["sparklines"]     = sparkline_result["sparklines"]
    merged["sparkline_meta"] = {
        "live_years":   sparkline_result["live_years"],
        "total_years":  sparkline_result["total_years"],
        "years":        sparkline_result["years"],
        "note":         sparkline_result["note"],
        "source":       sparkline_result["source"],
    }

    # ── 6. Attach live JJM districts ─────────────────────────────────────────
    merged["jjm_districts"]   = jjm_districts
    merged["jjm_districts_live"] = any(d.get("live") for d in jjm_districts)

    # ── 7. Compute derived fields ─────────────────────────────────────────────
    if merged.get("total_expenditure_cr") and not merged.get("education_cr"):
        merged["education_cr"] = round(
            merged["total_expenditure_cr"] * merged.get("education_pct", 18) / 100)

    if merged.get("total_expenditure_cr") and merged.get("health_cr"):
        merged["health_pct"] = round(
            merged["health_cr"] / merged["total_expenditure_cr"] * 100, 1)

    # ── 8. Format display values ──────────────────────────────────────────────
    def fmt_cr(v):
        if v is None: return "N/A"
        if v >= 100000: return f"₹{v/100000:.1f} L Cr"
        return f"₹{int(v):,} Cr"

    merged["display"] = {
        "total_expenditure":  fmt_cr(merged.get("total_expenditure_cr")),
        "capital_outlay":     fmt_cr(merged.get("capital_outlay_cr")),
        "fiscal_deficit":     fmt_cr(merged.get("fiscal_deficit_cr")),
        "fiscal_deficit_pct": f"{merged.get('fiscal_deficit_pct_gsdp', 4.25):.2f}% GSDP",
        "health":             fmt_cr(merged.get("health_cr")),
        "health_pct":         f"{merged.get('health_pct', 8.4):.1f}% of total",
        "education_pct":      f"{merged.get('education_pct', 18.0):.0f}% share",
        "social_security":    fmt_cr(merged.get("social_security_cr")),
        "jjm_coverage":       f"{merged.get('jjm_coverage_pct', 55.36):.2f}%",
        "gsdp":               fmt_cr(merged.get("gsdp_cr")),
    }

    live_figure_sources = sum([
        1 if prs_data    else 0,
        1 if fin_data    else 0,
        1 if rajras_data else 0,
    ])

    merged["scrape_meta"] = {
        "live_sources":     live_figure_sources,
        "scraped_fields":   scraped_fields,
        "fallback_used":    scraped_fields == 0,
        "jjm_districts_live": merged["jjm_districts_live"],
        "sparkline_live_years": sparkline_result["live_years"],
        "note": (
            f"Live: {live_figure_sources} budget sources, "
            f"{sparkline_result['live_years']}/{sparkline_result['total_years']} sparkline years, "
            f"JJM {'live' if merged['jjm_districts_live'] else 'fallback'}"
        ),
    }

    log.info(
        "Budget complete: %d figure fields, %d/%d sparkline years, JJM %s, %d districts",
        scraped_fields,
        sparkline_result["live_years"], sparkline_result["total_years"],
        "live" if merged["jjm_districts_live"] else "fallback",
        len(jjm_districts),
    )
    return merged
