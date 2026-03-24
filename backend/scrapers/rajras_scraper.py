"""
rajras_scraper.py
Scrapes rajras.in for Rajasthan government schemes.
Fetches index page + article details: objective, eligibility, benefits, apply_url.
"""
import re, logging, requests
from datetime import datetime, timezone
from urllib.parse import urljoin
from bs4 import BeautifulSoup, Tag

log = logging.getLogger("scraper.rajras")
INDEX_URL = "https://rajras.in/ras/pre/rajasthan/adm/schemes/"
BASE_URL  = "https://rajras.in"
HEADERS   = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/122.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-IN,en;q=0.9",
}

SECTOR_MAP = {
    r"agriculture|agri|kisan|crop|farm|horticulture|fisheri|dairy": "Agriculture",
    r"health|medical|ayush|nutrition|sanitation":                    "Health",
    r"education|school|scholarsh|student|literacy|skill|training":  "Education",
    r"social|pension|widow|disabled|SC|ST|OBC|welfare|women|child": "Social Welfare",
    r"labour|worker|employment|rozgar":                              "Labour & Employment",
    r"rural|panchayat|MGNREGA|village":                              "Rural Development",
    r"urban|housing|municipal":                                      "Housing",
    r"industry|MSME|enterprise|startup|business|invest":            "Industry & Commerce",
    r"energy|solar|renewable|electricity":                          "Energy",
    r"water|irrigation|dam":                                         "Water & Irrigation",
    r"digital|IT|e-gov|technology":                                  "Digital & IT",
    r"tourism|heritage|culture":                                     "Tourism & Culture",
    r"forest|environment|wildlife":                                  "Environment",
    r"transport|road|highway":                                       "Transport",
    r"revenue|land|jamabandi":                                       "Revenue & Land",
    r"mining|mineral":                                               "Mining",
}
NAV_SKIP = re.compile(r"^(home|about|contact|privacy|terms|login|register|menu|search|download|read more|click here|back|next|previous|sitemap)$", re.I)
ELIGIBILITY_KW = re.compile(r"eligib|who can apply|target group|beneficiar|criteria|qualification", re.I)
BENEFIT_KW     = re.compile(r"benefit|assistance|amount|grant|subsidy|what.*get|incentive|financial", re.I)
OBJECTIVE_KW   = re.compile(r"objective|aim|purpose|about|overview|introduc|background|salient", re.I)

def _sector(text):
    t = text.lower()
    for pat, sec in SECTOR_MAP.items():
        if re.search(pat, t, re.I): return sec
    return "General"

def _clean(text):
    text = re.sub(r"^\d+[.)]\s*","",text.strip())
    return re.sub(r"\s+"," ",text).strip()

def _is_scheme_link(href, name):
    if not href or not name: return False
    if href.startswith(("#","javascript","mailto","tel")): return False
    if NAV_SKIP.match(name.strip()): return False
    from urllib.parse import urlparse
    p = urlparse(href if href.startswith("http") else f"https://rajras.in{href}")
    if p.netloc and "rajras.in" not in p.netloc: return False
    if p.path in ("","/" ,"/ras/pre/rajasthan/adm/schemes/"): return False
    return len(name.strip()) >= 4

def _get_section(soup, kw_re):
    content = soup.select_one(".entry-content") or soup.find("article") or soup
    for h in content.find_all(["h2","h3","h4"]):
        if not kw_re.search(h.get_text(strip=True)): continue
        level = int(h.name[1])
        parts = []
        for sib in h.next_siblings:
            if not isinstance(sib, Tag): continue
            if sib.name in ("h2","h3","h4") and int(sib.name[1]) <= level: break
            if sib.name in ("p","ul","ol","table","div"):
                t = sib.get_text(" ", strip=True)
                if t: parts.append(t)
        if parts: return " ".join(parts)[:400]
    return ""

def _fetch_article(url, session):
    try:
        r = session.get(url, headers=HEADERS, timeout=10, verify=False)
        r.raise_for_status()
        soup = BeautifulSoup(r.text, "html.parser")
        meta = soup.find("meta", attrs={"name":"description"})
        desc = meta.get("content","").strip() if meta else ""
        obj  = _get_section(soup, OBJECTIVE_KW)
        elig = _get_section(soup, ELIGIBILITY_KW)
        bene = _get_section(soup, BENEFIT_KW)
        # Apply URL
        apply_url = ""
        for a in soup.find_all("a", href=True):
            href = a.get("href","")
            if re.search(r"\.gov\.in|sso\.rajasthan|emitra", href, re.I) and "rajras.in" not in href:
                apply_url = href; break
        return {"description": desc or obj or bene, "objective": obj, "eligibility": elig, "benefit": bene, "apply_url": apply_url}
    except: return {}

def _parse_index(html):
    soup = BeautifulSoup(html, "html.parser")
    content = soup.select_one(".entry-content") or soup.find("article") or soup.find("main") or soup
    schemes, sector, subsector = [], "General", ""
    for el in content.children:
        if not isinstance(el, Tag): continue
        if el.name == "h2":
            t = el.get_text(strip=True).rstrip(":")
            if t and len(t)>2: sector = _sector(t); subsector = ""
        elif el.name in ("h3","h4"):
            t = el.get_text(strip=True).rstrip(":")
            if t and len(t)>2: subsector = t
        elif el.name in ("ul","ol"):
            for li in el.find_all("li", recursive=False):
                a = li.find("a", href=True)
                if a:
                    href, name = a.get("href",""), _clean(a.get_text(strip=True))
                    if _is_scheme_link(href, name):
                        schemes.append({"name":name,"sector":sector,"subsector":subsector,"url":urljoin(BASE_URL,href),"has_article":True})
                else:
                    name = _clean(li.get_text(strip=True))
                    if name and len(name)>=4:
                        schemes.append({"name":name,"sector":sector,"subsector":subsector,"url":"","has_article":False})
    return schemes

def scrape_rajras():
    ts  = datetime.now(timezone.utc).isoformat()
    ses = requests.Session()
    log.info("Fetching RajRAS index...")
    try:
        r = ses.get(INDEX_URL, headers=HEADERS, timeout=15, verify=False)
        r.raise_for_status()
        raw = _parse_index(r.text)
    except Exception as e:
        log.error("RajRAS index failed: %s", e)
        return _fallback(ts)

    if not raw: return _fallback(ts)
    log.info("RajRAS: %d schemes found, fetching top 30 articles...", len(raw))

    result = []
    for i, s in enumerate(raw):
        detail = _fetch_article(s["url"], ses) if s.get("has_article") and s.get("url") else {}
        result.append({
            "id": f"rajras_{i+1}",
            "name": s["name"],
            "category": s["sector"],
            "subcategory": s.get("subsector",""),
            "url": s.get("url",""),
            "description": detail.get("description") or f"Rajasthan government scheme — {s['sector']}",
            "objective":   detail.get("objective",""),
            "eligibility": detail.get("eligibility",""),
            "benefit":     detail.get("benefit",""),
            "apply_url":   detail.get("apply_url",""),
            "has_article": s.get("has_article", False),
            "status": "Active",
            "source": "rajras.in",
            "scraped_at": ts,
        })
        if i >= 49: break  # cap at 50 for performance

    log.info("RajRAS: %d schemes with details", len(result))
    return result

def _fallback(ts):
    schemes = [
        ("Palanhar Yojana","Social Welfare","https://rajras.in/palanhar-yojana/","Financial support for orphaned children","Children without parents","₹2,500/month per child"),
        ("Mukhyamantri Rajshri Yojana","Education","https://rajras.in/rajshri-yojana/","Promote education of girl child","Girls born after June 2016","₹50,000 in 6 instalments"),
        ("Chiranjeevi Health Insurance","Health","","Health coverage for all families","All Rajasthan families","₹25 lakh/year cashless"),
        ("Social Security Pension","Social Welfare","","Pension for elderly/disabled/widows","Elderly, disabled, widows","₹1,000-₹1,500/month"),
        ("MGNREGA Rajasthan","Rural Development","","Guaranteed wage employment","Rural adult job seekers","100 days/year guaranteed wages"),
        ("PM Kisan Samman Nidhi","Agriculture","","Income support to farmers","Small & marginal farmers","₹6,000/year"),
        ("Indira Rasoi Yojana","Food Security","","Subsidised meals at Rasoi centres","All citizens","₹8 per meal"),
        ("Jan Aadhaar","Digital & IT","","Single family identity for all benefits","Rajasthan families","All govt benefits via one card"),
        ("Kisan Rin Mafi","Agriculture","","Crop loan waiver","Farmers with crop loans","Waiver up to ₹2 lakh"),
        ("PM Awas Yojana Gramin","Housing","","Housing for rural homeless","BPL rural families","₹1.2 lakh per house"),
        ("Mukhyamantri Nishulk Dawa","Health","","Free medicines at govt hospitals","All patients at govt hospitals","Free medicines & diagnostics"),
        ("E-Mitra Services","Digital & IT","","Digital delivery of govt services","All citizens","1000+ govt services"),
        ("Scholarship SC/ST","Education","","Scholarship for marginalized students","SC/ST students","Full tuition + stipend"),
        ("Anuprati Coaching Scheme","Education","","Free competitive exam coaching","Students from poor families","Free coaching + stipend"),
        ("Mukhyamantri Laghu Udyog","Industry & Commerce","","Promote micro enterprises","Unemployed youth","Loans up to ₹25 lakh"),
    ]
    return [{"id":f"rajras_{i+1}","name":n,"category":c,"subcategory":"","url":u,"description":obj,"objective":obj,"eligibility":elig,"benefit":ben,"apply_url":"","has_article":bool(u),"status":"Active","source":"rajras.in (fallback)","scraped_at":ts} for i,(n,c,u,obj,elig,ben) in enumerate(schemes)]