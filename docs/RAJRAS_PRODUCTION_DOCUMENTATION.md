# RajRAS Production Scraping System Documentation

Last updated: March 16, 2026

## 1. Scope
This document explains the RajRAS scraping pipeline integrated into the existing FastAPI + React Rajasthan dashboard, including:
- What was changed
- What new components were added
- Tech stack used
- Data model and field mapping
- Runtime and deployment architecture
- Operations and troubleshooting

## 2. Summary Of Implemented Changes

### 2.1 Backend changes
- Added a new production scraper:
  - `backend/scrapers/rajras_full_scraper.py`
- Added persistent dataset output:
  - `backend/data/rajras_schemes.json`
- Added a dedicated API route:
  - `GET /data/rajras` in `backend/main.py`
- Added dependency:
  - `tqdm==4.67.1` in `backend/requirements.txt`

### 2.2 Frontend changes
- Updated `frontend/src/App.js` to fetch RajRAS dataset from:
  - `GET /data/rajras`
- Integrated RajRAS JSON records into the Schemes tab card pipeline.
- Removed fake implementation fallback progress (`70%`/`40%`) behavior.
- Added strict progress rendering:
  - Show ring/bar only when real progress exists.
  - Show explicit “No official implementation progress…” when missing.
- Added normalization mapping so RajRAS JSON fields align with existing UI model.

## 3. Files Added / Modified

### Added
- `backend/scrapers/rajras_full_scraper.py`
- `backend/data/rajras_schemes.json`
- `docs/RAJRAS_PRODUCTION_DOCUMENTATION.md`

### Modified
- `backend/main.py`
- `backend/requirements.txt`
- `frontend/src/App.js`

## 4. Tech Stack Used

### Backend
- Python 3.x
- FastAPI (API layer)
- Uvicorn (ASGI runtime)
- requests (HTTP client)
- BeautifulSoup4 (HTML parsing)
- urllib3 Retry + requests HTTPAdapter (retry/backoff)
- tqdm (progress visibility during scraping)
- JSON file storage (initial persistence layer)

### Frontend
- React
- Axios (API client)
- Existing dashboard UI components in `App.js`

## 5. RajRAS Scraper Design

## 5.1 Source
- Index page:
  - `https://rajras.in/ras/pre/rajasthan/adm/schemes/`
- Scheme pages:
  - individual RajRAS article URLs discovered from index links

## 5.2 Pipeline stages
1. Fetch index page.
2. Extract all candidate scheme links from main content.
3. Normalize and deduplicate links.
4. Visit each link with retries/timeouts.
5. Parse headings/paragraphs/lists into semantic sections.
6. Extract structured fields.
7. Extract progress signal only if reliable context exists.
8. Save final JSON dataset to `backend/data/rajras_schemes.json`.

## 5.3 Resilience features
- User-Agent headers
- Timeout handling
- Automatic retries on 429/5xx
- Polite delay between requests
- Exception handling per page (does not crash whole run)
- Safe null fallback for missing fields
- Deduplication by URL

## 6. Data Schema

Each record in `rajras_schemes.json` follows:

```json
{
  "id": "rajras_001",
  "name": "Scheme Name",
  "category": "Health",
  "description": "Text...",
  "headings": ["Overview", "Benefits"],
  "benefits": ["..."],
  "eligibility": ["..."],
  "documents_required": ["..."],
  "progress_pct": 80.0,
  "progress": "80.0%",
  "progress_source": "Source sentence used for extraction",
  "progress_updated_at": "2026-03-16T17:25:35.120000+00:00",
  "source": "RajRAS",
  "url": "https://rajras.in/..."
}
```

Field extraction rules:
- Missing fields are stored as `null`.
- `progress_pct` is only set when percentage appears in implementation/coverage context.
- No synthetic/fake progress is generated.

## 7. API Layer

### 7.1 Dedicated RajRAS endpoint
- Route: `GET /data/rajras`
- Behavior:
  - Reads `backend/data/rajras_schemes.json`
  - Returns JSON array directly
  - Returns 404 if file is not present

### 7.2 Existing aggregate route
- Route: `GET /aggregate`
- Still used by frontend for non-RajRAS datasets and merged analytics.

## 8. Frontend Integration Behavior

Schemes tab now composes data as:
1. Fetch `/aggregate` (existing multi-source feed)
2. Fetch `/data/rajras` (file-backed RajRAS feed)
3. Replace only aggregate RajRAS rows with file-backed RajRAS rows
4. Keep Jan Soochna and MyScheme from aggregate feed

Progress display behavior:
- If `progress_pct` exists:
  - show ring/bar + percent
- If `progress_pct` does not exist:
  - show “No official implementation progress is available from current source data.”

## 9. Architecture

### 9.1 Logical architecture

```text
                 +---------------------------+
                 |       RajRAS Website      |
                 |  index + scheme articles  |
                 +-------------+-------------+
                               |
                               v
                 +---------------------------+
                 | rajras_full_scraper.py    |
                 | requests + BS4 + retries  |
                 +-------------+-------------+
                               |
                      writes JSON dataset
                               |
                               v
                 +---------------------------+
                 | backend/data/             |
                 | rajras_schemes.json       |
                 +-------------+-------------+
                               |
                               v
                 +---------------------------+
                 | FastAPI route             |
                 | GET /data/rajras          |
                 +-------------+-------------+
                               |
                               v
                 +---------------------------+
                 | React App (Schemes Tab)   |
                 | merges /aggregate + rajras|
                 +---------------------------+
```

### 9.2 Runtime architecture in current app

```text
Frontend (React)
  |- GET /status
  |- GET /aggregate
  |- GET /data/rajras
  |- POST /scrape/all, /scrape/{source}

Backend (FastAPI)
  |- In-memory cache for aggregate source scrapers
  |- File-backed RajRAS dataset endpoint
  |- Existing source scrapers (IGOD, RajRAS, Jan Soochna, MyScheme)
```

## 10. Operational Runbook

## 10.1 Generate/refresh RajRAS dataset
```bash
cd backend
python3 -m scrapers.rajras_full_scraper
```

## 10.2 Start backend
```bash
cd backend
uv run uvicorn main:app --reload --port 8000
```

## 10.3 Start frontend against local backend
```bash
cd frontend
REACT_APP_API_URL=http://localhost:8000 npm start
```

## 10.4 Validate
- Open `http://localhost:8000/data/rajras`
- Open frontend Schemes tab and filter `RajRAS`

## 11. Known Limitations
- RajRAS pages are descriptive; many do not provide explicit implementation percentages.
- As a result, `progress_pct` is often null.
- JSON file storage may be ephemeral on some hosting platforms (depending on deployment model).

## 12. Recommended Production Hardening
- Move dataset persistence from local file to DB/object storage.
- Add scheduled scraper job (cron/APScheduler/Celery Beat).
- Add incremental cache (ETag/Last-Modified/hash-based skip).
- Add source-level observability:
  - scrape success rate
  - extract completeness metrics
  - changed-record counts
- Extend progress from scheme-specific MIS portals for better coverage.

## 13. Change Log

### 2026-03-16
- Added full RajRAS scraper pipeline module
- Added `/data/rajras` endpoint
- Added frontend consumption of file-backed RajRAS dataset
- Replaced fake fallback progress with truthful null-state UI
- Added context-aware progress extraction with metadata
