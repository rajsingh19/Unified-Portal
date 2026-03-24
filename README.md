# 🏛️ Rajasthan Unified Portal Dashboard

A full-stack government data dashboard for Rajasthan, India. Scrapes live data from Jan Soochna, RajRAS, MyScheme, IGOD, JJM, PMKSY, and more — serving it through a FastAPI backend to a React frontend.

---

## 📁 Project Structure

```
RJ-Portal/
├── backend/                  # Python FastAPI API server
│   ├── main.py               # All API routes + business logic
│   ├── requirements.txt      # Python dependencies
│   └── scrapers/             # One scraper per data source
│       ├── __init__.py
│       ├── igod_scraper.py
│       ├── rajras_scraper.py
│       ├── jansoochna_scraper.py
│       ├── jansoochna_full_scraper.py
│       ├── myscheme_scraper.py
│       ├── budget_scraper.py
│       ├── jjm_scraper.py
│       ├── pmksy_scraper.py
│       ├── scheme_dashboard_scraper.py
│       └── ...
│
├── frontend/                 # React + Tailwind CSS dashboard
│   ├── public/index.html
│   ├── package.json
│   ├── tailwind.config.js
│   ├── postcss.config.js
│   └── src/
│       ├── index.js
│       ├── index.css
│       ├── App.js
│       ├── InsightsEngine.js # AI insights panel
│       └── legacy/
│           └── LegacyDashboardApp.js  # Main dashboard UI
│
├── scripts/                  # Node.js district-level data fetchers
│   ├── fetch_all.js          # Run all scheme fetchers
│   ├── package.json
│   └── schemes/
│       ├── common.js
│       ├── jjm.js
│       ├── mgnrega.js
│       ├── pmay.js
│       ├── pmfby.js
│       ├── pmkisan.js
│       └── sbm.js
│
├── docs/                     # Documentation
│   ├── SCRAPERS.md
│   └── RAJRAS_PRODUCTION_DOCUMENTATION.md
│
├── .env.example              # Environment variable template
├── .gitignore                # Comprehensive ignore rules
├── package.json              # Root convenience scripts
├── start.sh                  # One-command dev startup
└── README.md
```

---

## ⚙️ Prerequisites

| Tool | Version | Notes |
|------|---------|-------|
| Python | 3.10+ | For the FastAPI backend |
| Node.js | 18+ | For React frontend & scripts |
| npm | 9+ | Comes with Node.js |
| pip or uv | latest | Python package manager |

---

## 🚀 Running Locally

### Option A — One command (recommended)

```bash
# Clone the repo
git clone https://github.com/YOUR_USERNAME/rj-portal.git
cd rj-portal

# Copy and fill in your environment variables
cp .env.example .env
# Edit .env and add your ANTHROPIC_API_KEY

# Start both backend and frontend
bash start.sh
```

This starts:
- **Backend** → http://localhost:8000
- **Frontend** → http://localhost:3000
- **API Docs** → http://localhost:8000/docs

---

### Option B — Manual (step by step)

#### 1. Backend

```bash
cd backend

# Create and activate a virtual environment
python -m venv .venv
source .venv/bin/activate        # Windows: .venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt

# Install Playwright browsers (needed for JS-rendered scrapers)
playwright install chromium

# Start the API server
uvicorn main:app --reload --port 8000
```

#### 2. Frontend (in a new terminal)

```bash
cd frontend

npm install
npm start
```

The frontend proxies API calls to `http://localhost:8000` automatically (configured in `package.json`).

---

### Option C — Run Node.js scheme fetchers (optional)

These fetch district-level scheme data and write JSON output files.

```bash
cd scripts

npm install
node fetch_all.js
```

---

## 🌐 Key API Endpoints

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/` | GET | Health check |
| `/status` | GET | Scraper status summary |
| `/aggregate` | GET | All data merged (used by frontend) |
| `/scrape/all` | POST | Trigger all scrapers |
| `/scrape/{source}` | POST | Trigger one scraper |
| `/data/{source}` | GET | Raw data for a source |
| `/jjm` | GET | JJM district coverage |
| `/pmksy` | GET | PMKSY irrigation data |
| `/budget` | GET | Budget & financial data |
| `/insights` | POST | AI-generated policy insights (requires `ANTHROPIC_API_KEY`) |
| `/docs` | GET | Swagger interactive API docs |

---

## Render Deployment

This repo now includes a root [render.yaml](/home/raj/Downloads/RJ-Portal-Clean/render.yaml) Blueprint for deploying both services on Render:

- `rj-portal-backend` as a Python web service
- `rj-portal-frontend` as a static site

### What the Blueprint config does

- Builds the backend with `pip install -r backend/requirements.txt`
- Starts FastAPI with `cd backend && uvicorn main:app --host 0.0.0.0 --port $PORT`
- Builds the frontend with `cd frontend && npm ci && npm run build`
- Publishes the React build output as a static site
- Rewrites all frontend routes to `/index.html` for client-side routing

### Deploy on Render

1. Push this repo to GitHub or GitLab.
2. In Render, choose **New +** → **Blueprint**.
3. Select this repository.
4. Render will detect `render.yaml` and propose both services.
5. During setup, provide these environment values:
   - On `rj-portal-backend`, set `CORS_ORIGINS` to your frontend Render URL
   - On `rj-portal-frontend`, set `REACT_APP_API_URL` to your backend Render URL
6. Provide any secret values when prompted:
   - `ANTHROPIC_API_KEY` if you want the `/insights` endpoint enabled
   - `DATA_GOV_API_KEY` if you want to override the default demo key
7. Create the Blueprint and wait for both deploys to finish.

Important:
- Do not create a single root Node web service for this repo on Render.
- If Render runs `yarn start` from the repository root, it will try to execute the local dev script instead of the production split deployment.
- Use the root `render.yaml` Blueprint, or manually create two Render services: one static frontend and one Python backend.

### Notes

- If you deploy the backend first, its Render URL will usually look like `https://your-backend-name.onrender.com`.
- After the frontend is live, copy its URL into `CORS_ORIGINS` on the backend if you did not set it during initial Blueprint setup.
- Backend scrape caches are in memory, so a backend restart clears them until the next scrape/startup refresh.
- JSON files written under `backend/data/` are not durable on a standard Render web service filesystem. If you want long-lived file persistence for generated datasets, add a persistent disk or move these files to object storage / a database.

---

## 🔑 Environment Variables

Copy `.env.example` to `.env` before running:

```bash
cp .env.example .env
```

| Variable | Required | Description |
|----------|----------|-------------|
| `ANTHROPIC_API_KEY` | Yes (for `/insights`) | Claude API key from console.anthropic.com |
| `DATA_GOV_API_KEY` | No | data.gov.in API key (has a public default) |
| `REACT_APP_API_URL` | No locally, yes for separate frontend hosting | Frontend API base URL |
| `CORS_ORIGINS` | No locally, yes for locked-down production CORS | Comma-separated allowed frontend origins |

---

## 🧹 What Was Cleaned

- ❌ Removed `frontend/.tmp/` (96 compiled cache files, ~5 MB)
- ❌ Removed scattered `.gitignore` files → merged into one root `.gitignore`
- ❌ Removed `LEARNING_README.md` from root gitignore
- ✅ Moved `fetch_all.js` and `schemes/` into `scripts/` folder
- ✅ Added `.env.example` so secrets are never committed
- ✅ Added root `package.json` with convenience scripts
- ✅ Fixed `start.sh` to use absolute paths (works from any directory)
- ✅ Added `backend/data/` to `.gitignore` (scraped JSON, not source code)

---

## 📦 Before Pushing to GitHub

```bash
# 1. Make sure .env is NOT committed
echo ".env" >> .gitignore
git status   # confirm .env is not listed

# 2. Delete any leftover cache/junk
find . -name "__pycache__" -exec rm -rf {} + 2>/dev/null
find . -name ".tmp" -exec rm -rf {} + 2>/dev/null
find . -name "node_modules" -exec rm -rf {} + 2>/dev/null

# 3. Initialise git and push
git init
git add .
git commit -m "Initial clean commit — RJ Portal"
git remote add origin https://github.com/YOUR_USERNAME/rj-portal.git
git push -u origin main
```

---

## 🛠️ Improvement Suggestions

See [docs/SCRAPERS.md](docs/SCRAPERS.md) for scraper-specific notes.

**Architecture:**
- Consider adding a `docker-compose.yml` to containerise backend + frontend for easy deployment
- Add a Redis layer to persist scraper cache between server restarts (currently in-memory only)
- Move the `InsightsEngine.js` AI call to a dedicated backend route to keep the API key server-side only

**Frontend:**
- Split `LegacyDashboardApp.js` (large monolithic component) into smaller files under `src/components/`
- Add React Query or SWR for data fetching with caching and background refetch

**Backend:**
- Add rate limiting to `/scrape/*` endpoints to prevent abuse
- Add a simple auth token for the `/insights` and `/scrape/all` endpoints
- Log scraper errors to a file, not just stderr

---

## 📄 License

MIT
