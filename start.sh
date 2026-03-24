#!/bin/bash
# ════════════════════════════════════════════
#  RJ Portal — Local Dev Startup Script
#  Starts FastAPI backend + React frontend
# ════════════════════════════════════════════

set -e
ROOT_DIR="$(cd "$(dirname "$0")" && pwd)"

echo "============================================"
echo "  Rajasthan Portal — Starting Dev Servers"
echo "============================================"

# ── Backend ───────────────────────────────────
echo ""
echo "[1/2] Starting Python Backend (FastAPI)..."
cd "$ROOT_DIR/backend"

if ! command -v uvicorn &>/dev/null && ! command -v uv &>/dev/null; then
  echo "  → No uvicorn or uv found. Installing via pip..."
  pip install -r requirements.txt -q
fi

if command -v uv &>/dev/null; then
  uv pip install -r requirements.txt -q 2>/dev/null || true
  uv run uvicorn main:app --reload --port 8000 &
else
  uvicorn main:app --reload --port 8000 &
fi

BACKEND_PID=$!
echo "  ✓ Backend started (PID: $BACKEND_PID)"

# ── Frontend ──────────────────────────────────
echo ""
echo "[2/2] Starting React Frontend..."
cd "$ROOT_DIR/frontend"
npm install -q
npm start &
FRONTEND_PID=$!
echo "  ✓ Frontend started (PID: $FRONTEND_PID)"

echo ""
echo "============================================"
echo "  Backend  → http://localhost:8000"
echo "  Frontend → http://localhost:3000"
echo "  API Docs → http://localhost:8000/docs"
echo "============================================"
echo ""
echo "Press Ctrl+C to stop both servers"

trap "echo 'Stopping...'; kill $BACKEND_PID $FRONTEND_PID 2>/dev/null; exit" INT TERM
wait
