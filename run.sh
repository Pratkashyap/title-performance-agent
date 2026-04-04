#!/bin/bash
# ─────────────────────────────────────────────────────────────
# Title Performance Analyst — Start dashboard
# Run: bash run.sh
# ─────────────────────────────────────────────────────────────

source venv/bin/activate
streamlit run dashboard_app.py --server.port 8501
