"""
agents/dashboard_agent.py
WBD APAC — Title Performance Analyst

Streamlit integration wrapper. Thin bridge between the dashboard UI
and the full Orchestrator + Quality Critic pipeline.

Responsibilities:
  - Accept a question string from the Streamlit UI
  - Run it through the Orchestrator (which gates via the Critic)
  - Return a structured result dict ready for Streamlit rendering
  - Collect pipeline events for the live status display

Model: Claude Sonnet 4.6 (via Orchestrator — no direct model calls here)
"""

import os
import re
import sys
import time

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))
from agents.orchestrator import Orchestrator


# ─────────────────────────────────────────────────────────────
# CATEGORY METADATA  (for UI labels and colours)
# ─────────────────────────────────────────────────────────────

CATEGORY_META = {
    "A": {"label": "Diagnosis",        "icon": "🔍", "color": "#DC2626"},
    "B": {"label": "Snapshot",         "icon": "📊", "color": "#4f46e5"},
    "C": {"label": "Trend",            "icon": "📈", "color": "#0ea5e9"},
    "D": {"label": "Genre & Catalog",  "icon": "🎭", "color": "#7c3aed"},
    "E": {"label": "Subscriber",       "icon": "👤", "color": "#16a34a"},
    "F": {"label": "Alerts",           "icon": "🚨", "color": "#d97706"},
}

VERDICT_META = {
    "approved": {"badge": "✅", "color": "#16a34a", "label": "Approved"},
    "enhanced": {"badge": "⚡", "color": "#d97706", "label": "Enhanced"},
    "revised":  {"badge": "🔧", "color": "#dc2626", "label": "Revised"},
}

SUGGESTED_QUESTIONS = [
    ("A", "Why is The White Lotus S3 underperforming in Southeast Asia?"),
    ("B", "How is House of the Dragon S2 performing vs comparable fantasy titles?"),
    ("C", "Is The Last of Us S2 gaining or losing momentum week over week?"),
    ("D", "Which titles have high starts but low completions right now?"),
    ("E", "Does watching House of the Dragon reduce churn risk?"),
    ("F", "Which titles need immediate attention this week?"),
]


class DashboardAgent:
    """
    Streamlit-friendly wrapper around the full agent pipeline.
    Instantiate once and reuse across Streamlit reruns via
    st.cache_resource.
    """

    def __init__(self):
        self.orchestrator = Orchestrator()

    def run_query(self, question: str) -> dict:
        """
        Run a question through the full pipeline and return a
        dashboard-ready result dict.

        Returns:
            question      — original question
            response      — final markdown text (includes quality badge)
            category      — A–F or out_of_scope
            cat_label     — human label (e.g. "Diagnosis")
            cat_icon      — emoji icon
            cat_color     — hex colour for UI highlights
            score         — quality score integer (or None)
            verdict       — "approved" | "enhanced" | "revised"
            verdict_badge — e.g. "✅"
            verdict_color — hex colour
            verdict_label — e.g. "Approved"
            pipeline      — list of (agent, event, detail) tuples
            elapsed_s     — wall-clock seconds
            error         — None on success
        """
        pipeline_events = []

        def on_status(agent, event, detail=""):
            pipeline_events.append((agent, event, detail))

        t0     = time.time()
        result = self.orchestrator.run(question, on_status=on_status)
        elapsed = round(time.time() - t0, 1)

        category = result.get("category", "B")
        response = result.get("response", "")
        error    = result.get("error")

        # ── Extract quality score from critic badge on first line ─
        score   = None
        verdict = "approved"
        if response:
            first_line = response.split("\n")[0]
            m = re.search(r"(\d+)/10", first_line)
            if m:
                score = int(m.group(1))
            if "Enhanced" in first_line:
                verdict = "enhanced"
            elif "Revised" in first_line:
                verdict = "revised"

        cat  = CATEGORY_META.get(category, {"label": category, "icon": "❓", "color": "#64748b"})
        verd = VERDICT_META.get(verdict, VERDICT_META["approved"])

        return {
            "question":      question,
            "response":      response,
            "category":      category,
            "cat_label":     cat["label"],
            "cat_icon":      cat["icon"],
            "cat_color":     cat["color"],
            "score":         score,
            "verdict":       verdict,
            "verdict_badge": verd["badge"],
            "verdict_color": verd["color"],
            "verdict_label": verd["label"],
            "pipeline":      pipeline_events,
            "elapsed_s":     elapsed,
            "error":         error,
        }
