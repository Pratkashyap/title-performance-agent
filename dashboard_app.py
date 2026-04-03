"""
dashboard_app.py
Title Performance Analyst — AI-First Dashboard
APAC · Max / HBO Max

Run:
    cd ~/Desktop/title-performance-agent
    source venv/bin/activate
    streamlit run dashboard_app.py
"""

import os, sys
import duckdb
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
import streamlit as st

DB_PATH = os.path.join(os.path.dirname(__file__), "data", "title_performance.duckdb")

st.set_page_config(
    page_title="Title Performance Analyst",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ─────────────────────────────────────────────────────────────
# GLOBAL STYLES
# ─────────────────────────────────────────────────────────────
st.markdown("""
<style>
/* App background */
.stApp { background:#F0F2F8 !important; }
.block-container { padding-top:0.6rem !important; padding-bottom:1rem !important; }
#MainMenu, footer, header { visibility:hidden; }

/* Sidebar */
section[data-testid="stSidebar"] {
    background:#1E2130 !important;
    border-right:1px solid #2d3048;
    min-width:240px !important;
}
section[data-testid="stSidebar"] * { color:#CBD5E1 !important; }
section[data-testid="stSidebar"] .stButton > button {
    background:#2a2f47 !important;
    color:#CBD5E1 !important;
    border:1px solid #3d4461 !important;
    border-radius:5px !important;
    font-size:12px !important;
    font-weight:500 !important;
    text-align:left !important;
    padding:5px 10px !important;
    margin-bottom:2px !important;
}
section[data-testid="stSidebar"] .stButton > button:hover {
    background:#3b4268 !important;
    color:#fff !important;
    border-color:#6366f1 !important;
}

/* Top header bar */
.app-header {
    background:#1E2130;
    color:#fff;
    padding:10px 20px;
    border-radius:8px;
    margin-bottom:14px;
    display:flex;
    align-items:center;
    justify-content:space-between;
}
.app-header-title { font-size:16px; font-weight:800; color:#fff; }
.app-header-sub   { font-size:12px; color:#94A3B8; margin-top:2px; }
.app-header-badge {
    background:#4f46e5; color:#fff;
    border-radius:20px; padding:3px 12px;
    font-size:11px; font-weight:700;
}

/* Tabs */
.stTabs [data-baseweb="tab-list"] {
    background:#fff;
    border:1px solid #E2E8F0;
    border-radius:7px;
    padding:2px;
    gap:2px;
}
.stTabs [data-baseweb="tab"] {
    color:#64748B; font-weight:600; font-size:13px;
    border-radius:5px; padding:7px 18px; background:transparent;
}
.stTabs [aria-selected="true"] { background:#4f46e5 !important; color:#fff !important; }

/* Chat input area */
.chat-header {
    font-size:15px; font-weight:700; color:#0F172A;
    margin-bottom:12px;
}
.chat-subhead { font-size:13px; color:#64748B; margin-bottom:16px; }

/* Result cards */
.result-card {
    background:#fff;
    border:1px solid #E2E8F0;
    border-radius:10px;
    padding:18px 20px;
    margin-bottom:14px;
}
.result-card-new {
    background:#FAFBFF;
    border:1.5px solid #C7D2FE;
    border-radius:10px;
    padding:18px 20px;
    margin-bottom:14px;
}

/* Pipeline panel */
.pipe-header {
    font-size:12px; font-weight:700;
    text-transform:uppercase; letter-spacing:.7px;
    color:#64748B; margin-bottom:10px;
}
.pipe-event {
    font-family:monospace; font-size:11px; color:#475569;
    padding:3px 0; border-bottom:1px solid #F1F5F9;
    display:flex; align-items:baseline; gap:6px;
}
.pipe-done  { color:#16A34A !important; }
.pipe-run   { color:#4f46e5 !important; }
.pipe-time  { color:#94A3B8; font-size:10px; margin-left:auto; }
.pipe-section { font-size:10px; font-weight:700; color:#94A3B8;
    text-transform:uppercase; letter-spacing:.5px;
    margin:8px 0 3px; }

/* KPI cards */
.kpi { background:#fff; border:1px solid #E2E8F0; border-radius:10px; padding:16px 18px; }
.kpi-label { color:#64748B; font-size:10px; font-weight:700; letter-spacing:.8px;
             text-transform:uppercase; margin-bottom:5px; }
.kpi-value { color:#0f172a; font-size:24px; font-weight:800; line-height:1; }
.kpi-sub   { color:#64748B; font-size:11px; margin-top:4px; }
.kpi-up    { color:#16A34A; font-size:11px; margin-top:4px; font-weight:600; }
.kpi-down  { color:#DC2626; font-size:11px; margin-top:4px; font-weight:600; }
.kpi-warn  { color:#D97706; font-size:11px; margin-top:4px; font-weight:600; }

/* Section headings */
.sec { font-size:13px; font-weight:700; color:#0F172A;
       border-left:3px solid #4f46e5; padding-left:8px; margin:16px 0 8px; }

/* Pills */
.pill-red   { display:inline-block; background:#FEF2F2; border:1px solid #FCA5A5;
              color:#DC2626; border-radius:20px; padding:2px 10px; font-size:11px; font-weight:600; }
.pill-amber { display:inline-block; background:#FFFBEB; border:1px solid #FCD34D;
              color:#D97706; border-radius:20px; padding:2px 10px; font-size:11px; font-weight:600; }
.pill-green { display:inline-block; background:#F0FDF4; border:1px solid #86EFAC;
              color:#16A34A; border-radius:20px; padding:2px 10px; font-size:11px; font-weight:600; }

/* Filter bar in data tabs */
.filter-bar {
    background:#fff; border:1px solid #E2E8F0; border-radius:8px;
    padding:10px 16px; margin-bottom:16px;
    display:flex; align-items:center; gap:12px;
}
.filter-label {
    font-size:10px; font-weight:700; color:#64748B;
    text-transform:uppercase; letter-spacing:.5px;
}

/* Main ask button */
div[data-testid="stButton"] button[kind="primary"] {
    background:#4f46e5 !important;
    color:#fff !important;
    border:none !important;
    font-weight:700 !important;
    font-size:14px !important;
    border-radius:6px !important;
    padding:8px 0 !important;
}
div[data-testid="stButton"] button[kind="primary"]:hover {
    background:#4338ca !important;
}
</style>
""", unsafe_allow_html=True)


# ─────────────────────────────────────────────────────────────
# DATA HELPERS
# ─────────────────────────────────────────────────────────────
@st.cache_data(ttl=60)
def run(sql: str) -> pd.DataFrame:
    con = duckdb.connect(DB_PATH, read_only=True)
    df  = con.execute(sql).df()
    con.close()
    return df


def fmt_k(n):
    if n >= 1_000_000: return f"{n/1_000_000:.1f}M"
    if n >= 1_000:     return f"{n/1_000:.0f}K"
    return str(int(n))


COLORS = {
    "indigo": "#4f46e5", "purple": "#7c3aed", "sky": "#0ea5e9",
    "green":  "#16a34a", "amber":  "#d97706", "red": "#dc2626",
    "slate":  "#64748b",
}
CHART_BG   = "#FFFFFF"
GRID_COLOR = "#E2E8F0"


# ─────────────────────────────────────────────────────────────
# SESSION STATE
# ─────────────────────────────────────────────────────────────
if "ai_history"  not in st.session_state: st.session_state.ai_history  = []
if "ai_prefill"  not in st.session_state: st.session_state.ai_prefill  = ""
if "ai_inp_key"  not in st.session_state: st.session_state.ai_inp_key  = 0


# ─────────────────────────────────────────────────────────────
# AGENT IMPORTS
# ─────────────────────────────────────────────────────────────
sys.path.insert(0, os.path.dirname(__file__))
from agents.dashboard_agent import DashboardAgent, SUGGESTED_QUESTIONS, CATEGORY_META

@st.cache_resource
def get_agent():
    return DashboardAgent()

agent = get_agent()


# ─────────────────────────────────────────────────────────────
# SAMPLE QUERIES
# ─────────────────────────────────────────────────────────────
ALL_SAMPLES = [
    ("A", "🔍", "#DC2626", "Why is Euphoria S2 underperforming in Southeast Asia?"),
    ("A", "🔍", "#DC2626", "Why did viewership drop after episode 3 for The Idol?"),
    ("B", "📊", "#4f46e5", "How is House of the Dragon S2 performing vs comparable fantasy titles?"),
    ("B", "📊", "#4f46e5", "Compare The Last of Us S2 vs genre benchmarks across all APAC markets"),
    ("C", "📈", "#0ea5e9", "Is The Last of Us S2 gaining or losing momentum week over week?"),
    ("C", "📈", "#0ea5e9", "Which markets are growing fastest for House of the Dragon this month?"),
    ("D", "🎭", "#7c3aed", "Which titles have high starts but low completions right now?"),
    ("D", "🎭", "#7c3aed", "Give me a full genre health report for APAC this month"),
    ("E", "👤", "#16a34a", "Does watching House of the Dragon reduce churn risk for subscribers?"),
    ("E", "👤", "#16a34a", "What subscriber segments watch The White Lotus the most?"),
    ("F", "🚨", "#d97706", "Which titles need immediate attention this week?"),
    ("F", "🚨", "#d97706", "Give me the full weekly alert bulletin for APAC"),
]

CAT_LABELS = {
    "A": "Diagnosis", "B": "Snapshot", "C": "Trend",
    "D": "Genre & Catalog", "E": "Subscriber", "F": "Alerts",
}

AGENT_ICONS = {
    "orchestrator":        ("🧠", "Orchestrator",          "Classifying & routing"),
    "data_agent":          ("📊", "Data Agent",            "SQL → DuckDB"),
    "performance_analyst": ("🔍", "Performance Analyst",   "Diagnosing performance"),
    "benchmark_agent":     ("📐", "Benchmark Agent",       "Peer comparison"),
    "trend_agent":         ("📈", "Trend Agent",           "WoW momentum"),
    "genre_catalog_agent": ("🎭", "Genre Catalog Agent",   "Genre health"),
    "subscriber_agent":    ("👤", "Subscriber Agent",      "Churn signals"),
    "alert_agent":         ("🚨", "Alert Agent",           "Urgent issues"),
    "critic_agent":        ("⚖️",  "Quality Critic",        "Scoring 0–10"),
}


# ─────────────────────────────────────────────────────────────
# SIDEBAR — Quick Queries + Agent Roster
# ─────────────────────────────────────────────────────────────
with st.sidebar:
    st.markdown(
        "<div style='font-size:15px;font-weight:800;color:#fff;margin-bottom:2px;'>"
        "🎬 Title Performance</div>"
        "<div style='font-size:11px;color:#64748B;margin-bottom:16px;'>"
        "APAC Analytics · Max / HBO Max</div>",
        unsafe_allow_html=True,
    )

    st.markdown(
        "<div style='font-size:10px;font-weight:700;color:#64748B;"
        "text-transform:uppercase;letter-spacing:.8px;margin-bottom:8px;'>"
        "Quick Queries</div>",
        unsafe_allow_html=True,
    )

    cat_groups = {}
    for cat, icon, color, q in ALL_SAMPLES:
        cat_groups.setdefault(cat, []).append((icon, color, q))

    for cat, samples in cat_groups.items():
        icon0, color0, _ = samples[0]
        st.markdown(
            f"<div style='font-size:9px;font-weight:700;letter-spacing:.5px;"
            f"text-transform:uppercase;color:{color0};margin:8px 0 3px;'>"
            f"{icon0} {CAT_LABELS.get(cat,'')}</div>",
            unsafe_allow_html=True,
        )
        for icon, color, q in samples:
            label = q[:46] + "…" if len(q) > 46 else q
            if st.button(label, key=f"sq_{cat}_{q[:18]}", use_container_width=True):
                st.session_state.ai_prefill = q
                st.session_state.ai_inp_key += 1
                st.rerun()

    st.markdown("<div style='margin:14px 0;border-top:1px solid #2d3048;'></div>",
                unsafe_allow_html=True)

    st.markdown(
        "<div style='font-size:10px;font-weight:700;color:#64748B;"
        "text-transform:uppercase;letter-spacing:.8px;margin-bottom:8px;'>"
        "Active Agents</div>",
        unsafe_allow_html=True,
    )
    for key, (icon, name, desc) in AGENT_ICONS.items():
        st.markdown(
            f"<div style='font-size:11px;color:#94A3B8;padding:3px 0;'>"
            f"{icon} {name}</div>",
            unsafe_allow_html=True,
        )


# ─────────────────────────────────────────────────────────────
# TOP HEADER BAR
# ─────────────────────────────────────────────────────────────
h1, h2 = st.columns([4, 1])
with h1:
    st.markdown(
        "<div class='app-header'>"
        "<div>"
        "<div class='app-header-title'>🎬 TITLE PERFORMANCE ANALYST</div>"
        "<div class='app-header-sub'>APAC · Max / HBO Max · AI-Powered Streaming Analytics</div>"
        "</div>"
        "</div>",
        unsafe_allow_html=True,
    )
with h2:
    st.markdown(
        "<div style='text-align:right;padding-top:8px;'>"
        "<span style='background:#4f46e5;color:#fff;border-radius:20px;"
        "padding:4px 14px;font-size:11px;font-weight:700;'>9 Agents · Live</span>"
        "</div>",
        unsafe_allow_html=True,
    )


# ─────────────────────────────────────────────────────────────
# TABS — Agent Chat FIRST
# ─────────────────────────────────────────────────────────────
tab_ai, tab1, tab2, tab3, tab4, tab5 = st.tabs([
    "🤖 Agent Chat", "📊 Overview", "🌏 Markets", "🎭 Genre Health",
    "📉 Episode Drop-off", "🚨 Alerts",
])


# ══════════════════════════════════════════════════════════════
# TAB: AGENT CHAT
# ══════════════════════════════════════════════════════════════
with tab_ai:
    chat_col, pipe_col = st.columns([3, 1], gap="medium")

    # ── PIPELINE PANEL (right) ─────────────────────────────────
    with pipe_col:
        st.markdown(
            "<div style='background:#1E2130;border-radius:8px;padding:14px 14px 10px;'>"
            "<div class='pipe-header' style='color:#94A3B8;'>📡 Pipeline</div>",
            unsafe_allow_html=True,
        )

        if not st.session_state.ai_history:
            st.markdown(
                "<div style='font-size:11px;color:#475569;padding:6px 0;'>"
                "Ask a question to see agent activity here.</div>",
                unsafe_allow_html=True,
            )
        else:
            last = st.session_state.ai_history[0]
            pipeline = last.get("pipeline", [])
            elapsed  = last.get("elapsed_s", 0)

            # Group events by agent
            agent_events: dict = {}
            for ag, ev, detail in pipeline:
                agent_events.setdefault(ag, []).append((ev, detail))

            for ag, events in agent_events.items():
                ic, lbl, _ = AGENT_ICONS.get(ag, ("·", ag, ""))
                is_done = any(e == "done" for e, _ in events)
                done_detail = next((d for e, d in events if e == "done"), "")
                color = "#16A34A" if is_done else "#4f46e5"

                st.markdown(
                    f"<div style='border-bottom:1px solid #2d3048;padding:5px 0;'>"
                    f"<div style='font-size:12px;color:{color};font-weight:600;'>"
                    f"{ic} {lbl}</div>",
                    unsafe_allow_html=True,
                )
                for ev, detail in events:
                    if ev in ("start", "done"):
                        continue
                    ev_color = "#64748B"
                    st.markdown(
                        f"<div style='font-size:10px;font-family:monospace;"
                        f"color:{ev_color};padding-left:14px;'>"
                        f"↳ {ev}"
                        f"{(' · ' + str(detail)[:35]) if detail else ''}</div>",
                        unsafe_allow_html=True,
                    )
                if done_detail:
                    st.markdown(
                        f"<div style='font-size:10px;color:#16A34A;"
                        f"font-family:monospace;padding-left:14px;'>"
                        f"✓ done · {done_detail}</div>",
                        unsafe_allow_html=True,
                    )
                st.markdown("</div>", unsafe_allow_html=True)

            sc = last.get("score")
            sc_col = "#16a34a" if (sc or 0) >= 8 else "#d97706" if (sc or 0) >= 6 else "#dc2626"
            st.markdown(
                f"<div style='margin-top:10px;font-size:11px;color:#94A3B8;'>"
                f"⏱ {elapsed}s total"
                f"{'  ·  <span style=\"color:' + sc_col + ';font-weight:700;\">Quality ' + str(sc) + '/10</span>' if sc else ''}"
                f"</div>",
                unsafe_allow_html=True,
            )

        st.markdown("</div>", unsafe_allow_html=True)

        # Agent roster (always visible)
        st.markdown(
            "<div style='background:#1E2130;border-radius:8px;padding:12px 14px;margin-top:10px;'>"
            "<div style='font-size:10px;font-weight:700;color:#64748B;"
            "text-transform:uppercase;letter-spacing:.7px;margin-bottom:8px;'>"
            "Agent Roster</div>",
            unsafe_allow_html=True,
        )
        for key, (icon, name, desc) in AGENT_ICONS.items():
            st.markdown(
                f"<div style='font-size:11px;color:#94A3B8;padding:2px 0;'>"
                f"{icon} <span style='color:#CBD5E1;'>{name}</span></div>",
                unsafe_allow_html=True,
            )
        st.markdown("</div>", unsafe_allow_html=True)

    # ── CHAT AREA (left) ──────────────────────────────────────
    with chat_col:
        st.markdown(
            "<div class='chat-header'>Ask your title performance team</div>"
            "<div class='chat-subhead'>"
            "Type your question, or pick a topic from the sidebar"
            "</div>",
            unsafe_allow_html=True,
        )

        question = st.text_input(
            label="question",
            label_visibility="collapsed",
            value=st.session_state.ai_prefill,
            placeholder="Type your question here, or pick a topic from the sidebar…",
            key=f"ai_input_{st.session_state.ai_inp_key}",
        )

        btn_col, clr_col = st.columns([2, 1])
        with btn_col:
            ask_btn = st.button(
                "▶  Send to Agent Team",
                use_container_width=True,
                type="primary",
                key="ask_btn",
            )
        with clr_col:
            if st.button("✕  Clear", use_container_width=True, key="clr_btn"):
                st.session_state.ai_history = []
                st.session_state.ai_prefill = ""
                st.session_state.ai_inp_key += 1
                st.rerun()

        # ── Run pipeline ───────────────────────────────────────
        if ask_btn and question.strip():
            st.session_state.ai_prefill = ""
            st.session_state.ai_inp_key += 1
            with st.spinner("Routing to agents — 30–75 seconds…"):
                result = agent.run_query(question.strip())
            st.session_state.ai_history.insert(0, result)
            if len(st.session_state.ai_history) > 8:
                st.session_state.ai_history = st.session_state.ai_history[:8]
            st.rerun()

        # ── Results history ────────────────────────────────────
        if not st.session_state.ai_history:
            st.markdown(
                "<div style='background:#fff;border:1px dashed #CBD5E1;border-radius:10px;"
                "padding:40px 24px;text-align:center;margin-top:20px;'>"
                "<div style='font-size:32px;margin-bottom:10px;'>🤖</div>"
                "<div style='font-size:15px;font-weight:700;color:#0F172A;margin-bottom:6px;'>"
                "No question yet</div>"
                "<div style='font-size:13px;color:#64748B;'>"
                "Pick a quick query from the sidebar or type your own question above.<br>"
                "The agent pipeline will classify, query, analyse, and deliver insights."
                "</div>"
                "</div>",
                unsafe_allow_html=True,
            )
        else:
            for idx, r in enumerate(st.session_state.ai_history):
                cat_color = r.get("cat_color", "#64748b")
                cat_icon  = r.get("cat_icon",  "❓")
                cat_lbl   = r.get("cat_label", "")
                cat_key   = r.get("category",  "?")
                score     = r.get("score")
                vbadge    = r.get("verdict_badge", "")
                vlabel    = r.get("verdict_label", "")
                elapsed   = r.get("elapsed_s", 0)
                sc_str    = f"{score}/10" if score is not None else "—"
                sc_col    = "#16a34a" if (score or 0) >= 8 else "#d97706" if (score or 0) >= 6 else "#dc2626"

                card_cls  = "result-card-new" if idx == 0 else "result-card"

                st.markdown(f"<div class='{card_cls}'>", unsafe_allow_html=True)

                # Question row
                st.markdown(
                    f"<div style='display:flex;align-items:center;gap:8px;"
                    f"margin-bottom:10px;flex-wrap:wrap;'>"
                    f"<span style='background:{cat_color};color:#fff;border-radius:4px;"
                    f"padding:2px 10px;font-size:11px;font-weight:700;'>"
                    f"{cat_icon} {cat_lbl}</span>"
                    f"<span style='font-size:11px;color:{sc_col};font-weight:700;'>"
                    f"{vbadge} Quality {sc_str} · {vlabel}</span>"
                    f"<span style='font-size:11px;color:#94a3b8;margin-left:auto;'>"
                    f"⏱ {elapsed}s</span>"
                    f"</div>"
                    f"<div style='font-size:13px;color:#0F172A;font-weight:600;"
                    f"margin-bottom:12px;'>Q: {r.get('question','')}</div>",
                    unsafe_allow_html=True,
                )

                if r.get("error"):
                    st.error(f"Pipeline error: {r['error']}")
                else:
                    st.markdown(r["response"])

                st.markdown("</div>", unsafe_allow_html=True)


# ─────────────────────────────────────────────────────────────
# SHARED FILTER HELPER for data tabs
# ─────────────────────────────────────────────────────────────
def data_tab_filters(tab_key: str):
    """Render a compact filter bar, return SQL fragments."""
    all_genres = run("SELECT DISTINCT genre FROM titles ORDER BY genre")["genre"].tolist()
    all_markets = ["AU", "SG", "HK", "IN", "JP", "KR", "TW", "TH", "PH", "MY"]

    f1, f2, f3, f4 = st.columns(4)
    with f1:
        st.markdown("<div class='filter-label'>Time Period</div>", unsafe_allow_html=True)
        period = st.selectbox("Period", ["Last 7 days", "Last 30 days", "Last 90 days (all)"],
                              index=1, label_visibility="collapsed", key=f"period_{tab_key}")
    with f2:
        st.markdown("<div class='filter-label'>Market</div>", unsafe_allow_html=True)
        sel_market = st.selectbox("Market", ["All APAC Markets"] + all_markets,
                                  index=0, label_visibility="collapsed", key=f"mkt_{tab_key}")
    with f3:
        st.markdown("<div class='filter-label'>Genre</div>", unsafe_allow_html=True)
        sel_genre = st.selectbox("Genre", ["All Genres"] + all_genres,
                                 index=0, label_visibility="collapsed", key=f"gen_{tab_key}")
    with f4:
        st.markdown("<div class='filter-label'>Format</div>", unsafe_allow_html=True)
        format_filter = st.selectbox("Format", ["All Formats", "Series only", "Movies only"],
                                     index=0, label_visibility="collapsed", key=f"fmt_{tab_key}")

    period_map = {
        "Last 7 days":        "date >= '2026-03-25'",
        "Last 30 days":       "date >= '2026-03-01'",
        "Last 90 days (all)": "date >= '2026-01-01'",
    }
    date_filter = period_map[period]
    mkt_sql  = f"AND v.market = '{sel_market}'" if sel_market != "All APAC Markets" else ""
    gen_sql  = f"AND t.genre = '{sel_genre}'"   if sel_genre  != "All Genres" else ""
    fmt_map  = {"All Formats": "", "Series only": "AND t.format='Series'", "Movies only": "AND t.format='Movie'"}
    fmt_sql  = fmt_map[format_filter]
    return date_filter, mkt_sql, gen_sql, fmt_sql, sel_market, sel_genre, period


# ══════════════════════════════════════════════════════════════
# TAB 1 — OVERVIEW
# ══════════════════════════════════════════════════════════════
with tab1:
    date_filter, mkt_sql, gen_sql, fmt_sql, sel_market, sel_genre, period = data_tab_filters("ov")
    st.markdown("<div style='margin-top:6px'></div>", unsafe_allow_html=True)

    kpi_df = run(f"""
        SELECT
            SUM(v.starts)                           AS total_starts,
            SUM(v.completions)                      AS total_completions,
            ROUND(AVG(v.completion_rate)*100, 1)    AS avg_completion_pct,
            COUNT(DISTINCT v.title_id)              AS titles_tracked,
            SUM(v.watch_time_mins)/60               AS total_hours,
            COUNT(DISTINCT v.market)                AS markets
        FROM viewership_daily v
        JOIN titles t ON t.title_id = v.title_id
        WHERE {date_filter} {mkt_sql} {gen_sql} {fmt_sql}
    """)
    at_risk_df = run(f"""
        SELECT COUNT(DISTINCT title_id) AS at_risk_count
        FROM (
            SELECT v.title_id, AVG(v.completion_rate) AS avg_cr
            FROM viewership_daily v
            JOIN titles t ON t.title_id = v.title_id
            WHERE {date_filter} {mkt_sql} {gen_sql} {fmt_sql}
            GROUP BY v.title_id HAVING avg_cr < 0.45
        )
    """)
    k = kpi_df.iloc[0]
    at_risk = int(at_risk_df.iloc[0]["at_risk_count"])

    c1, c2, c3, c4, c5 = st.columns(5)
    with c1:
        st.markdown(f"""<div class="kpi">
            <div class="kpi-label">Total Starts</div>
            <div class="kpi-value">{fmt_k(int(k['total_starts']))}</div>
            <div class="kpi-sub">{period.lower()}</div>
        </div>""", unsafe_allow_html=True)
    with c2:
        st.markdown(f"""<div class="kpi">
            <div class="kpi-label">Avg Completion Rate</div>
            <div class="kpi-value">{k['avg_completion_pct']}%</div>
            <div class="{'kpi-up' if k['avg_completion_pct'] >= 60 else 'kpi-warn'}">
                {'↑ Above benchmark' if k['avg_completion_pct'] >= 60 else '↓ Below benchmark'}
            </div>
        </div>""", unsafe_allow_html=True)
    with c3:
        st.markdown(f"""<div class="kpi">
            <div class="kpi-label">Watch Hours</div>
            <div class="kpi-value">{fmt_k(int(k['total_hours']))}</div>
            <div class="kpi-sub">across {int(k['markets'])} markets</div>
        </div>""", unsafe_allow_html=True)
    with c4:
        st.markdown(f"""<div class="kpi">
            <div class="kpi-label">Titles Tracked</div>
            <div class="kpi-value">{int(k['titles_tracked'])}</div>
            <div class="kpi-sub">active in period</div>
        </div>""", unsafe_allow_html=True)
    with c5:
        st.markdown(f"""<div class="kpi">
            <div class="kpi-label">Titles At Risk</div>
            <div class="kpi-value" style="color:#DC2626">{at_risk}</div>
            <div class="kpi-down">completion &lt; 45%</div>
        </div>""", unsafe_allow_html=True)

    st.markdown("<div style='margin-top:16px'></div>", unsafe_allow_html=True)
    col_l, col_r = st.columns([3, 2])

    with col_l:
        st.markdown('<div class="sec">Top 15 Titles by Total Starts</div>', unsafe_allow_html=True)
        top_titles = run(f"""
            SELECT t.title_name, t.genre, t.format,
                   SUM(v.starts)                        AS total_starts,
                   ROUND(AVG(v.completion_rate)*100, 1) AS avg_completion_pct
            FROM viewership_daily v
            JOIN titles t ON t.title_id = v.title_id
            WHERE {date_filter} {mkt_sql} {gen_sql} {fmt_sql}
            GROUP BY t.title_name, t.genre, t.format
            ORDER BY total_starts DESC LIMIT 15
        """)
        colors = [
            COLORS["green"] if r >= 65 else COLORS["amber"] if r >= 45 else COLORS["red"]
            for r in top_titles["avg_completion_pct"]
        ]
        fig = go.Figure(go.Bar(
            y=top_titles["title_name"], x=top_titles["total_starts"], orientation="h",
            marker_color=colors,
            text=[f"{r}% completion" for r in top_titles["avg_completion_pct"]],
            textposition="inside", textfont=dict(size=10, color="white"),
            hovertemplate="<b>%{y}</b><br>Starts: %{x:,}<extra></extra>",
        ))
        fig.update_layout(
            height=420, margin=dict(l=200, r=10, t=10, b=0),
            paper_bgcolor=CHART_BG, plot_bgcolor=CHART_BG,
            xaxis=dict(gridcolor=GRID_COLOR, showgrid=True, tickfont=dict(size=11)),
            yaxis=dict(autorange="reversed", tickfont=dict(size=11, color="#0f172a")),
            showlegend=False,
        )
        st.plotly_chart(fig, use_container_width=True)

    with col_r:
        st.markdown('<div class="sec">Completion Rate Distribution</div>', unsafe_allow_html=True)
        cr_dist = run(f"""
            SELECT
                CASE
                    WHEN AVG(v.completion_rate) >= 0.70 THEN '70%+ Champion'
                    WHEN AVG(v.completion_rate) >= 0.55 THEN '55–70% Solid'
                    WHEN AVG(v.completion_rate) >= 0.45 THEN '45–55% Watch'
                    ELSE 'Under 45% At-Risk'
                END AS band,
                COUNT(DISTINCT v.title_id) AS n_titles
            FROM viewership_daily v
            JOIN titles t ON t.title_id = v.title_id
            WHERE {date_filter} {mkt_sql} {gen_sql} {fmt_sql}
            GROUP BY v.title_id
        """)
        cr_agg = cr_dist.groupby("band")["n_titles"].sum().reset_index()
        band_order = ['70%+ Champion', '55–70% Solid', '45–55% Watch', 'Under 45% At-Risk']
        cr_agg["band"] = pd.Categorical(cr_agg["band"], categories=band_order, ordered=True)
        cr_agg = cr_agg.sort_values("band")
        fig2 = go.Figure(go.Pie(
            labels=cr_agg["band"], values=cr_agg["n_titles"], hole=0.55,
            marker_colors=[COLORS["green"], COLORS["sky"], COLORS["amber"], COLORS["red"]],
            textinfo="label+value", textfont=dict(size=11),
            hovertemplate="%{label}<br>%{value} titles<extra></extra>",
        ))
        fig2.update_layout(
            height=260, margin=dict(l=0, r=0, t=10, b=0),
            paper_bgcolor=CHART_BG, showlegend=False,
        )
        st.plotly_chart(fig2, use_container_width=True)

        st.markdown('<div class="sec">Daily Starts Trend</div>', unsafe_allow_html=True)
        trend = run(f"""
            SELECT date, SUM(starts) AS total_starts
            FROM viewership_daily v
            JOIN titles t ON t.title_id = v.title_id
            WHERE {date_filter} {mkt_sql} {gen_sql} {fmt_sql}
            GROUP BY date ORDER BY date
        """)
        fig3 = go.Figure()
        fig3.add_trace(go.Scatter(
            x=trend["date"], y=trend["total_starts"], mode="lines", fill="tozeroy",
            line=dict(color=COLORS["indigo"], width=2),
            fillcolor="rgba(79,70,229,0.08)",
            hovertemplate="%{x}<br>Starts: %{y:,}<extra></extra>",
        ))
        fig3.update_layout(
            height=150, margin=dict(l=0, r=0, t=5, b=0),
            paper_bgcolor=CHART_BG, plot_bgcolor=CHART_BG,
            xaxis=dict(showgrid=False, tickfont=dict(size=9)),
            yaxis=dict(gridcolor=GRID_COLOR, tickfont=dict(size=9)),
            showlegend=False,
        )
        st.plotly_chart(fig3, use_container_width=True)


# ══════════════════════════════════════════════════════════════
# TAB 2 — MARKETS
# ══════════════════════════════════════════════════════════════
with tab2:
    date_filter, mkt_sql, gen_sql, fmt_sql, sel_market, sel_genre, period = data_tab_filters("mk")
    st.markdown("<div style='margin-top:6px'></div>", unsafe_allow_html=True)

    col_l, col_r = st.columns(2)
    with col_l:
        st.markdown('<div class="sec">Total Starts by Market</div>', unsafe_allow_html=True)
        mkt_perf = run(f"""
            SELECT v.market,
                   SUM(v.starts)                        AS total_starts,
                   ROUND(AVG(v.completion_rate)*100, 1) AS avg_cr,
                   COUNT(DISTINCT v.title_id)           AS titles_active
            FROM viewership_daily v
            JOIN titles t ON t.title_id = v.title_id
            WHERE {date_filter} {mkt_sql} {gen_sql} {fmt_sql}
            GROUP BY v.market ORDER BY total_starts DESC
        """)
        fig = go.Figure(go.Bar(
            x=mkt_perf["market"], y=mkt_perf["total_starts"],
            marker_color=COLORS["indigo"],
            text=[f"{c}%" for c in mkt_perf["avg_cr"]], textposition="outside",
            textfont=dict(size=10, color="#0f172a"),
            hovertemplate="<b>%{x}</b><br>Starts: %{y:,}<extra></extra>",
        ))
        fig.update_layout(
            height=320, margin=dict(l=0, r=0, t=30, b=0),
            paper_bgcolor=CHART_BG, plot_bgcolor=CHART_BG,
            yaxis=dict(gridcolor=GRID_COLOR), showlegend=False,
        )
        st.plotly_chart(fig, use_container_width=True)

    with col_r:
        st.markdown('<div class="sec">Completion Rate by Market</div>', unsafe_allow_html=True)
        cr_mkt = run(f"""
            SELECT v.market, ROUND(AVG(v.completion_rate)*100, 1) AS avg_cr
            FROM viewership_daily v
            JOIN titles t ON t.title_id = v.title_id
            WHERE {date_filter} {mkt_sql} {gen_sql} {fmt_sql}
            GROUP BY v.market ORDER BY avg_cr DESC
        """)
        bar_colors = [
            COLORS["green"] if r >= 65 else COLORS["amber"] if r >= 50 else COLORS["red"]
            for r in cr_mkt["avg_cr"]
        ]
        fig2 = go.Figure(go.Bar(
            x=cr_mkt["market"], y=cr_mkt["avg_cr"],
            marker_color=bar_colors,
            text=[f"{r}%" for r in cr_mkt["avg_cr"]], textposition="outside",
            textfont=dict(size=10, color="#0f172a"),
        ))
        fig2.update_layout(
            height=320, margin=dict(l=0, r=0, t=30, b=0),
            paper_bgcolor=CHART_BG, plot_bgcolor=CHART_BG,
            yaxis=dict(gridcolor=GRID_COLOR, range=[0, 100]), showlegend=False,
        )
        st.plotly_chart(fig2, use_container_width=True)

    st.markdown('<div class="sec">Top 10 Titles per Market</div>', unsafe_allow_html=True)
    all_markets_list = ["AU", "SG", "HK", "IN", "JP", "KR", "TW", "TH", "PH", "MY"]
    default_mkt = sel_market if sel_market != "All APAC Markets" else "AU"
    sel_mkt = st.selectbox("Select market to drill into", all_markets_list,
                           index=all_markets_list.index(default_mkt) if default_mkt in all_markets_list else 0,
                           key="mkt_drill")
    top_by_mkt = run(f"""
        SELECT t.title_name, t.genre,
               SUM(v.starts)                        AS starts,
               ROUND(AVG(v.completion_rate)*100, 1) AS completion_pct
        FROM viewership_daily v
        JOIN titles t ON t.title_id = v.title_id
        WHERE {date_filter} AND v.market='{sel_mkt}' {gen_sql} {fmt_sql}
        GROUP BY t.title_name, t.genre
        ORDER BY starts DESC LIMIT 10
    """)
    st.dataframe(
        top_by_mkt.rename(columns={
            "title_name": "Title", "genre": "Genre",
            "starts": "Starts", "completion_pct": "Completion %",
        }),
        use_container_width=True, hide_index=True,
    )


# ══════════════════════════════════════════════════════════════
# TAB 3 — GENRE HEALTH
# ══════════════════════════════════════════════════════════════
with tab3:
    date_filter, mkt_sql, gen_sql, fmt_sql, sel_market, sel_genre, period = data_tab_filters("gh")
    st.markdown("<div style='margin-top:6px'></div>", unsafe_allow_html=True)

    col_l, col_r = st.columns(2)
    with col_l:
        st.markdown('<div class="sec">Starts vs Completion Rate by Genre</div>', unsafe_allow_html=True)
        genre_health = run(f"""
            SELECT t.genre,
                   SUM(v.starts)                        AS total_starts,
                   ROUND(AVG(v.completion_rate)*100, 1) AS avg_cr,
                   COUNT(DISTINCT v.title_id)           AS n_titles
            FROM viewership_daily v
            JOIN titles t ON t.title_id = v.title_id
            WHERE {date_filter} {mkt_sql} {gen_sql} {fmt_sql}
            GROUP BY t.genre ORDER BY total_starts DESC
        """)
        fig = px.scatter(
            genre_health, x="total_starts", y="avg_cr",
            size="n_titles", text="genre", color="avg_cr",
            color_continuous_scale=["#DC2626", "#D97706", "#16A34A"], size_max=40,
        )
        fig.update_traces(textposition="top center", textfont=dict(size=10))
        fig.add_hline(y=60, line_dash="dot", line_color=COLORS["slate"],
                      annotation_text="60% benchmark", annotation_font_size=10)
        fig.update_layout(
            height=380, margin=dict(l=0, r=0, t=10, b=0),
            paper_bgcolor=CHART_BG, plot_bgcolor=CHART_BG,
            coloraxis_showscale=False,
            xaxis=dict(title="Total Starts", gridcolor=GRID_COLOR),
            yaxis=dict(title="Avg Completion %", gridcolor=GRID_COLOR),
            showlegend=False,
        )
        st.plotly_chart(fig, use_container_width=True)

    with col_r:
        st.markdown('<div class="sec">Originals vs Licensed Performance</div>', unsafe_allow_html=True)
        orig_perf = run(f"""
            SELECT
                CASE WHEN t.is_hbo_original=1 THEN 'Original' ELSE 'Licensed' END AS content_type,
                SUM(v.starts)                        AS total_starts,
                ROUND(AVG(v.completion_rate)*100, 1) AS avg_cr,
                COUNT(DISTINCT v.title_id)           AS n_titles
            FROM viewership_daily v
            JOIN titles t ON t.title_id = v.title_id
            WHERE {date_filter} {mkt_sql} {gen_sql} {fmt_sql}
            GROUP BY content_type
        """)
        fig2 = go.Figure()
        colors_orig = [COLORS["indigo"], COLORS["sky"]]
        for i, row in orig_perf.iterrows():
            fig2.add_trace(go.Bar(
                name=row["content_type"],
                x=["Total Starts", "Completion %", "Titles"],
                y=[row["total_starts"], row["avg_cr"], row["n_titles"]],
                marker_color=colors_orig[i % 2],
            ))
        fig2.update_layout(
            height=220, barmode="group", margin=dict(l=0, r=0, t=10, b=0),
            paper_bgcolor=CHART_BG, plot_bgcolor=CHART_BG,
            yaxis=dict(gridcolor=GRID_COLOR), legend=dict(orientation="h", y=1.1),
        )
        st.plotly_chart(fig2, use_container_width=True)

        st.markdown('<div class="sec">Language Performance</div>', unsafe_allow_html=True)
        lang_perf = run(f"""
            SELECT t.language,
                   SUM(v.starts)                        AS total_starts,
                   ROUND(AVG(v.completion_rate)*100, 1) AS avg_cr,
                   COUNT(DISTINCT v.title_id)           AS n_titles
            FROM viewership_daily v
            JOIN titles t ON t.title_id = v.title_id
            WHERE {date_filter} {mkt_sql} {gen_sql} {fmt_sql}
            GROUP BY t.language ORDER BY total_starts DESC
        """)
        st.dataframe(
            lang_perf.rename(columns={
                "language": "Language", "total_starts": "Starts",
                "avg_cr": "Completion %", "n_titles": "Titles",
            }),
            use_container_width=True, hide_index=True,
        )


# ══════════════════════════════════════════════════════════════
# TAB 4 — EPISODE DROP-OFF
# ══════════════════════════════════════════════════════════════
with tab4:
    st.markdown('<div class="sec">Episode-by-Episode Drop-off</div>', unsafe_allow_html=True)

    ep_titles = run("""
        SELECT DISTINCT t.title_name
        FROM viewership_episode ve
        JOIN titles t ON t.title_id = ve.title_id
        ORDER BY t.title_name
    """)["title_name"].tolist()

    sel_title = st.selectbox("Select title", ep_titles, index=0, key="ep_title")
    ep_data = run(f"""
        SELECT ve.episode_number,
               SUM(ve.starts)                           AS total_starts,
               SUM(ve.completions)                      AS total_completions,
               ROUND(AVG(ve.drop_off_point_pct)*100, 1) AS avg_drop_off_pct,
               ROUND(AVG(ve.avg_watch_time_mins), 1)    AS avg_watch_mins
        FROM viewership_episode ve
        JOIN titles t ON t.title_id = ve.title_id
        WHERE t.title_name = '{sel_title}'
        GROUP BY ve.episode_number ORDER BY ve.episode_number
    """)

    col_l, col_r = st.columns(2)
    with col_l:
        st.markdown('<div class="sec">Starts per Episode</div>', unsafe_allow_html=True)
        fig = go.Figure(go.Bar(
            x=[f"Ep {e}" for e in ep_data["episode_number"]],
            y=ep_data["total_starts"],
            marker_color=[
                COLORS["red"] if v < ep_data["total_starts"].max() * 0.5
                else COLORS["amber"] if v < ep_data["total_starts"].max() * 0.75
                else COLORS["green"]
                for v in ep_data["total_starts"]
            ],
            text=[fmt_k(int(v)) for v in ep_data["total_starts"]],
            textposition="outside", textfont=dict(size=10, color="#0f172a"),
            hovertemplate="<b>%{x}</b><br>Starts: %{y:,}<extra></extra>",
        ))
        fig.update_layout(
            height=300, margin=dict(l=0, r=0, t=30, b=0),
            paper_bgcolor=CHART_BG, plot_bgcolor=CHART_BG,
            yaxis=dict(gridcolor=GRID_COLOR),
        )
        st.plotly_chart(fig, use_container_width=True)

    with col_r:
        st.markdown('<div class="sec">Avg Drop-off Point (% through episode)</div>', unsafe_allow_html=True)
        fig2 = go.Figure(go.Scatter(
            x=[f"Ep {e}" for e in ep_data["episode_number"]],
            y=ep_data["avg_drop_off_pct"],
            mode="lines+markers",
            line=dict(color=COLORS["indigo"], width=2),
            marker=dict(size=8, color=COLORS["indigo"]),
            fill="tozeroy", fillcolor="rgba(79,70,229,0.07)",
            hovertemplate="<b>%{x}</b><br>Viewers drop at %{y}% through<extra></extra>",
        ))
        fig2.add_hline(y=50, line_dash="dot", line_color=COLORS["amber"],
                       annotation_text="50% midpoint", annotation_font_size=10)
        fig2.update_layout(
            height=300, margin=dict(l=0, r=0, t=5, b=0),
            paper_bgcolor=CHART_BG, plot_bgcolor=CHART_BG,
            yaxis=dict(gridcolor=GRID_COLOR, range=[0, 105], title="% through episode at drop-off"),
        )
        st.plotly_chart(fig2, use_container_width=True)


# ══════════════════════════════════════════════════════════════
# TAB 5 — ALERTS
# ══════════════════════════════════════════════════════════════
with tab5:
    date_filter, mkt_sql, gen_sql, fmt_sql, sel_market, sel_genre, period = data_tab_filters("al")
    st.markdown("<div style='margin-top:6px'></div>", unsafe_allow_html=True)

    st.markdown('<div class="sec">🚨 Titles Needing Immediate Attention</div>', unsafe_allow_html=True)
    alerts = run(f"""
        SELECT t.title_name, t.genre, t.format,
               ROUND(AVG(v.completion_rate)*100, 1)  AS avg_cr,
               SUM(v.starts)                          AS total_starts,
               COUNT(DISTINCT v.market)               AS markets_active
        FROM viewership_daily v
        JOIN titles t ON t.title_id = v.title_id
        WHERE {date_filter} {mkt_sql} {gen_sql} {fmt_sql}
        GROUP BY t.title_name, t.genre, t.format
        HAVING avg_cr < 45
        ORDER BY avg_cr ASC
    """)

    if len(alerts) == 0:
        st.success("No titles below the 45% completion threshold in selected filters.")
    else:
        for _, row in alerts.iterrows():
            sev = "🔴 Critical" if row["avg_cr"] < 35 else "🟡 Watch"
            col_a, col_b, col_c, col_d = st.columns([3, 1, 1, 1])
            with col_a:
                st.markdown(f"**{row['title_name']}** · {row['genre']} · {row['format']}")
            with col_b:
                color = "pill-red" if row["avg_cr"] < 35 else "pill-amber"
                st.markdown(f'<span class="{color}">{row["avg_cr"]}% completion</span>',
                            unsafe_allow_html=True)
            with col_c:
                st.markdown(f'<span class="pill-amber">{fmt_k(int(row["total_starts"]))} starts</span>',
                            unsafe_allow_html=True)
            with col_d:
                st.markdown(f"<span style='font-size:12px;color:#64748B'>{sev}</span>",
                            unsafe_allow_html=True)
        st.markdown("---")

    col_l, col_r = st.columns(2)
    with col_l:
        st.markdown('<div class="sec">Subscriber Churn Risk by Segment</div>', unsafe_allow_html=True)
        churn = run(f"""
            SELECT sv.subscriber_segment,
                   COUNT(DISTINCT sv.subscriber_id)                                         AS total_subs,
                   COUNT(DISTINCT CASE WHEN sv.is_churned=1 THEN sv.subscriber_id END)      AS churned,
                   ROUND(COUNT(DISTINCT CASE WHEN sv.is_churned=1 THEN sv.subscriber_id END)
                         * 100.0 / NULLIF(COUNT(DISTINCT sv.subscriber_id),0), 1)           AS churn_pct
            FROM subscriber_viewing sv
            JOIN titles t ON t.title_id = sv.title_id
            WHERE 1=1 {gen_sql.replace('v.', 'sv.')} {fmt_sql}
            GROUP BY sv.subscriber_segment ORDER BY churn_pct DESC
        """)
        bar_col = [
            COLORS["red"] if r >= 25 else COLORS["amber"] if r >= 10 else COLORS["green"]
            for r in churn["churn_pct"]
        ]
        fig = go.Figure(go.Bar(
            x=churn["subscriber_segment"], y=churn["churn_pct"],
            marker_color=bar_col,
            text=[f"{r}%" for r in churn["churn_pct"]], textposition="outside",
            textfont=dict(size=10, color="#0f172a"),
        ))
        fig.update_layout(
            height=280, margin=dict(l=0, r=0, t=30, b=0),
            paper_bgcolor=CHART_BG, plot_bgcolor=CHART_BG,
            yaxis=dict(gridcolor=GRID_COLOR, title="Churn %"),
        )
        st.plotly_chart(fig, use_container_width=True)

    with col_r:
        st.markdown('<div class="sec">WoW Starts Trend — At-Risk Titles</div>', unsafe_allow_html=True)
        wow = run(f"""
            WITH curr AS (
                SELECT v.title_id, SUM(v.starts) AS curr_starts
                FROM viewership_daily v
                JOIN titles t ON t.title_id = v.title_id
                WHERE v.date >= '2026-03-25' {mkt_sql} {gen_sql} {fmt_sql}
                GROUP BY v.title_id
            ),
            prev AS (
                SELECT v.title_id, SUM(v.starts) AS prev_starts
                FROM viewership_daily v
                JOIN titles t ON t.title_id = v.title_id
                WHERE v.date >= '2026-03-18' AND v.date < '2026-03-25' {mkt_sql} {gen_sql} {fmt_sql}
                GROUP BY v.title_id
            )
            SELECT t.title_name,
                   ROUND((curr.curr_starts - prev.prev_starts)*100.0
                         / NULLIF(prev.prev_starts, 0), 1) AS wow_pct
            FROM curr
            JOIN prev ON curr.title_id = prev.title_id
            JOIN titles t ON t.title_id = curr.title_id
            WHERE wow_pct < -10
            ORDER BY wow_pct ASC LIMIT 10
        """)
        if len(wow) == 0:
            st.info("No titles with >10% WoW decline in selected filters.")
        else:
            fig2 = go.Figure(go.Bar(
                x=wow["wow_pct"], y=wow["title_name"], orientation="h",
                marker_color=COLORS["red"],
                text=[f"{v}%" for v in wow["wow_pct"]], textposition="outside",
                textfont=dict(size=10, color="#dc2626"),
            ))
            fig2.update_layout(
                height=280, margin=dict(l=200, r=60, t=5, b=0),
                paper_bgcolor=CHART_BG, plot_bgcolor=CHART_BG,
                xaxis=dict(gridcolor=GRID_COLOR, title="WoW % Change"),
                yaxis=dict(autorange="reversed", tickfont=dict(size=11, color="#0f172a")),
            )
            st.plotly_chart(fig2, use_container_width=True)
