# CLAUDE.md — WBD APAC Title Performance Analyst
## Auto-loaded by Claude Code at the start of every session.
## Keep this file updated after every phase.

---

## WHAT THIS PROJECT IS

A multi-agent AI system that answers any natural language question about how a
title is performing on Max APAC — diagnosis, benchmarking, trends, genre
analysis, subscriber behaviour, and proactive alerts.

Replaces: Screenshot → CSV → deck → meeting (3-5 days)
With: Natural language question → structured answer (< 15 seconds)

**Company:** Warner Bros. Discovery (WBD)
**Region:** APAC (10 markets: AU, SG, HK, IN, JP, KR, TW, TH, PH, MY)
**Platform:** Max / HBO Max Streaming
**Owner:** Prateek Kashyap — Data Analytics & Science Team

---

## CURRENT STATUS

| Phase | What | Status |
|---|---|---|
| 0 | Schema + DuckDB data generation | ✅ Complete |
| 1 | Data Agent + sql_tool | ✅ Complete |
| 2 | Orchestrator + Performance Analyst (Cat A+B) | ✅ Complete |
| 3 | Benchmark Agent + Trend Agent (Cat B+C) | ✅ Complete |
| 4 | Genre & Catalog Agent (Cat D) | ⬜ Next |
| 5 | Subscriber Behaviour Agent (Cat E) | ⬜ |
| 6 | Alert Agent (Cat F) | ⬜ |
| 7 | Quality Critic | ⬜ |
| 8 | Dashboard Agent (Streamlit) | ⬜ |
| 9 | End-to-end testing | ⬜ |

**Build approach:** Phase-by-phase (Option B) — complete and test each phase before moving on.

---

## HOW TO RUN

```bash
cd ~/Desktop/title-performance-agent
source venv/bin/activate
python3 main.py
```

**Python version:** 3.12 (IMPORTANT — do not use system Python 3.7)
**Venv path:** `~/Desktop/title-performance-agent/venv`

To regenerate the database:
```bash
python3 data/generate_data.py
```

---

## TECH STACK

| Layer | Tool | Version |
|---|---|---|
| Language | Python | 3.12 |
| Database | DuckDB | 1.5.0 |
| AI — Deep reasoning | Claude Sonnet 4.6 | claude-sonnet-4-6 |
| AI — Lightweight/fast | Claude Haiku 4.5 | claude-haiku-4-5-20251001 |
| Dashboard | Streamlit | 1.55.0 |
| Charts | Plotly | 6.6.0 |
| Terminal UI | Rich | 14.3.3 |
| AI SDK | Anthropic Python SDK | 0.84.0 |

**No LangChain. No vector DB. Direct Anthropic SDK only.**

---

## ARCHITECTURE — SUPERVISOR PATTERN

```
User Question
  → Orchestrator (Sonnet) — classifies into A/B/C/D/E/F, routes
    → Data Agent (Haiku)            — SQL → DuckDB → result set
    → Performance Analyst (Sonnet)  — Category A + B
    → Benchmark Agent (Haiku)       — Category B comparisons
    → Trend Agent (Haiku)           — Category C
    → Genre & Catalog Agent (Haiku) — Category D
    → Subscriber Agent (Sonnet)     — Category E
    → Alert Agent (Haiku)           — Category F
    → Quality Critic (Haiku)        — scores 0-10, gates output
  → Structured answer delivered
```

**Routing logic:** Orchestrator classifies, routes to 1-N specialists in parallel
where possible, synthesises, passes to Critic before delivery.
**Quality gate:** Score ≥ 8 = pass | 6-7 = enhance | < 6 = rewrite

---

## DATABASE — title_performance.duckdb

**Path:** `data/title_performance.duckdb`
**Note:** Gitignored (too large). Regenerate with `python3 data/generate_data.py`

### Tables & Row Counts
| Table | Rows | Key Columns |
|---|---|---|
| `titles` | 59 | title_id, title_name, genre, format, release_date, seasons, episodes_count, **language**, **is_hbo_original** |
| `viewership_daily` | 50,981 | date, title_id, market, starts, completions, watch_time_mins, completion_rate, unique_viewers, returning_viewers |
| `viewership_episode` | 3,370 | date, title_id, episode_number, starts, completions, avg_watch_time_mins, drop_off_point_pct |
| `subscriber_viewing` | 37,095 | subscriber_id, title_id, date, watch_time_mins, completed, subscriber_segment, plan_type, market, **is_churned**, **subscription_start_date** |
| `title_benchmarks` | 59 | title_id, comparable_title_ids, genre_avg_completion_rate, genre_avg_starts_day7, genre_avg_starts_day30 |

### Key Data Facts
- Date range: 2026-01-01 → 2026-03-31 (90 days)
- 10 APAC markets: AU, SG, HK, IN, JP, KR, TW, TH, PH, MY
- 10 genres: Drama, Fantasy, Sci-Fi, Crime, Thriller, Horror, Comedy, Documentary, K-Drama, Action
- 4 languages: EN, KO, JA, IT
- 48 HBO Originals + 11 Licensed/Theatrical titles
- 258 churned subscribers flagged across 2,200 subscribers

### Performance Tiers (how titles behave in the data)
- **champion:** HotD S2, White Lotus S3, TLOU S1+S2, Dune 2, Barbie, Succession S4
- **underperform:** The Idol, The Nevers, Aquaman, The Flash, True Blood
- **sleeper:** Mare of Easttown, The Undoing, We Own This City, Expats, The Sympathizer
- **at_risk:** Euphoria S2, Industry S2, Curb S12, His Dark Materials
- **new_launch:** Pachinko S2, Tokyo Vice S2, The Jinx Part 2

---

## QUESTION CATEGORIES (A–F)

| Cat | Type | Example |
|---|---|---|
| A | Diagnosis — Why? | "Why is The White Lotus underperforming in SE Asia?" |
| B | Snapshot — How is it doing? | "How is HotD performing vs. comparable fantasy titles?" |
| C | Trends & Patterns | "Is [Title X] gaining or losing momentum WoW?" |
| D | Genre & Catalog | "Which titles have high starts but low completions?" |
| E | Subscriber Behaviour | "Does watching [Title X] reduce churn risk?" |
| F | Alerts & Proactive | "Which titles need immediate attention this week?" |

---

## AGENT ROSTER

| Agent | File | Model | Phase |
|---|---|---|---|
| Orchestrator | agents/orchestrator.py | Sonnet 4.6 | 2 |
| Data Agent | agents/data_agent.py | Haiku 4.5 | 1 |
| Performance Analyst | agents/performance_analyst.py | Sonnet 4.6 | 2 |
| Benchmark Agent | agents/benchmark_agent.py | Haiku 4.5 | 3 |
| Trend Agent | agents/trend_agent.py | Haiku 4.5 | 3 |
| Genre & Catalog Agent | agents/genre_catalog_agent.py | Haiku 4.5 | 4 |
| Subscriber Agent | agents/subscriber_agent.py | Sonnet 4.6 | 5 |
| Alert Agent | agents/alert_agent.py | Haiku 4.5 | 6 |
| Quality Critic | agents/critic_agent.py | Haiku 4.5 | 7 |
| Dashboard Agent | agents/dashboard_agent.py | Sonnet 4.6 | 8 |

---

## REFERENCE PROJECT

The marketing-analytics-agent (`~/Desktop/marketing-analytics-agent`) is the
completed reference implementation. Same architecture, same stack. Read it
before building any new agent — the patterns are directly reusable.

Key files to reference:
- `agents/orchestrator.py` — routing logic pattern
- `agents/data_agent.py` — SQL generation + DuckDB query pattern
- `agents/critic_agent.py` — quality scoring pattern
- `main.py` — Rich terminal UI pattern

---

## GIT & GITHUB

**Repo:** https://github.com/Pratkashyap/title-performance-agent
**Branch:** main
**Commit after:** every completed phase

```bash
git add .
git commit -m "Phase X complete — [description]"
git push
```

---

## BROADER ROADMAP (10 projects)

This is Project 1 of 10 in the WBD APAC AI roadmap.
Full roadmap: see `WBD_APAC_AI_Projects_Roadmap.md` in the planning docs.

After this project: Churn Early Warning System (Project 2)
All projects share this same architecture and DuckDB foundation.

---

## KNOWN GAPS (to fix in V2)

- No time-of-day data (C2 question unanswerable) — viewership_hourly table ready to activate in generate_data.py
- Episode data not split by market — market column ready to activate
- No real Max API connection — simulated data only in V1
- Subscriber data is session-level, not event-level

---
*Last updated: April 2026 | Current phase: Phase 3 complete → Phase 4 next (Genre & Catalog Agent)*
