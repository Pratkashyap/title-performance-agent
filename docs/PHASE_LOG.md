# Phase Build Log
## WBD APAC — Title Performance Analyst

| Phase | What | Status | Date |
|---|---|---|---|
| 0 | Schema design + DuckDB data generation | ✅ Complete | Apr 2026 |
| 1 | Data Agent + sql_tool (DuckDB query layer) | ✅ Complete | Apr 2026 |
| 2 | Orchestrator + Performance Analyst (Cat A + B) | ✅ Complete | Apr 2026 |
| 3 | Benchmark Agent + Trend Agent (Cat B + C) | — | — |
| 4 | Genre & Catalog Agent (Cat D) | — | — |
| 5 | Subscriber Behaviour Agent (Cat E) | — | — |
| 6 | Alert Agent (Cat F) | — | — |
| 7 | Quality Critic integration | — | — |
| 8 | Dashboard Agent (Streamlit) | — | — |
| 9 | End-to-end testing (all 5 test cases) | — | — |

---

## Phase 2 — Notes
- `agents/orchestrator.py` — Classifies A/B/C/D/E/F via Haiku; routes Cat A/B to PerformanceAnalyst, C/D/E/F data-only (Phase 3+ placeholder)
- `agents/performance_analyst.py` — Sonnet 4.6 analysis engine; Cat A = root cause diagnosis, Cat B = performance snapshot vs benchmarks
- `main.py` — Rich terminal UI with live pipeline display, demo mode, sample queries browser
- `tools/sql_tool.py` — Added GROUP BY rule to schema prompt; fixes Cat F alert scan queries
- Cat A makes 2 data fetches (primary + benchmark supplement); Cat B makes 1 fetch
- 6/6 question categories passing end-to-end; `python3 main.py` is fully operational

## Phase 1 — Notes
- `tools/sql_tool.py` — Natural language → SQL via Haiku; schema context for all 5 tables; auto-retry on error
- `agents/data_agent.py` — Classifies A/B/C/D/E/F + query type; fetch() + fetch_multiple() for orchestrator use
- 8/8 self-tests passing across all 6 question categories

## Phase 0 — Notes
- DuckDB generated: 59 titles, 50,981 viewership_daily, 3,370 episode, 37,095 subscriber_viewing rows
- 10 APAC markets: AU, SG, HK, IN, JP, KR, TW, TH, PH, MY
- 4 languages: EN, KO, JA, IT
- New columns added vs original spec: `language`, `is_hbo_original`, `is_churned`, `subscription_start_date`
- Extension zone documented in generate_data.py for future column additions
