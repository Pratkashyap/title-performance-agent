# Phase Build Log
## WBD APAC — Title Performance Analyst

| Phase | What | Status | Date |
|---|---|---|---|
| 0 | Schema design + DuckDB data generation | ✅ Complete | Apr 2026 |
| 1 | Data Agent + sql_tool (DuckDB query layer) | ✅ Complete | Apr 2026 |
| 2 | Orchestrator + Performance Analyst (Cat A + B) | ✅ Complete | Apr 2026 |
| 3 | Benchmark Agent + Trend Agent (Cat B + C) | ✅ Complete | Apr 2026 |
| 4 | Genre & Catalog Agent (Cat D) | ✅ Complete | Apr 2026 |
| 5 | Subscriber Behaviour Agent (Cat E) | ✅ Complete | Apr 2026 |
| 6 | Alert Agent (Cat F) | — | — |
| 7 | Quality Critic integration | — | — |
| 8 | Dashboard Agent (Streamlit) | — | — |
| 9 | End-to-end testing (all 5 test cases) | — | — |

---

## Phase 5 — Notes
- `agents/subscriber_agent.py` — Sonnet 4.6; 3 dedicated fetches: segment breakdown (who watches, how much), churn rate by segment + plan type, completion/watch depth by plan type; produces SUBSCRIBER BEHAVIOUR ANALYSIS section
- `agents/orchestrator.py` — Cat E now routes through SubscriberAgent (was data-only stub); Cat F remains data-only (Phase 6)
- Churn signal thresholds: 🟢 <5% | 🟡 5–12% | 🔴 >12% churn rate
- Engagement depth: deep ≥45 mins | moderate 20–44 mins | shallow <20 mins
- Title hint extraction: maps question text to known title names for targeted SQL filters
- 3/3 self-tests passing: churn risk, segment analysis, plan-type retention
- 6/6 question categories passing end-to-end

## Phase 4 — Notes
- `agents/genre_catalog_agent.py` — Haiku; 3 dedicated fetches: genre-level aggregates, engagement gap titles (high starts/low completion), top 10 performers by starts; produces CATALOG & GENRE ANALYSIS section
- `agents/orchestrator.py` — Cat D now routes through GenreCatalogAgent; Cat E/F remain data-only (Phase 5–6)
- `tools/sql_tool.py` — max_tokens increased from 1024 → 2048 in `_ask_claude_for_sql` to prevent SQL truncation on complex alert queries (Cat F regression fix)
- Engagement gap = avg_daily_starts > catalog average AND completion_rate < 50%
- Genre health scoring: 🟢 ≥65% | 🟡 50–64% | 🔴 <50% completion rate
- 6/6 question categories passing end-to-end

## Phase 3 — Notes
- `agents/benchmark_agent.py` — Haiku; fetches genre benchmarks + peer group (same-genre titles last 30d); computes ±% deltas vs genre avg; produces PEER GROUP COMPARISON section
- `agents/trend_agent.py` — Haiku; fetches weekly aggregated viewership + market-level WoW data; produces TREND ANALYSIS with momentum verdict (GROWING/DECLINING/STABLE/RECOVERING/AT RISK)
- `agents/orchestrator.py` — Updated: Cat B now routes through BenchmarkAgent + PerformanceAnalyst (combined response); Cat C routes through TrendAgent; Cat D/E/F remain data-only (Phase 4–6)
- `tools/sql_tool.py` — Added WEEKLY GROUPING rule: use `DATE_TRUNC('week', CAST(date AS DATE))` and repeat full expression in GROUP BY (not alias)
- Cat B response = PerformanceAnalyst snapshot + BenchmarkAgent peer comparison (two sections combined)
- Cat C response = TrendAgent WoW momentum analysis (sole specialist for Cat C)
- 6/6 question categories passing end-to-end after weekly GROUP BY fix

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
