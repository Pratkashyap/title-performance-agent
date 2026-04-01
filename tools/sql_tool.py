"""
tools/sql_tool.py
WBD APAC — Title Performance Analyst

Natural language → SQL → DuckDB results.
Used by the Data Agent. No other agent touches the database directly.

How it works:
  1. Question arrives in plain English
  2. Claude Haiku converts it to SQL (knows the full schema)
  3. SQL runs against DuckDB (read-only)
  4. Clean DataFrame returned
"""

import os
import re
import duckdb
import anthropic
from dotenv import load_dotenv

_env_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "../.env")
load_dotenv(_env_path, override=True)

DB_PATH = os.path.join(os.path.dirname(__file__), "../data/title_performance.duckdb")

# ─────────────────────────────────────────────────────────────
# SCHEMA CONTEXT — Claude needs this to write correct SQL
# ─────────────────────────────────────────────────────────────

SCHEMA_CONTEXT = """
You are a SQL expert for WBD APAC's streaming analytics database (Max / HBO Max).
Today's date is 2026-03-31. All dates are VARCHAR in YYYY-MM-DD format.

═══════════════════════════════════════════════════════
DATABASE SCHEMA
═══════════════════════════════════════════════════════

TABLE: titles  [59 rows — one row per title]
  title_id          INTEGER   primary key
  title_name        VARCHAR   e.g. 'House of the Dragon S2', 'The White Lotus S3'
  genre             VARCHAR   Drama | Fantasy | Sci-Fi | Crime | Thriller | Horror |
                              Comedy | Documentary | K-Drama | Action
  format            VARCHAR   'Series' or 'Movie'
  release_date      VARCHAR   YYYY-MM-DD — global release date
  seasons           INTEGER   number of seasons (0 for movies)
  episodes_count    INTEGER   total episodes (1 for movies)
  language          VARCHAR   'EN' | 'KO' | 'JA' | 'IT'
  is_hbo_original   INTEGER   1 = HBO/Max Original | 0 = Licensed / Theatrical

TABLE: viewership_daily  [50,981 rows — title × market × day]
  date              VARCHAR   YYYY-MM-DD
  title_id          INTEGER
  market            VARCHAR   AU | SG | HK | IN | JP | KR | TW | TH | PH | MY
  starts            INTEGER   number of viewing sessions started
  completions       INTEGER   sessions that reached at least 90% of content
  watch_time_mins   INTEGER   total watch time in minutes
  completion_rate   DOUBLE    completions / starts  (e.g. 0.74 = 74%)
  unique_viewers    INTEGER   distinct viewers that day
  returning_viewers INTEGER   viewers who watched this title before

TABLE: viewership_episode  [3,370 rows — series episodes only]
  date                  VARCHAR   YYYY-MM-DD
  title_id              INTEGER
  episode_number        INTEGER   1-based
  starts                INTEGER
  completions           INTEGER
  avg_watch_time_mins   DOUBLE
  drop_off_point_pct    DOUBLE    fraction of episode watched before dropping off
                                  (0.85 = dropped at 85% through, 0.25 = dropped early)

TABLE: subscriber_viewing  [37,095 rows — individual subscriber sessions]
  subscriber_id         INTEGER
  title_id              INTEGER
  date                  VARCHAR   YYYY-MM-DD
  watch_time_mins       DOUBLE
  completed             INTEGER   1 = watched to completion | 0 = did not complete
  subscriber_segment    VARCHAR   New | Returning | Lapsed-Reactivated | At-Risk | Loyal
  plan_type             VARCHAR   Monthly-Standard | Monthly-Premium |
                                  Annual-Standard | Annual-Premium
  market                VARCHAR   AU | SG | HK | IN | JP | KR | TW | TH | PH | MY
  is_churned            INTEGER   1 = subscriber later churned | 0 = still active
  subscription_start_date VARCHAR YYYY-MM-DD

TABLE: title_benchmarks  [59 rows — one per title]
  title_id                  INTEGER
  comparable_title_ids      VARCHAR   comma-separated list of comparable title_ids
  genre_avg_completion_rate DOUBLE    average completion rate for this title's genre
  genre_avg_starts_day7     INTEGER   genre average total starts in first 7 days
  genre_avg_starts_day30    INTEGER   genre average total starts in first 30 days

═══════════════════════════════════════════════════════
KEY BUSINESS RULES
═══════════════════════════════════════════════════════

DATE WINDOWS (today = 2026-03-31):
  "this week"      → date >= '2026-03-25'
  "last 7 days"    → date >= '2026-03-24'
  "this month"     → date >= '2026-03-01'
  "last 30 days"   → date >= '2026-03-01'
  "last 90 days"   → date >= '2026-01-01'
  "Q1 2026"        → date >= '2026-01-01' AND date <= '2026-03-31'
  "WoW" / week-over-week → compare current week vs prior week using date ranges

MARKETS:
  Southeast Asia (SEA) = SG + TH + PH + MY
  Northeast Asia (NEA) = JP + KR + TW + HK
  All APAC             = all 10 markets

PERFORMANCE BENCHMARKS:
  High completion rate  → completion_rate >= 0.65
  Low completion rate   → completion_rate < 0.45
  Engagement gap        → starts high (top 25%) but completion_rate < 0.50
  Underperforming       → starts < genre_avg_starts_day7 OR completion_rate < genre_avg_completion_rate * 0.80
  At-risk (momentum)    → WoW starts decline > 15%

SUBSCRIBER SEGMENTS:
  New                 — recently subscribed, exploring
  Returning           — re-engaged after a gap
  Lapsed-Reactivated  — came back after cancelling
  At-Risk             — low engagement, likely to churn
  Loyal               — consistently high engagement

═══════════════════════════════════════════════════════
SQL RULES — ALWAYS FOLLOW THESE
═══════════════════════════════════════════════════════
- date is VARCHAR — use string comparison: date >= '2026-03-01'  (works for YYYY-MM-DD)
- Never use CURRENT_DATE — use literal dates based on today = 2026-03-31
- Write ONE simple SELECT query only — no multiple statements
- CTEs (WITH ...) are allowed but keep them simple
- ALWAYS join titles for title_name: JOIN titles t ON t.title_id = v.title_id
- ROUND all rates to 4 decimal places, percentages to 1 decimal place
- LIMIT 20 unless question asks for totals/aggregations
- title_name values use exact casing: 'The White Lotus S3', 'House of the Dragon S2', etc.
- market values are 2-letter codes: 'AU', 'SG', 'IN', etc.
- Never SELECT literal string labels ('some label' AS col) — use real column values only
- For WoW: use two separate aggregations with date range filters, then compute delta
- NEVER use INTERVAL arithmetic — date is VARCHAR, interval math will fail
  BAD:  date >= CURRENT_DATE - INTERVAL '7 days'
  BAD:  CAST(date AS DATE) + INTERVAL '1 week'
  GOOD: date >= '2026-03-24'
- NEVER use CURRENT_DATE — always use literal date strings
"""


def _ask_claude_for_sql(question: str) -> str:
    """Send question to Claude Haiku, return raw SQL string."""
    client = anthropic.Anthropic(api_key=os.getenv("ANTHROPIC_API_KEY"))
    response = client.messages.create(
        model="claude-haiku-4-5-20251001",
        max_tokens=1024,
        system=SCHEMA_CONTEXT,
        messages=[{
            "role": "user",
            "content": f"""Convert this question to a single DuckDB SQL query.

Question: {question}

Return ONLY the raw SQL — no explanation, no markdown, no ```sql blocks.
End with exactly one semicolon.
Use table aliases. LIMIT 20 unless the question needs totals."""
        }]
    )
    return response.content[0].text.strip()


def _clean_sql(raw: str) -> str:
    """Strip markdown fences, fix unterminated quotes, ensure single semicolon."""
    raw = re.sub(r"```sql\s*", "", raw)
    raw = re.sub(r"```\s*",    "", raw)
    raw = raw.strip().rstrip(";").rstrip()

    # Fix unterminated string literals (odd number of single quotes on a line)
    lines = raw.split("\n")
    fixed = []
    for line in lines:
        if line.count("'") % 2 != 0:
            line = line.rstrip() + "'"
        fixed.append(line)

    return "\n".join(fixed) + ";"


def _run_query(sql: str):
    """Execute SQL against DuckDB (read-only). Returns (DataFrame, error)."""
    try:
        con = duckdb.connect(DB_PATH, read_only=True)
        df  = con.execute(sql).df()
        con.close()
        return df, None
    except Exception as e:
        return None, str(e)


def query(question: str, verbose: bool = True) -> dict:
    """
    Main entry point.
    Input : plain English question
    Output: dict — {question, sql, data (DataFrame | None), error}

    Auto-retries once with a simpler prompt on SQL error.
    """
    if verbose:
        print(f"\n  {'─'*56}")
        print(f"  Q: {question}")

    raw_sql = _ask_claude_for_sql(question)
    sql     = _clean_sql(raw_sql)

    if verbose:
        preview = sql.replace("\n", " ")
        print(f"  SQL: {preview[:100]}{'...' if len(preview) > 100 else ''}")

    df, error = _run_query(sql)

    # One automatic retry on error — simpler prompt
    if error:
        retry_q = (
            f"Simple version only: {question}. "
            "Use basic SELECT, SUM, AVG, COUNT, GROUP BY. "
            "No CTEs, no subqueries, no CASE statements."
        )
        sql2      = _clean_sql(_ask_claude_for_sql(retry_q))
        df, error = _run_query(sql2)
        if not error:
            sql = sql2

    if verbose:
        if error:
            print(f"  ❌ Error: {error}")
        else:
            print(f"  ✓ {len(df)} rows returned")

    return {"question": question, "sql": sql, "data": df, "error": error}


# ─────────────────────────────────────────────────────────────
# Self-test — run directly to verify sql_tool is working
# ─────────────────────────────────────────────────────────────
if __name__ == "__main__":
    print("=" * 65)
    print("sql_tool self-test — WBD APAC Title Performance")
    print("=" * 65)

    tests = [
        # Basic lookups
        "How many titles are in the database?",
        "List all genres and how many titles are in each",
        # Viewership
        "What are the top 5 titles by total starts this month?",
        "What is the average completion rate by market?",
        # Episode
        "Which episode of House of the Dragon S2 has the highest drop-off?",
        # Subscriber
        "How many churned subscribers watched The White Lotus S3?",
        # Benchmark
        "How does The Last of Us S1 completion rate compare to the Fantasy genre average?",
    ]

    passed = 0
    for q in tests:
        result = query(q, verbose=True)
        status = "✅" if result["error"] is None else f"❌ {result['error'][:60]}"
        if result["error"] is None:
            passed += 1
        print(f"  {status}\n")

    print(f"{'='*65}")
    print(f"sql_tool: {passed}/{len(tests)} passed")
    print(f"{'='*65}")
