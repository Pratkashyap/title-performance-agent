"""
agents/data_agent.py
WBD APAC — Title Performance Analyst

The Data Agent is the ONLY agent that touches the database.
All other agents ask this agent for data.

Responsibilities:
  1. Receive a question from the Orchestrator
  2. Classify the query type and time window
  3. Run the SQL tool
  4. Return a clean, structured result set with context

Model: Claude Haiku 4.5 (fast + cheap — SQL is lightweight)
"""

import os
import sys
import json
import re
import anthropic
from dotenv import load_dotenv

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))
from tools.sql_tool import query as sql_query

_env_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "../.env")
load_dotenv(_env_path, override=True)

# ─────────────────────────────────────────────────────────────
# AGENT SYSTEM PROMPT
# ─────────────────────────────────────────────────────────────

DATA_AGENT_SYSTEM = """You are the Data Agent for WBD APAC's Title Performance Analyst system.
You handle all data retrieval for Max / HBO Max streaming analytics across 10 APAC markets.

Your job:
  1. Receive a question about title performance
  2. Classify it precisely so the SQL tool can retrieve the right data
  3. Identify the correct time window and markets in scope

Today's date: 2026-03-31
Markets: AU, SG, HK, IN, JP, KR, TW, TH, PH, MY
Southeast Asia (SEA): SG, TH, PH, MY
Northeast Asia (NEA): JP, KR, TW, HK

QUESTION CATEGORIES:
  A — Diagnosis    : Why is a title underperforming? Root cause analysis.
  B — Snapshot     : Current performance, benchmarks, head-to-head comparisons.
  C — Trends       : WoW momentum, day-of-week, market growth patterns.
  D — Genre/Catalog: Platform-wide, genre health, high-starts/low-completion.
  E — Subscriber   : Segment behaviour, churn risk, acquisition signal.
  F — Alerts       : Proactive flags, WoW drops, launch benchmarks.

QUERY TYPES:
  single_title   — question about one specific title
  multi_title    — comparing 2+ titles or looking across the catalog
  market_split   — market-level breakdown required
  episode_level  — episode drop-off or episode comparison
  subscriber     — involves subscriber segment or churn data
  genre_catalog  — genre-wide or catalog-wide analysis
  trend          — time-series / WoW / momentum analysis
  alert_scan     — scanning all titles for issues

When you receive a question, respond with ONLY a JSON object (no other text):
{
  "category":        "A|B|C|D|E|F",
  "query_type":      "single_title|multi_title|market_split|episode_level|subscriber|genre_catalog|trend|alert_scan",
  "time_window":     "human description e.g. 'last 7 days (2026-03-24 to 2026-03-31)'",
  "markets_in_scope":"'all' or specific markets e.g. 'SG, TH, PH, MY (SEA)'",
  "refined_question":"rewritten question optimised for SQL generation — include exact title names, date ranges, markets",
  "needs_episode_data": true|false,
  "needs_subscriber_data": true|false
}"""


class DataAgent:
    def __init__(self):
        self.client = anthropic.Anthropic(api_key=os.getenv("ANTHROPIC_API_KEY"))
        self.model  = "claude-haiku-4-5-20251001"

    def _classify(self, question: str, on_status=None) -> dict:
        """Classify the question and extract metadata before SQL generation."""
        if on_status:
            on_status("data_agent", "classifying_query", "Analysing question type and scope...")

        response = self.client.messages.create(
            model=self.model,
            max_tokens=512,
            system=DATA_AGENT_SYSTEM,
            messages=[{"role": "user", "content": question}]
        )
        text = response.content[0].text.strip()

        # Extract JSON robustly
        match = re.search(r'\{.*\}', text, re.DOTALL)
        if match:
            try:
                classification = json.loads(match.group())
                if on_status:
                    on_status("data_agent", "query_classified", json.dumps({
                        "query_type": classification.get("query_type", ""),
                        "time_window": classification.get("time_window", ""),
                        "category": classification.get("category", ""),
                    }))
                return classification
            except json.JSONDecodeError:
                pass

        # Fallback if JSON parse fails
        return {
            "category":             "B",
            "query_type":           "single_title",
            "time_window":          "last 30 days",
            "markets_in_scope":     "all",
            "refined_question":     question,
            "needs_episode_data":   False,
            "needs_subscriber_data":False,
        }

    def fetch(self, question: str, verbose: bool = False, on_status=None) -> dict:
        """
        Main entry point for all agents.

        Args:
            question    : plain English question about title performance
            verbose     : print SQL and row counts to terminal
            on_status   : optional callback(agent, event, detail) for pipeline UI

        Returns dict:
            question            — original question
            refined             — SQL-optimised question
            category            — A/B/C/D/E/F
            query_type          — classification
            time_window         — human description
            markets_in_scope    — which markets
            needs_episode_data  — bool
            needs_subscriber_data — bool
            sql                 — generated SQL
            data                — pandas DataFrame (None on error)
            error               — error string (None on success)
            row_count           — number of rows returned
        """
        if on_status:
            on_status("data_agent", "start", "")

        # Step 1: Classify and refine
        classification = self._classify(question, on_status=on_status)
        refined_q = classification.get("refined_question", question)

        # Step 2: Run SQL tool
        if on_status:
            on_status("data_agent", "sql_generated", refined_q[:120])

        result = sql_query(refined_q, verbose=verbose)

        # Step 3: Surface SQL error clearly
        if result.get("error"):
            if on_status:
                on_status("data_agent", "error", result["error"])
        else:
            row_count = len(result["data"]) if result.get("data") is not None else 0
            if on_status:
                on_status("data_agent", "query_executed", str(row_count))

        return {
            "question":             question,
            "refined":              refined_q,
            "category":             classification.get("category", "B"),
            "query_type":           classification.get("query_type", ""),
            "time_window":          classification.get("time_window", ""),
            "markets_in_scope":     classification.get("markets_in_scope", "all"),
            "needs_episode_data":   classification.get("needs_episode_data", False),
            "needs_subscriber_data":classification.get("needs_subscriber_data", False),
            "sql":                  result.get("sql"),
            "data":                 result.get("data"),
            "error":                result.get("error"),
            "row_count":            len(result["data"]) if result.get("data") is not None else 0,
        }

    def fetch_multiple(self, questions: list, verbose: bool = False, on_status=None) -> list:
        """
        Fetch data for multiple questions in sequence.
        Used when the Orchestrator needs several data pulls for one answer.
        Returns list of fetch() results.
        """
        return [self.fetch(q, verbose=verbose, on_status=on_status) for q in questions]


# ─────────────────────────────────────────────────────────────
# Self-test — covers all 6 question categories
# ─────────────────────────────────────────────────────────────
if __name__ == "__main__":
    from rich.console import Console
    from rich.table   import Table
    from rich         import box

    console = Console()
    agent   = DataAgent()

    console.print("\n[bold cyan]Data Agent Self-Test — WBD APAC Title Performance[/bold cyan]")
    console.print("[dim]Testing all 6 question categories (A–F)[/dim]\n")

    tests = [
        # Cat A — Diagnosis
        ("A", "Why is The White Lotus S3 underperforming in Southeast Asia this month?"),
        # Cat B — Snapshot
        ("B", "How is House of the Dragon S2 performing vs. comparable fantasy titles?"),
        ("B", "What is the completion rate for The Last of Us S1 across all APAC markets?"),
        # Cat C — Trends
        ("C", "Is House of the Dragon S2 gaining or losing momentum week over week?"),
        # Cat D — Genre / Catalog
        ("D", "Which titles have high starts but low completions right now?"),
        ("D", "Which genre is overperforming on Max APAC this month?"),
        # Cat E — Subscriber
        ("E", "What subscriber segment watches The Last of Us S2 the most?"),
        # Cat F — Alerts
        ("F", "Which titles this week need immediate attention?"),
    ]

    results_table = Table(box=box.SIMPLE, show_header=True, header_style="bold white")
    results_table.add_column("Cat", width=4)
    results_table.add_column("Question", width=52)
    results_table.add_column("Rows", width=6, justify="right")
    results_table.add_column("Status", width=8)

    passed = 0
    for expected_cat, question in tests:
        console.print(f"[dim]Running:[/dim] [white]{question[:70]}[/white]")
        result = agent.fetch(question, verbose=False)

        ok     = result["error"] is None
        status = "[green]✅ pass[/green]" if ok else "[red]❌ fail[/red]"
        rows   = str(result["row_count"]) if ok else "—"
        cat    = result["category"]

        results_table.add_row(cat, question[:52], rows, status)
        if ok:
            passed += 1
        else:
            console.print(f"  [red]Error: {result['error']}[/red]")

        # Show sample data for first result of each category
        if ok and result["data"] is not None and len(result["data"]) > 0:
            console.print(f"  [dim]Sample: {result['data'].columns.tolist()} — {result['row_count']} rows[/dim]")

    console.print()
    console.print(results_table)
    console.print(f"\n[bold]Result: {passed}/{len(tests)} passed[/bold]")
    if passed == len(tests):
        console.print("[bold green]✅ Data Agent fully operational. Phase 1 complete.[/bold green]\n")
    else:
        console.print(f"[bold yellow]⚠️  {len(tests)-passed} tests failed — check errors above.[/bold yellow]\n")
