"""
agents/performance_analyst.py
WBD APAC — Title Performance Analyst

Core analysis engine. Handles:
  Category A — Diagnosis (Why is a title underperforming? Root cause analysis)
  Category B — Snapshot  (Current performance vs benchmarks)

Receives structured data from the Data Agent and produces
content-team-ready insights using Claude Sonnet (deep reasoning).

Model: Claude Sonnet 4.6
"""

import os
import sys
import json
import anthropic
import pandas as pd
from dotenv import load_dotenv

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))

_env_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "../.env")
load_dotenv(_env_path, override=True)

# ─────────────────────────────────────────────────────────────
# ANALYST SYSTEM PROMPT
# ─────────────────────────────────────────────────────────────

ANALYST_SYSTEM = """You are the Performance Analyst for WBD APAC's Title Performance Analyst system.
You receive streaming data from Max / HBO Max and produce actionable analysis for the content team.

Today's date: 2026-03-31
Region: APAC — 10 markets: AU, SG, HK, IN, JP, KR, TW, TH, PH, MY
Southeast Asia (SEA): SG, TH, PH, MY
Northeast Asia (NEA): JP, KR, TW, HK

KEY PERFORMANCE BENCHMARKS:
- High completion rate : >= 65%
- Low completion rate  : < 45%
- Engagement gap       : starts high (top 25%) but completion_rate < 50%
- Underperforming      : starts < genre_avg_starts OR completion_rate < 80% of genre avg
- At-risk (momentum)   : WoW starts decline > 15%
- Sleeper hit          : starts below median but completion_rate >= 70%

TITLE PERFORMANCE TIERS (for context):
- champion   : HotD S2, White Lotus S3, TLOU S1+S2, Dune 2, Barbie, Succession S4
- underperform: The Idol, The Nevers, Aquaman, The Flash, True Blood
- sleeper    : Mare of Easttown, The Undoing, We Own This City, Expats, The Sympathizer
- at_risk    : Euphoria S2, Industry S2, Curb S12, His Dark Materials
- new_launch : Pachinko S2, Tokyo Vice S2, The Jinx Part 2

─────────────────────────────────────────────────────────────
CATEGORY A — DIAGNOSIS (Why is a title underperforming? What is wrong?)

Structure your response exactly as:

**DIAGNOSIS: [Title Name]**

**ROOT CAUSE SUMMARY**
2-3 sentences summarising the main performance issue with specific numbers.

**KEY METRICS**
- Completion rate: X% (genre avg: Y%) — [Above/At/Below benchmark]
- Total starts (last 30d): X (genre avg: Y) — [Above/At/Below]
- Returning viewers: X% — [High/Average/Low retention]
- Watch time: Xm avg per session

**MARKET BREAKDOWN**
| Market | Starts | Completion Rate | Status |
|---|---|---|---|
[top 3 + bottom 3 markets or all if fewer rows]

**ROOT CAUSES IDENTIFIED**
1. [Primary cause] — evidence from the data
2. [Secondary cause] — evidence from the data
3. [Optional third cause if clearly supported]

**RECOMMENDATIONS**
1. [Specific action for content/marketing team]
2. [Specific action]
3. [Specific action]

─────────────────────────────────────────────────────────────
CATEGORY B — SNAPSHOT (How is a title performing vs benchmarks?)

Structure your response exactly as:

**SNAPSHOT: [Title Name]**

**PERFORMANCE SUMMARY**
2-3 sentences on overall status. Include numbers. Be direct — is this title healthy?

**KEY METRICS**
- Completion rate: X% (genre avg: Y%) — [Above/At/Below]
- Total starts (last 30d): X (genre avg: Y) — [Above/At/Below]
- Unique viewers: X | Returning viewers: X%
- Watch time: Xm avg

**VS BENCHMARKS**
[Compare vs genre average and comparable titles if data provided.
 Use percentages: "23% above genre avg" or "41% below genre avg".]

**TOP & BOTTOM MARKETS**
- Top 3: [Market] (X%), [Market] (X%), [Market] (X%)
- Bottom 3: [Market] (X%), [Market] (X%), [Market] (X%)

**OUTLOOK**
1-2 sentences on momentum and recommended next action.

─────────────────────────────────────────────────────────────
RULES (apply to both categories):
- Always use specific numbers — never say "ROAS improved" without a number
- Comparison is mandatory — always benchmark vs genre avg
- Tone: confident, data-driven, content-team ready (not marketing fluff)
- Max 420 words total
- If data is sparse or a column is missing, acknowledge it briefly and work with what's available
"""


def _format_data(data_results: list, question: str, category: str) -> str:
    """Convert list of DataAgent fetch() results to a clean text block for Claude."""
    lines = [
        f"Question: {question}",
        f"Category: {category}",
        "",
    ]

    for i, dr in enumerate(data_results, 1):
        label = "PRIMARY DATA" if i == 1 else f"SUPPLEMENTARY DATA {i-1}"
        lines.append(f"── {label} ──")
        lines.append(f"Sub-question: {dr.get('refined', dr.get('question', ''))}")
        lines.append(f"Time window: {dr.get('time_window', 'unspecified')}")
        lines.append(f"Markets: {dr.get('markets_in_scope', 'all')}")

        df = dr.get("data")
        if df is not None and not df.empty:
            lines.append(f"Rows: {len(df)}")
            lines.append(df.to_string(index=False))
        else:
            lines.append("No data returned.")

        lines.append("")

    return "\n".join(lines)


class PerformanceAnalyst:
    def __init__(self):
        self.client = anthropic.Anthropic(api_key=os.getenv("ANTHROPIC_API_KEY"))
        self.model  = "claude-sonnet-4-6"

    def analyse(
        self,
        question: str,
        category: str,
        data_results: list,
        on_status=None,
    ) -> dict:
        """
        Core analysis method.

        Args:
            question     : original user question
            category     : 'A' or 'B'
            data_results : list of DataAgent.fetch() return dicts
            on_status    : optional callback(agent, event, detail)

        Returns dict:
            question     — original question
            category     — A or B
            insight      — full analysis text (markdown)
            model_used   — model ID
            error        — None on success
        """
        if on_status:
            total_rows = sum(
                len(dr["data"]) for dr in data_results
                if dr.get("data") is not None
            )
            on_status("performance_analyst", "preparing",
                      f"{total_rows} rows across {len(data_results)} data pull(s)")
            label = "root cause diagnosis" if category == "A" else "performance snapshot"
            on_status("performance_analyst", "analysing",
                      f"Running {label} via Claude Sonnet...")

        formatted = _format_data(data_results, question, category)

        response = self.client.messages.create(
            model=self.model,
            max_tokens=1200,
            system=ANALYST_SYSTEM,
            messages=[{
                "role": "user",
                "content": (
                    f"Analyse this streaming performance data and produce a "
                    f"Category {category} response:\n\n{formatted}"
                ),
            }],
        )

        insight = response.content[0].text.strip()

        if on_status:
            on_status("performance_analyst", "done", "Analysis complete")

        return {
            "question":   question,
            "category":   category,
            "insight":    insight,
            "model_used": self.model,
            "error":      None,
        }


# ─────────────────────────────────────────────────────────────
# Self-test
# ─────────────────────────────────────────────────────────────
if __name__ == "__main__":
    from rich.console import Console
    from rich         import box
    from rich.table   import Table
    import sys
    sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))
    from agents.data_agent import DataAgent

    console = Console()
    data_ag  = DataAgent()
    analyst  = PerformanceAnalyst()

    console.print("\n[bold cyan]Performance Analyst Self-Test — WBD APAC[/bold cyan]")
    console.print("[dim]Testing Cat A (Diagnosis) + Cat B (Snapshot)[/dim]\n")

    tests = [
        ("A", "Why is The White Lotus S3 underperforming in Southeast Asia this month?"),
        ("B", "How is House of the Dragon S2 performing vs comparable fantasy titles?"),
        ("B", "What is the current performance snapshot for The Last of Us S2?"),
        ("A", "Why does Euphoria S2 have low completion rates across APAC?"),
    ]

    results_table = Table(box=box.SIMPLE, show_header=True, header_style="bold white")
    results_table.add_column("Cat", width=4)
    results_table.add_column("Question", width=55)
    results_table.add_column("Words", width=6, justify="right")
    results_table.add_column("Status", width=8)

    passed = 0
    for cat, question in tests:
        console.print(f"[dim]Running:[/dim] [white]{question[:65]}[/white]")

        # Fetch data
        data_results = [data_ag.fetch(question, verbose=False)]

        # For Cat A, also fetch benchmark context
        if cat == "A":
            bench_q = question.replace("Why is ", "What are the benchmark starts and completion rates for ")
            bench_q = bench_q.replace(" underperforming", "").replace(" this month?", " in the genre?")
            bench_r = data_ag.fetch(bench_q, verbose=False)
            if bench_r.get("data") is not None:
                data_results.append(bench_r)

        result = analyst.analyse(question, cat, data_results)

        ok     = result["error"] is None
        status = "[green]✅ pass[/green]" if ok else "[red]❌ fail[/red]"
        words  = str(len(result["insight"].split())) if ok else "—"

        results_table.add_row(cat, question[:55], words, status)
        if ok:
            passed += 1
            console.print(f"  [dim]Preview: {result['insight'][:120]}...[/dim]")
        else:
            console.print(f"  [red]Error: {result['error']}[/red]")

    console.print()
    console.print(results_table)
    console.print(f"\n[bold]Result: {passed}/{len(tests)} passed[/bold]")
    if passed == len(tests):
        console.print("[bold green]✅ Performance Analyst fully operational.[/bold green]\n")
    else:
        console.print(f"[bold yellow]⚠️  {len(tests)-passed} tests failed.[/bold yellow]\n")
