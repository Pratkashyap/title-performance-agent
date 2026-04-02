"""
agents/benchmark_agent.py
WBD APAC — Title Performance Analyst

Peer-group comparison engine. Handles:
  Category B (enhanced) — Finds comparable title peer groups,
  computes performance delta vs genre averages and comparable titles,
  produces a structured benchmark comparison section.

Runs AFTER the primary DataAgent fetch. Adds a second opinion to
the Performance Analyst's snapshot by providing peer context.

Model: Claude Haiku 4.5 (fast, focused)
"""

import os
import sys
import anthropic
from dotenv import load_dotenv

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))
from agents.data_agent import DataAgent

_env_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "../.env")
load_dotenv(_env_path, override=True)

# ─────────────────────────────────────────────────────────────
# BENCHMARK SYSTEM PROMPT
# ─────────────────────────────────────────────────────────────

BENCHMARK_SYSTEM = """You are the Benchmark Agent for WBD APAC's Title Performance Analyst system.
Your job: compare a streaming title against its peer group and genre benchmarks.

Today's date: 2026-03-31
Region: APAC — 10 markets: AU, SG, HK, IN, JP, KR, TW, TH, PH, MY

You receive:
1. PRIMARY DATA — the target title's recent viewership metrics
2. PEER GROUP DATA — comparable titles' recent performance (last 30 days)
3. GENRE BENCHMARKS — genre avg completion rate, avg starts day 7, avg starts day 30

Produce a PEER GROUP COMPARISON section exactly as:

**PEER GROUP COMPARISON: [Title Name]**

**VS GENRE AVERAGE**
- Completion rate: X% vs genre avg Y% → [+Z% above | -Z% below]
- Avg daily starts: X vs genre_avg_starts_day7 Y → [+Z% above | -Z% below]

**VS COMPARABLE TITLES**
| Title | Avg Daily Starts | Completion Rate | vs Target |
|---|---|---|---|
[Up to 5 comparable titles. Calculate delta vs target title — e.g., "+12%" or "-8%".]

**PEER RANKING**
The target title ranks [#N of M comparable titles] by [starts / completion_rate].
Closest rival: [Title] is [X% above/below] on [key metric].

**BENCHMARK VERDICT**
1–2 sentences: Is this title punching above, at, or below its weight in its peer group?

RULES:
- Always show ± deltas as percentages (rounded to 1 decimal)
- Use concrete numbers only — never vague statements like "performing well"
- If peer data is sparse or a column is missing, acknowledge briefly and work with what's available
- Max 250 words total
"""


def _format_benchmark_data(
    question: str,
    primary_data: dict,
    peer_data: dict,
    bench_data: dict,
) -> str:
    """Format all three data sources into a clean text block for Haiku."""
    lines = [f"Question: {question}", ""]

    # ── Primary title performance ──────────────────────────────
    lines.append("── PRIMARY DATA (target title) ──")
    df_primary = primary_data.get("data")
    if df_primary is not None and not df_primary.empty:
        lines.append(f"Rows: {len(df_primary)}")
        lines.append(df_primary.to_string(index=False))
    else:
        lines.append("No primary data returned.")
    lines.append("")

    # ── Genre benchmarks ──────────────────────────────────────
    lines.append("── GENRE BENCHMARKS (from title_benchmarks table) ──")
    df_bench = bench_data.get("data") if bench_data else None
    if df_bench is not None and not df_bench.empty:
        lines.append(df_bench.to_string(index=False))
    else:
        lines.append("No benchmark data available.")
    lines.append("")

    # ── Peer group ────────────────────────────────────────────
    lines.append("── PEER GROUP DATA (comparable titles, last 30 days) ──")
    df_peer = peer_data.get("data") if peer_data else None
    if df_peer is not None and not df_peer.empty:
        lines.append(f"Rows: {len(df_peer)}")
        lines.append(df_peer.to_string(index=False))
    else:
        lines.append("No peer group data available.")
    lines.append("")

    return "\n".join(lines)


class BenchmarkAgent:
    def __init__(self):
        self.client     = anthropic.Anthropic(api_key=os.getenv("ANTHROPIC_API_KEY"))
        self.model      = "claude-haiku-4-5-20251001"
        self.data_agent = DataAgent()

    def _extract_title_hint(self, question: str) -> str:
        """Best-effort extraction of the title name from the question."""
        q = question
        # Strip common lead phrases
        for prefix in [
            "How is ", "How does ", "What is the performance of ",
            "Compare ", "Is ", "Show me ", "How well is ",
        ]:
            if q.lower().startswith(prefix.lower()):
                q = q[len(prefix):]
                break
        # Trim at common stop phrases
        for stopper in [
            " vs ", " compared", " benchmark", " performing",
            " doing", " in ", " across", "?",
        ]:
            idx = q.lower().find(stopper.lower())
            if idx > 2:
                q = q[:idx]
                break
        return q.strip()

    def analyse(
        self,
        question: str,
        primary_data: dict,
        on_status=None,
    ) -> dict:
        """
        Run peer-group comparison for a Cat B question.

        Args:
            question     : original user question
            primary_data : DataAgent.fetch() result for the target title
            on_status    : optional callback(agent, event, detail)

        Returns dict:
            question    — original question
            insight     — peer group comparison markdown section
            peer_count  — number of comparable titles found
            model_used  — model ID
            error       — None on success
        """
        def emit(event, detail=""):
            if on_status:
                on_status("benchmark_agent", event, detail)

        emit("start")

        title_hint = self._extract_title_hint(question)
        emit("preparing", f"Fetching benchmarks + peer group for '{title_hint}'...")

        # ── Fetch 1: Genre benchmarks ─────────────────────────
        bench_q = (
            f"What are the genre_avg_completion_rate, genre_avg_starts_day7, "
            f"genre_avg_starts_day30, and comparable_title_ids "
            f"for the title closest to '{title_hint}'? "
            f"Join title_benchmarks to titles on title_id."
        )
        bench_data = self.data_agent.fetch(bench_q, verbose=False)
        emit("progress", f"Benchmarks: {bench_data.get('row_count', 0)} rows")

        # ── Fetch 2: Comparable titles' recent performance ────
        peer_q = (
            f"Show avg daily starts, avg completion rate, and sum of unique_viewers "
            f"for each title over the last 30 days (date >= '2026-03-01'). "
            f"Join viewership_daily to titles on title_id. "
            f"Group by t.title_name, t.genre. "
            f"Filter to the same genre as '{title_hint}'. "
            f"Order by avg_daily_starts DESC. "
            f"Limit 8."
        )
        peer_data = self.data_agent.fetch(peer_q, verbose=False)
        peer_count = peer_data.get("row_count", 0)
        emit("progress", f"Peer group: {peer_count} titles")

        # ── Build prompt and call Haiku ───────────────────────
        formatted = _format_benchmark_data(question, primary_data, peer_data, bench_data)
        emit("analysing", "Computing peer deltas via Claude Haiku...")

        response = self.client.messages.create(
            model=self.model,
            max_tokens=600,
            system=BENCHMARK_SYSTEM,
            messages=[{
                "role": "user",
                "content": (
                    f"Produce a PEER GROUP COMPARISON for this question:\n\n{formatted}"
                ),
            }],
        )

        insight = response.content[0].text.strip()
        emit("done", f"{peer_count} comparable titles analysed")

        return {
            "question":   question,
            "insight":    insight,
            "peer_count": peer_count,
            "model_used": self.model,
            "error":      None,
        }


# ─────────────────────────────────────────────────────────────
# Self-test — 3 Cat B questions
# ─────────────────────────────────────────────────────────────
if __name__ == "__main__":
    from rich.console import Console
    from rich.table   import Table
    from rich         import box

    console  = Console()
    data_ag  = DataAgent()
    bench_ag = BenchmarkAgent()

    console.print("\n[bold cyan]Benchmark Agent Self-Test — WBD APAC[/bold cyan]")
    console.print("[dim]Testing Cat B peer group comparisons[/dim]\n")

    tests = [
        "How is House of the Dragon S2 performing vs comparable fantasy titles?",
        "How does The Last of Us S2 benchmark against similar HBO Originals?",
        "Is White Lotus S3 above or below its peer group in completion rates?",
    ]

    tbl = Table(box=box.SIMPLE, show_header=True, header_style="bold white")
    tbl.add_column("Question", width=56)
    tbl.add_column("Peers", width=6, justify="right")
    tbl.add_column("Words", width=6, justify="right")
    tbl.add_column("Status", width=8)

    passed = 0
    for q in tests:
        console.print(f"[dim]Running:[/dim] [white]{q[:70]}[/white]")
        primary = data_ag.fetch(q, verbose=False)
        result  = bench_ag.analyse(q, primary)
        ok      = result["error"] is None
        words   = str(len(result["insight"].split())) if ok else "—"
        peers   = str(result.get("peer_count", 0))
        status  = "[green]✅ pass[/green]" if ok else "[red]❌ fail[/red]"
        tbl.add_row(q[:56], peers, words, status)
        if ok:
            passed += 1
            console.print(f"  [dim]Preview: {result['insight'][:130]}...[/dim]\n")
        else:
            console.print(f"  [red]Error: {result['error']}[/red]\n")

    console.print(tbl)
    console.print(f"\n[bold]Result: {passed}/{len(tests)} passed[/bold]")
    if passed == len(tests):
        console.print("[bold green]✅ Benchmark Agent fully operational.[/bold green]\n")
    else:
        console.print(f"[bold yellow]⚠️  {len(tests)-passed} tests failed.[/bold yellow]\n")
