"""
agents/alert_agent.py
WBD APAC — Title Performance Analyst

Proactive monitoring engine. Scans all titles and surfaces issues
before anyone asks. Handles:
  Category F — WoW viewership drops, completion rate warnings,
  new launch benchmarks, platform-wide weekly alert summary.

Runs as the sole specialist for Cat F questions. Fetches three
dedicated scan data sets via the DataAgent and produces a
prioritised alert bulletin.

Model: Claude Haiku 4.5 (fast, platform-wide scan)
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
# ALERT AGENT SYSTEM PROMPT
# ─────────────────────────────────────────────────────────────

ALERT_SYSTEM = """You are the Alert Agent for WBD APAC's Title Performance Analyst system.
Your job: scan the platform, identify titles with urgent issues, and produce a prioritised alert bulletin.

Today's date: 2026-03-31
Region: APAC — 10 markets: AU, SG, HK, IN, JP, KR, TW, TH, PH, MY
Alert window: last 14 days (current week vs prior week).

ALERT SEVERITY LEVELS:
  🔴 CRITICAL  : WoW starts drop ≥ 30% OR completion rate < 35%
  🟡 WARNING   : WoW starts drop 15–29% OR completion rate 35–49%
  🟢 WATCH     : WoW starts drop 10–14% OR new launch underperforming day-7 benchmark

MOMENTUM definition:
  WoW change = (this_week_starts - last_week_starts) / last_week_starts * 100
  Positive = growing, Negative = declining

You receive:
1. WoW DROP SCAN   — titles with declining week-over-week starts
2. COMPLETION SCAN — titles with completion rates below 50% this week
3. LAUNCH SCAN     — new titles (< 30 days old) vs their day-7 genre benchmarks

Structure your response exactly as:

**PLATFORM ALERT BULLETIN — APAC**
*Week ending 2026-03-31*

**CRITICAL ALERTS** 🔴
| Priority | Title | Issue | WoW Change | Metric | Action |
|---|---|---|---|---|---|
[Titles with ≥30% WoW drop OR completion <35%. Sort by severity DESC. Max 5.]
If none: "No critical alerts this week."

**WARNINGS** 🟡
| Priority | Title | Issue | WoW Change | Metric | Action |
|---|---|---|---|---|---|
[Titles with 15–29% WoW drop OR completion 35–49%. Max 5.]
If none: "No warnings this week."

**WATCH LIST** 🟢
| Title | Issue | Signal |
|---|---|---|
[Titles with 10–14% WoW drop OR new launches below benchmark. Max 5.]
If none: "Nothing on watch list this week."

**SUMMARY**
Total alerts: [X critical · Y warnings · Z watch]
Biggest risk: [1 sentence naming the single most urgent title and why]
Recommended first action: [1 specific action for the content/programming team]

RULES:
- Always show WoW change as percentage with sign (e.g. -34.2%, +12.1%)
- Always show completion rates as percentages (e.g. 38.5%, not 0.385)
- Round all numbers to 1 decimal place
- Action column: max 8 words — be specific (e.g. "Escalate to programming team", "Push editorial spotlight")
- Never say "significant" without a number
- Max 420 words total
- If a title appears in multiple scans, include it once at its highest severity level
"""


def _format_alert_data(
    question: str,
    wow_data: dict,
    completion_data: dict,
    launch_data: dict,
) -> str:
    """Format all three scan data sets into a clean text block for the Haiku prompt."""
    lines = [f"Question: {question}", ""]

    # ── WoW drop scan ──────────────────────────────────────────
    lines.append("── WoW DROP SCAN (titles with declining week-over-week starts) ──")
    df_wow = wow_data.get("data")
    if df_wow is not None and not df_wow.empty:
        lines.append(f"Rows: {len(df_wow)}")
        lines.append(df_wow.to_string(index=False))
    else:
        lines.append("No WoW drop data available.")
    lines.append("")

    # ── Completion rate scan ──────────────────────────────────
    lines.append("── COMPLETION RATE SCAN (titles below 50% completion this week) ──")
    df_comp = completion_data.get("data") if completion_data else None
    if df_comp is not None and not df_comp.empty:
        lines.append(f"Rows: {len(df_comp)}")
        lines.append(df_comp.to_string(index=False))
    else:
        lines.append("No low-completion titles found.")
    lines.append("")

    # ── New launch scan ───────────────────────────────────────
    lines.append("── NEW LAUNCH SCAN (titles < 30 days old vs day-7 genre benchmark) ──")
    df_launch = launch_data.get("data") if launch_data else None
    if df_launch is not None and not df_launch.empty:
        lines.append(f"Rows: {len(df_launch)}")
        lines.append(df_launch.to_string(index=False))
    else:
        lines.append("No new launches found or all meeting benchmarks.")
    lines.append("")

    return "\n".join(lines)


class AlertAgent:
    def __init__(self):
        self.client     = anthropic.Anthropic(api_key=os.getenv("ANTHROPIC_API_KEY"))
        self.model      = "claude-haiku-4-5-20251001"
        self.data_agent = DataAgent()

    def analyse(
        self,
        question: str,
        primary_data: dict,
        on_status=None,
    ) -> dict:
        """
        Run platform-wide alert scan for a Cat F question.

        Args:
            question     : original user question
            primary_data : DataAgent.fetch() result (initial context)
            on_status    : optional callback(agent, event, detail)

        Returns dict:
            question       — original question
            insight        — full alert bulletin markdown text
            alerts_found   — total number of flagged rows across all scans
            model_used     — model ID
            error          — None on success
        """
        def emit(event, detail=""):
            if on_status:
                on_status("alert_agent", event, detail)

        emit("start")
        emit("preparing", "Scanning platform: WoW drops + completion rates + new launches...")

        # ── Fetch 1: WoW viewership drop scan ────────────────
        wow_q = (
            "Compare week-over-week starts for all titles. "
            "Current week = 2026-03-24 to 2026-03-31. Prior week = 2026-03-17 to 2026-03-23. "
            "SELECT t.title_name, t.genre, "
            "SUM(CASE WHEN v.date >= '2026-03-24' THEN v.starts ELSE 0 END) as this_week_starts, "
            "SUM(CASE WHEN v.date >= '2026-03-17' AND v.date < '2026-03-24' THEN v.starts ELSE 0 END) as last_week_starts, "
            "ROUND( "
            "  (SUM(CASE WHEN v.date >= '2026-03-24' THEN v.starts ELSE 0 END) - "
            "   SUM(CASE WHEN v.date >= '2026-03-17' AND v.date < '2026-03-24' THEN v.starts ELSE 0 END)) * 100.0 / "
            "  NULLIF(SUM(CASE WHEN v.date >= '2026-03-17' AND v.date < '2026-03-24' THEN v.starts ELSE 0 END), 0), "
            "1) as wow_change_pct. "
            "FROM viewership_daily v JOIN titles t ON v.title_id = t.title_id "
            "WHERE v.date >= '2026-03-17'. "
            "GROUP BY t.title_name, t.genre. "
            "HAVING last_week_starts > 0 AND wow_change_pct < -10. "
            "ORDER BY wow_change_pct ASC. "
            "LIMIT 10."
        )
        wow_data = self.data_agent.fetch(wow_q, verbose=False)
        wow_count = wow_data.get("row_count", 0)
        emit("progress", f"WoW drop scan: {wow_count} titles flagged")

        # ── Fetch 2: Low completion rate scan ─────────────────
        completion_q = (
            "Find titles with average completion rate below 50% in the last 7 days "
            "(date >= '2026-03-24'). "
            "SELECT t.title_name, t.genre, t.format, "
            "ROUND(AVG(v.completion_rate) * 100, 1) as avg_completion_pct, "
            "SUM(v.starts) as total_starts_7d, "
            "ROUND(AVG(v.starts), 1) as avg_daily_starts. "
            "FROM viewership_daily v JOIN titles t ON v.title_id = t.title_id "
            "WHERE v.date >= '2026-03-24'. "
            "GROUP BY t.title_name, t.genre, t.format. "
            "HAVING AVG(v.completion_rate) < 0.50 AND SUM(v.starts) > 100. "
            "ORDER BY avg_completion_pct ASC. "
            "LIMIT 10."
        )
        completion_data = self.data_agent.fetch(completion_q, verbose=False)
        comp_count = completion_data.get("row_count", 0)
        emit("progress", f"Low completion scan: {comp_count} titles flagged")

        # ── Fetch 3: New launch benchmark scan ────────────────
        launch_q = (
            "Find titles released in the last 30 days and compare their average "
            "daily starts to the genre day-7 benchmark. "
            "SELECT t.title_name, t.genre, t.release_date, "
            "ROUND(AVG(v.starts), 1) as avg_daily_starts, "
            "b.genre_avg_starts_day7 as genre_day7_benchmark, "
            "ROUND( "
            "  (AVG(v.starts) - b.genre_avg_starts_day7) * 100.0 / "
            "  NULLIF(b.genre_avg_starts_day7, 0), "
            "1) as vs_benchmark_pct. "
            "FROM viewership_daily v "
            "JOIN titles t ON v.title_id = t.title_id "
            "JOIN title_benchmarks b ON t.title_id = b.title_id "
            "WHERE t.release_date >= '2026-03-01' AND v.date >= '2026-03-01'. "
            "GROUP BY t.title_name, t.genre, t.release_date, b.genre_avg_starts_day7. "
            "ORDER BY vs_benchmark_pct ASC. "
            "LIMIT 10."
        )
        launch_data = self.data_agent.fetch(launch_q, verbose=False)
        launch_count = launch_data.get("row_count", 0)
        emit("progress", f"New launch scan: {launch_count} titles found")

        total_flags = wow_count + comp_count + launch_count

        # ── LLM: Alert bulletin ───────────────────────────────
        formatted = _format_alert_data(question, wow_data, completion_data, launch_data)
        emit("analysing", "Prioritising alerts via Claude Haiku...")

        response = self.client.messages.create(
            model=self.model,
            max_tokens=900,
            system=ALERT_SYSTEM,
            messages=[{
                "role": "user",
                "content": (
                    f"Produce a PLATFORM ALERT BULLETIN for this Cat F question:\n\n{formatted}"
                ),
            }],
        )

        insight = response.content[0].text.strip()
        emit("done", f"{total_flags} total flagged rows across 3 scans")

        return {
            "question":     question,
            "insight":      insight,
            "alerts_found": total_flags,
            "model_used":   self.model,
            "error":        None,
        }


# ─────────────────────────────────────────────────────────────
# Self-test — 3 Cat F questions
# ─────────────────────────────────────────────────────────────
if __name__ == "__main__":
    from rich.console import Console
    from rich.table   import Table
    from rich         import box

    console  = Console()
    data_ag  = DataAgent()
    alert_ag = AlertAgent()

    console.print("\n[bold cyan]Alert Agent Self-Test — WBD APAC[/bold cyan]")
    console.print("[dim]Testing Cat F platform-wide alert scanning[/dim]\n")

    tests = [
        "Which titles need immediate attention this week?",
        "Flag any title whose viewership dropped more than 30% week over week.",
        "Give me the full weekly alert bulletin for APAC.",
    ]

    tbl = Table(box=box.SIMPLE, show_header=True, header_style="bold white")
    tbl.add_column("Question", width=56)
    tbl.add_column("Flags", width=6, justify="right")
    tbl.add_column("Words", width=6, justify="right")
    tbl.add_column("Status", width=8)

    passed = 0
    for q in tests:
        console.print(f"[dim]Running:[/dim] [white]{q[:70]}[/white]")
        primary = data_ag.fetch(q, verbose=False)

        def show(agent, event, detail=""):
            if event not in ("start", "done"):
                console.print(f"  [dim]{agent}:{event} {detail}[/dim]")

        result  = alert_ag.analyse(q, primary, on_status=show)
        ok      = result["error"] is None
        words   = str(len(result["insight"].split())) if ok else "—"
        flags   = str(result.get("alerts_found", 0))
        status  = "[green]✅ pass[/green]" if ok else "[red]❌ fail[/red]"
        tbl.add_row(q[:56], flags, words, status)
        if ok:
            passed += 1
            console.print(f"  [dim]Preview: {result['insight'][:150]}...[/dim]\n")
        else:
            console.print(f"  [red]Error: {result['error']}[/red]\n")

    console.print(tbl)
    console.print(f"\n[bold]Result: {passed}/{len(tests)} passed[/bold]")
    if passed == len(tests):
        console.print("[bold green]✅ Alert Agent fully operational.[/bold green]\n")
    else:
        console.print(f"[bold yellow]⚠️  {len(tests)-passed} tests failed.[/bold yellow]\n")
