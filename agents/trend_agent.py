"""
agents/trend_agent.py
WBD APAC — Title Performance Analyst

Week-over-week momentum and trend analysis engine. Handles:
  Category C — Is a title gaining or losing audience?
  WoW starts momentum, completion rate trends, market-level growth/decline.

Runs as the sole specialist for Cat C questions. Fetches its own
weekly time-series and market-level data via the DataAgent.

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
# TREND SYSTEM PROMPT
# ─────────────────────────────────────────────────────────────

TREND_SYSTEM = """You are the Trend Agent for WBD APAC's Title Performance Analyst system.
Your job: analyse week-over-week (WoW) momentum and time-series patterns for streaming titles.

Today's date: 2026-03-31
Region: APAC — 10 markets: AU, SG, HK, IN, JP, KR, TW, TH, PH, MY
Southeast Asia (SEA): SG, TH, PH, MY  |  Northeast Asia (NEA): JP, KR, TW, HK

TREND THRESHOLDS:
- GROWING    📈 : WoW starts change > +5%
- DECLINING  📉 : WoW starts change < -5%
- STABLE     ➡️ : WoW starts change ±5%
- RECOVERING 🔄 : Was declining, now turning positive WoW
- AT RISK    ⚠️  : WoW decline > 15% for 2+ consecutive weeks

You receive:
1. WEEKLY DATA    — weekly totals: starts, completions, completion_rate per week
2. MARKET DATA    — market-level starts + completion_rate for recent weeks

Structure your response exactly as:

**TREND ANALYSIS: [Title Name]**

**MOMENTUM VERDICT**
[GROWING 📈 | DECLINING 📉 | STABLE ➡️ | RECOVERING 🔄 | AT RISK ⚠️]
One sentence: most recent WoW % change in starts and what it means.

**WEEK-OVER-WEEK TABLE**
| Week | Starts | Completion % | WoW Δ Starts | WoW Δ Completion |
|---|---|---|---|---|
[All available weeks. First week = "N/A" for WoW. Show delta as +X% or -X%.]
[Calculate WoW delta: ((this_week - prior_week) / prior_week) * 100]

**MARKET MOMENTUM**
- Strongest growth market: [Market] — [context, e.g., +22% WoW starts]
- Sharpest decline market: [Market] — [context]
- Most stable market: [Market] — [context]
(Skip if market data unavailable — note that briefly)

**TREND DRIVERS**
1. [Primary driver of the current trend] — supporting evidence from the data
2. [Secondary driver if visible in the data]

**OUTLOOK**
If this trend continues for 2 more weeks: [1 sentence forecast]
Recommended action: [specific, actionable recommendation for content or marketing team]

RULES:
- Always compute WoW delta as: ((this_week_value - last_week_value) / last_week_value) * 100
- Round all percentages to 1 decimal place
- Never say "significant" without a number — always quantify
- Max 320 words total
- If only 1 week of data exists, say so clearly and give best available analysis
"""


def _format_trend_data(
    question: str,
    weekly_data: dict,
    market_data: dict,
) -> str:
    """Format weekly and market-level data for the Haiku trend prompt."""
    lines = [f"Question: {question}", ""]

    # ── Weekly time-series ─────────────────────────────────────
    lines.append("── WEEKLY AGGREGATED DATA ──")
    df_weekly = weekly_data.get("data")
    if df_weekly is not None and not df_weekly.empty:
        lines.append(f"Rows: {len(df_weekly)}")
        lines.append(df_weekly.to_string(index=False))
    else:
        lines.append("No weekly time-series data available.")
    lines.append("")

    # ── Market-level breakdown ────────────────────────────────
    lines.append("── MARKET-LEVEL DATA (last 2 weeks) ──")
    df_market = market_data.get("data") if market_data else None
    if df_market is not None and not df_market.empty:
        lines.append(f"Rows: {len(df_market)}")
        lines.append(df_market.to_string(index=False))
    else:
        lines.append("No market-level data available.")
    lines.append("")

    return "\n".join(lines)


class TrendAgent:
    def __init__(self):
        self.client     = anthropic.Anthropic(api_key=os.getenv("ANTHROPIC_API_KEY"))
        self.model      = "claude-haiku-4-5-20251001"
        self.data_agent = DataAgent()

    def _extract_title_hint(self, question: str) -> str:
        """Best-effort extraction of the title name from a Cat C question."""
        q = question
        # Strip common lead phrases (order: longest first to avoid partial matches)
        for prefix in [
            "What is the WoW trend for ", "What is the week-over-week trend for ",
            "What is the trend for ", "Show me the trend for ",
            "What's the momentum of ", "Is there a trend for ",
            "How is ", "Is ",
        ]:
            if q.lower().startswith(prefix.lower()):
                q = q[len(prefix):]
                break
        # Trim at common stop words (do NOT use "wow" alone — avoid matching mid-title)
        for stopper in [
            " gaining", " losing", " trending", " momentum",
            " week over week", " week-over-week", " over the",
            " across", " recovering", " growing", " declining", " in ", "?",
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
        Run WoW trend analysis for a Cat C question.

        Args:
            question     : original user question
            primary_data : DataAgent.fetch() result (used for context)
            on_status    : optional callback(agent, event, detail)

        Returns dict:
            question        — original question
            insight         — full trend analysis markdown text
            trend_direction — GROWING | DECLINING | STABLE | RECOVERING | AT RISK
            weeks_of_data   — number of weekly data points found
            model_used      — model ID
            error           — None on success
        """
        def emit(event, detail=""):
            if on_status:
                on_status("trend_agent", event, detail)

        emit("start")

        title_hint = self._extract_title_hint(question)
        emit("preparing", f"Fetching weekly time-series for '{title_hint}'...")

        # ── Fetch 1: Weekly aggregated starts + completions ───
        weekly_q = (
            f"For the title closest to '{title_hint}', "
            f"show weekly viewership totals grouped by week. "
            f"Use DATE_TRUNC('week', CAST(date AS DATE)) to get the week start date. "
            f"SELECT: week_start, SUM(starts) as total_starts, SUM(completions) as total_completions, "
            f"ROUND(AVG(completion_rate)*100, 1) as avg_completion_pct. "
            f"GROUP BY the DATE_TRUNC expression (not the alias). "
            f"JOIN viewership_daily to titles on title_id. "
            f"Order by week_start ASC. No LIMIT — include all weeks."
        )
        weekly_data = self.data_agent.fetch(weekly_q, verbose=False)
        weeks_count = weekly_data.get("row_count", 0)
        emit("progress", f"Weekly data: {weeks_count} weeks")

        # ── Fetch 2: Market-level WoW (last 2 weeks) ─────────
        market_q = (
            f"For the title closest to '{title_hint}', "
            f"show total starts and avg completion rate by market "
            f"for dates >= '2026-03-17' (last 2 complete weeks). "
            f"JOIN viewership_daily to titles on title_id. "
            f"GROUP BY market, DATE_TRUNC('week', CAST(date AS DATE)) — "
            f"always put the full DATE_TRUNC expression in GROUP BY, not an alias. "
            f"Order by market, week ascending."
        )
        market_data = self.data_agent.fetch(market_q, verbose=False)
        emit("progress", f"Market data: {market_data.get('row_count', 0)} rows")

        # ── LLM: Trend analysis ───────────────────────────────
        formatted = _format_trend_data(question, weekly_data, market_data)
        emit("analysing", f"Computing WoW momentum via Claude Haiku...")

        response = self.client.messages.create(
            model=self.model,
            max_tokens=700,
            system=TREND_SYSTEM,
            messages=[{
                "role": "user",
                "content": (
                    f"Produce a TREND ANALYSIS for this Cat C question:\n\n{formatted}"
                ),
            }],
        )

        insight = response.content[0].text.strip()

        # ── Extract trend direction from response text ────────
        insight_upper   = insight.upper()
        trend_direction = "STABLE"
        if "AT RISK" in insight_upper or "AT-RISK" in insight_upper:
            trend_direction = "AT RISK"
        elif "RECOVERING" in insight_upper:
            trend_direction = "RECOVERING"
        elif "GROWING" in insight_upper:
            trend_direction = "GROWING"
        elif "DECLINING" in insight_upper:
            trend_direction = "DECLINING"

        emit("done", f"{trend_direction} | {weeks_count} weeks of data")

        return {
            "question":        question,
            "insight":         insight,
            "trend_direction": trend_direction,
            "weeks_of_data":   weeks_count,
            "model_used":      self.model,
            "error":           None,
        }


# ─────────────────────────────────────────────────────────────
# Self-test — 3 Cat C questions
# ─────────────────────────────────────────────────────────────
if __name__ == "__main__":
    from rich.console import Console
    from rich.table   import Table
    from rich         import box

    console  = Console()
    data_ag  = DataAgent()
    trend_ag = TrendAgent()

    console.print("\n[bold cyan]Trend Agent Self-Test — WBD APAC[/bold cyan]")
    console.print("[dim]Testing Cat C WoW momentum analysis[/dim]\n")

    tests = [
        "Is The Last of Us S2 gaining or losing momentum week over week?",
        "What is the WoW trend for House of the Dragon S2 across APAC?",
        "Is Euphoria S2 recovering or still declining in viewership?",
    ]

    tbl = Table(box=box.SIMPLE, show_header=True, header_style="bold white")
    tbl.add_column("Question", width=56)
    tbl.add_column("Trend", width=12)
    tbl.add_column("Weeks", width=6, justify="right")
    tbl.add_column("Status", width=8)

    passed = 0
    for q in tests:
        console.print(f"[dim]Running:[/dim] [white]{q[:70]}[/white]")
        primary = data_ag.fetch(q, verbose=False)
        result  = trend_ag.analyse(q, primary)
        ok      = result["error"] is None
        trend   = result.get("trend_direction", "?")
        weeks   = str(result.get("weeks_of_data", 0))
        status  = "[green]✅ pass[/green]" if ok else "[red]❌ fail[/red]"
        tbl.add_row(q[:56], trend, weeks, status)
        if ok:
            passed += 1
            console.print(f"  [dim]Preview: {result['insight'][:130]}...[/dim]\n")
        else:
            console.print(f"  [red]Error: {result['error']}[/red]\n")

    console.print(tbl)
    console.print(f"\n[bold]Result: {passed}/{len(tests)} passed[/bold]")
    if passed == len(tests):
        console.print("[bold green]✅ Trend Agent fully operational.[/bold green]\n")
    else:
        console.print(f"[bold yellow]⚠️  {len(tests)-passed} tests failed.[/bold yellow]\n")
