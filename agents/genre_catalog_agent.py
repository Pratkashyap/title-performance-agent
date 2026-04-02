"""
agents/genre_catalog_agent.py
WBD APAC — Title Performance Analyst

Platform-wide and genre-level analysis engine. Handles:
  Category D — Genre health, catalog scoring, engagement gaps,
  top/bottom performers, format/language distribution.

Runs as the sole specialist for Cat D questions. Fetches three
dedicated data sets via the DataAgent and produces a structured
genre health + catalog report.

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
# GENRE & CATALOG SYSTEM PROMPT
# ─────────────────────────────────────────────────────────────

GENRE_CATALOG_SYSTEM = """You are the Genre & Catalog Agent for WBD APAC's Title Performance Analyst system.
Your job: analyse platform-wide catalog health, genre performance, and surface engagement gaps.

Today's date: 2026-03-31
Region: APAC — 10 markets: AU, SG, HK, IN, JP, KR, TW, TH, PH, MY
Time window: last 30 days unless the question specifies otherwise.

Genres on platform: Drama, Fantasy, Sci-Fi, Crime, Thriller, Horror, Comedy, Documentary, K-Drama, Action

GENRE HEALTH SCORING:
  Strong   🟢 : avg completion rate >= 65%
  Moderate 🟡 : avg completion rate 50%–64%
  Weak     🔴 : avg completion rate < 50%

ENGAGEMENT GAP definition:
  Titles where starts are ABOVE the catalog average BUT completion_rate < 50%.
  = High viewer intent but poor content satisfaction — a content/quality risk.

You receive:
1. GENRE DATA      — genre-level aggregates (avg starts, completion rate, title count)
2. ENGAGEMENT GAP  — titles with above-average starts but completion rate < 50%
3. TOP PERFORMERS  — titles ranked by total starts for the period

Structure your response exactly as:

**CATALOG & GENRE ANALYSIS**

**GENRE HEALTH SCOREBOARD**
| Genre | Avg Daily Starts | Avg Completion % | Titles | Health |
|---|---|---|---|---|
[All genres with data. Sort by avg completion rate DESC. Add 🟢/🟡/🔴 health indicator.]

**ENGAGEMENT GAP TITLES** ⚠️
These titles attract viewers but struggle to keep them:
| Title | Genre | Avg Daily Starts | Completion % |
|---|---|---|---|
[Up to 5 titles, sorted by avg_daily_starts DESC.]
If no engagement gap titles exist, write: "No engagement gaps detected this period."

**TOP PERFORMERS THIS MONTH** 🏆
| Rank | Title | Genre | Total Starts | Completion % |
|---|---|---|---|---|
[Top 5 by total starts.]

**CATALOG INSIGHT**
Most important pattern: [1 sentence summarising the biggest finding]
Recommended action: [1 specific, actionable recommendation for the content or marketing team]

RULES:
- Always show completion rates as percentages (e.g. 74.3%, not 0.743)
- Round all numbers to 1 decimal place
- Never say "significant" without a number — always quantify
- Max 380 words total
- If a section has no data, acknowledge briefly and move on
- Tailor the insight to the specific question asked
"""


def _format_catalog_data(
    question: str,
    genre_data: dict,
    gap_data: dict,
    top_data: dict,
) -> str:
    """Format all three data sources into a clean text block for the Haiku prompt."""
    lines = [f"Question: {question}", ""]

    # ── Genre-level aggregates ─────────────────────────────────
    lines.append("── GENRE-LEVEL AGGREGATES (last 30 days) ──")
    df_genre = genre_data.get("data")
    if df_genre is not None and not df_genre.empty:
        lines.append(f"Rows: {len(df_genre)}")
        lines.append(df_genre.to_string(index=False))
    else:
        lines.append("No genre data available.")
    lines.append("")

    # ── Engagement gap titles ─────────────────────────────────
    lines.append("── ENGAGEMENT GAP TITLES (high starts, low completion) ──")
    df_gap = gap_data.get("data") if gap_data else None
    if df_gap is not None and not df_gap.empty:
        lines.append(f"Rows: {len(df_gap)}")
        lines.append(df_gap.to_string(index=False))
    else:
        lines.append("No engagement gap titles found.")
    lines.append("")

    # ── Top performers ─────────────────────────────────────────
    lines.append("── TOP PERFORMERS BY TOTAL STARTS (last 30 days) ──")
    df_top = top_data.get("data") if top_data else None
    if df_top is not None and not df_top.empty:
        lines.append(f"Rows: {len(df_top)}")
        lines.append(df_top.to_string(index=False))
    else:
        lines.append("No top performer data available.")
    lines.append("")

    return "\n".join(lines)


class GenreCatalogAgent:
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
        Run genre & catalog analysis for a Cat D question.

        Args:
            question     : original user question
            primary_data : DataAgent.fetch() result (used for initial context)
            on_status    : optional callback(agent, event, detail)

        Returns dict:
            question         — original question
            insight          — full catalog analysis markdown text
            genres_analysed  — number of genres found in data
            model_used       — model ID
            error            — None on success
        """
        def emit(event, detail=""):
            if on_status:
                on_status("genre_catalog_agent", event, detail)

        emit("start")
        emit("preparing", "Fetching genre health + engagement gaps + top performers...")

        # ── Fetch 1: Genre-level aggregates ──────────────────
        genre_q = (
            "Show genre-level performance for the last 30 days (date >= '2026-03-01'). "
            "SELECT t.genre, "
            "COUNT(DISTINCT t.title_id) as title_count, "
            "ROUND(AVG(v.starts), 1) as avg_daily_starts, "
            "ROUND(AVG(v.completion_rate) * 100, 1) as avg_completion_pct, "
            "SUM(v.starts) as total_starts. "
            "FROM viewership_daily v JOIN titles t ON v.title_id = t.title_id "
            "WHERE v.date >= '2026-03-01'. "
            "GROUP BY t.genre. "
            "ORDER BY avg_daily_starts DESC. "
            "No LIMIT — return all genres."
        )
        genre_data = self.data_agent.fetch(genre_q, verbose=False)
        genres_count = genre_data.get("row_count", 0)
        emit("progress", f"Genre data: {genres_count} genres")

        # ── Fetch 2: Engagement gap — high starts, low completion ─
        gap_q = (
            "Find titles with an engagement gap over the last 30 days (date >= '2026-03-01'): "
            "titles where average starts per day are above the catalog-wide average "
            "AND average completion_rate < 0.50. "
            "SELECT t.title_name, t.genre, "
            "ROUND(AVG(v.starts), 1) as avg_daily_starts, "
            "ROUND(AVG(v.completion_rate) * 100, 1) as avg_completion_pct, "
            "SUM(v.starts) as total_starts. "
            "FROM viewership_daily v JOIN titles t ON v.title_id = t.title_id "
            "WHERE v.date >= '2026-03-01'. "
            "GROUP BY t.title_name, t.genre. "
            "HAVING AVG(v.starts) > "
            "(SELECT AVG(starts) FROM viewership_daily WHERE date >= '2026-03-01') "
            "AND AVG(v.completion_rate) < 0.50. "
            "ORDER BY avg_daily_starts DESC. "
            "LIMIT 10."
        )
        gap_data = self.data_agent.fetch(gap_q, verbose=False)
        emit("progress", f"Engagement gaps: {gap_data.get('row_count', 0)} titles")

        # ── Fetch 3: Top performers by total starts ───────────
        top_q = (
            "Show the top 10 titles by total starts in the last 30 days (date >= '2026-03-01'). "
            "SELECT t.title_name, t.genre, t.format, "
            "SUM(v.starts) as total_starts, "
            "ROUND(AVG(v.completion_rate) * 100, 1) as avg_completion_pct, "
            "SUM(v.unique_viewers) as total_unique_viewers. "
            "FROM viewership_daily v JOIN titles t ON v.title_id = t.title_id "
            "WHERE v.date >= '2026-03-01'. "
            "GROUP BY t.title_name, t.genre, t.format. "
            "ORDER BY total_starts DESC. "
            "LIMIT 10."
        )
        top_data = self.data_agent.fetch(top_q, verbose=False)
        emit("progress", f"Top performers: {top_data.get('row_count', 0)} titles")

        # ── LLM: Catalog analysis ─────────────────────────────
        formatted = _format_catalog_data(question, genre_data, gap_data, top_data)
        emit("analysing", "Computing genre health via Claude Haiku...")

        response = self.client.messages.create(
            model=self.model,
            max_tokens=800,
            system=GENRE_CATALOG_SYSTEM,
            messages=[{
                "role": "user",
                "content": (
                    f"Produce a CATALOG & GENRE ANALYSIS for this Cat D question:\n\n{formatted}"
                ),
            }],
        )

        insight = response.content[0].text.strip()
        emit("done", f"{genres_count} genres analysed")

        return {
            "question":        question,
            "insight":         insight,
            "genres_analysed": genres_count,
            "model_used":      self.model,
            "error":           None,
        }


# ─────────────────────────────────────────────────────────────
# Self-test — 3 Cat D questions
# ─────────────────────────────────────────────────────────────
if __name__ == "__main__":
    from rich.console import Console
    from rich.table   import Table
    from rich         import box

    console    = Console()
    data_ag    = DataAgent()
    catalog_ag = GenreCatalogAgent()

    console.print("\n[bold cyan]Genre & Catalog Agent Self-Test — WBD APAC[/bold cyan]")
    console.print("[dim]Testing Cat D genre health + catalog analysis[/dim]\n")

    tests = [
        "Which titles have high starts but low completions right now?",
        "Which genre is overperforming on Max APAC this month?",
        "Give me a full catalog health report for APAC this month.",
    ]

    tbl = Table(box=box.SIMPLE, show_header=True, header_style="bold white")
    tbl.add_column("Question", width=56)
    tbl.add_column("Genres", width=7, justify="right")
    tbl.add_column("Words", width=6, justify="right")
    tbl.add_column("Status", width=8)

    passed = 0
    for q in tests:
        console.print(f"[dim]Running:[/dim] [white]{q[:70]}[/white]")
        primary = data_ag.fetch(q, verbose=False)
        result  = catalog_ag.analyse(q, primary)
        ok      = result["error"] is None
        words   = str(len(result["insight"].split())) if ok else "—"
        genres  = str(result.get("genres_analysed", 0))
        status  = "[green]✅ pass[/green]" if ok else "[red]❌ fail[/red]"
        tbl.add_row(q[:56], genres, words, status)
        if ok:
            passed += 1
            console.print(f"  [dim]Preview: {result['insight'][:130]}...[/dim]\n")
        else:
            console.print(f"  [red]Error: {result['error']}[/red]\n")

    console.print(tbl)
    console.print(f"\n[bold]Result: {passed}/{len(tests)} passed[/bold]")
    if passed == len(tests):
        console.print("[bold green]✅ Genre & Catalog Agent fully operational.[/bold green]\n")
    else:
        console.print(f"[bold yellow]⚠️  {len(tests)-passed} tests failed.[/bold yellow]\n")
