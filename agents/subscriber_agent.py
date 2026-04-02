"""
agents/subscriber_agent.py
WBD APAC — Title Performance Analyst

Links viewing behaviour to subscriber outcomes. Handles:
  Category E — segment analysis, churn correlation, plan-type
  breakdowns, acquisition signals, retention indicators.

Runs as the sole specialist for Cat E questions. Fetches three
dedicated data sets via the DataAgent and produces a structured
subscriber behaviour report.

Model: Claude Sonnet 4.6 (deep reasoning for behaviour patterns)
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
# SUBSCRIBER BEHAVIOUR SYSTEM PROMPT
# ─────────────────────────────────────────────────────────────

SUBSCRIBER_SYSTEM = """You are the Subscriber Behaviour Agent for WBD APAC's Title Performance Analyst system.
Your job: connect viewing patterns to subscriber outcomes — churn risk, loyalty signals, segment behaviour, and retention.

Today's date: 2026-03-31
Region: APAC — 10 markets: AU, SG, HK, IN, JP, KR, TW, TH, PH, MY
Time window: last 30 days unless the question specifies otherwise.

Subscriber segments on platform: casual, regular, binge, loyal
Plan types: basic, standard, premium
Churn flag: is_churned = true means the subscriber cancelled within the analysis window.

CHURN SIGNAL INTERPRETATION:
  Churn rate < 5%  🟢 Strong retention signal — title drives loyalty
  Churn rate 5–12% 🟡 Neutral — no strong signal either way
  Churn rate > 12% 🔴 Risk signal — title may not be retaining subscribers

ENGAGEMENT DEPTH definition:
  avg_watch_time_mins >= 45  = deep engagement (≥ 1 episode equivalent)
  avg_watch_time_mins 20–44  = moderate engagement
  avg_watch_time_mins < 20   = shallow engagement (browsed, didn't commit)

You receive:
1. SEGMENT DATA    — subscriber segments watching the title(s) in question
2. CHURN DATA      — churn rates by segment and plan type for viewers of the title
3. COMPLETION DATA — completion rates and watch depth by subscriber type

Structure your response exactly as:

**SUBSCRIBER BEHAVIOUR ANALYSIS**

**WHO WATCHES THIS TITLE**
| Segment | Viewers | Avg Watch Time (mins) | Completion % | Engagement Depth |
|---|---|---|---|---|
[All segments with data. Sort by viewer count DESC. Add engagement depth label.]

**CHURN SIGNAL** 📊
| Segment | Plan Type | Viewers | Churn Rate | Signal |
|---|---|---|---|---|
[Breakdown by segment + plan type. Sort by churn rate ASC (best signal first). Add 🟢/🟡/🔴 signal.]

**KEY INSIGHT**
Retention finding: [1 sentence — does watching this title correlate with staying or leaving?]
Best audience segment: [name the segment + plan type that shows the strongest retention, with numbers]
At-risk group: [segment + plan type with worst churn rate, if > 12%; otherwise write "None identified"]

**RECOMMENDATION**
[1–2 sentences. Specific action for content, marketing, or subscriber team based on the data.]

RULES:
- Always show churn rates as percentages (e.g. 8.3%, not 0.083)
- Always show completion rates as percentages (e.g. 74.3%, not 0.743)
- Round all numbers to 1 decimal place
- Never say "significant" without a number — always quantify
- Max 400 words total
- If a section has no data, acknowledge briefly and move on
- Tailor the recommendation to the specific question and title(s) asked about
"""


def _extract_title_hint(question: str) -> str:
    """Pull a title name hint from the question for targeted SQL queries."""
    q_lower = question.lower()
    known_titles = [
        "house of the dragon", "hotd", "white lotus", "the last of us", "tlou",
        "succession", "dune", "barbie", "euphoria", "industry", "the idol",
        "the nevers", "aquaman", "the flash", "true blood", "mare of easttown",
        "the undoing", "we own this city", "expats", "the sympathizer",
        "pachinko", "tokyo vice", "the jinx", "curb", "his dark materials",
    ]
    for title in known_titles:
        if title in q_lower:
            return title
    return ""


def _format_subscriber_data(
    question: str,
    segment_data: dict,
    churn_data: dict,
    completion_data: dict,
) -> str:
    """Format all three data sources into a clean text block for the Sonnet prompt."""
    lines = [f"Question: {question}", ""]

    # ── Segment breakdown ──────────────────────────────────────
    lines.append("── SUBSCRIBER SEGMENT DATA (who watches, how much) ──")
    df_seg = segment_data.get("data")
    if df_seg is not None and not df_seg.empty:
        lines.append(f"Rows: {len(df_seg)}")
        lines.append(df_seg.to_string(index=False))
    else:
        lines.append("No segment data available.")
    lines.append("")

    # ── Churn correlation ─────────────────────────────────────
    lines.append("── CHURN DATA (retention signal by segment + plan type) ──")
    df_churn = churn_data.get("data") if churn_data else None
    if df_churn is not None and not df_churn.empty:
        lines.append(f"Rows: {len(df_churn)}")
        lines.append(df_churn.to_string(index=False))
    else:
        lines.append("No churn data available.")
    lines.append("")

    # ── Completion depth ──────────────────────────────────────
    lines.append("── COMPLETION & WATCH DEPTH BY PLAN TYPE ──")
    df_comp = completion_data.get("data") if completion_data else None
    if df_comp is not None and not df_comp.empty:
        lines.append(f"Rows: {len(df_comp)}")
        lines.append(df_comp.to_string(index=False))
    else:
        lines.append("No completion data available.")
    lines.append("")

    return "\n".join(lines)


class SubscriberAgent:
    def __init__(self):
        self.client     = anthropic.Anthropic(api_key=os.getenv("ANTHROPIC_API_KEY"))
        self.model      = "claude-sonnet-4-6"
        self.data_agent = DataAgent()

    def analyse(
        self,
        question: str,
        primary_data: dict,
        on_status=None,
    ) -> dict:
        """
        Run subscriber behaviour analysis for a Cat E question.

        Args:
            question     : original user question
            primary_data : DataAgent.fetch() result (initial context)
            on_status    : optional callback(agent, event, detail)

        Returns dict:
            question          — original question
            insight           — full subscriber analysis markdown text
            segments_analysed — number of segment rows found
            model_used        — model ID
            error             — None on success
        """
        def emit(event, detail=""):
            if on_status:
                on_status("subscriber_agent", event, detail)

        emit("start")
        emit("preparing", "Fetching segment data + churn signals + completion depth...")

        title_hint = _extract_title_hint(question)
        title_filter = f"AND LOWER(t.title_name) LIKE '%{title_hint}%'" if title_hint else ""

        # ── Fetch 1: Segment breakdown ────────────────────────
        segment_q = (
            f"Show subscriber segment breakdown for viewers over the last 30 days "
            f"(date >= '2026-03-01'). "
            f"SELECT t.title_name, sv.subscriber_segment, "
            f"COUNT(DISTINCT sv.subscriber_id) as unique_viewers, "
            f"ROUND(AVG(sv.watch_time_mins), 1) as avg_watch_time_mins, "
            f"ROUND(AVG(CAST(sv.completed AS INTEGER)) * 100, 1) as completion_pct. "
            f"FROM subscriber_viewing sv JOIN titles t ON sv.title_id = t.title_id "
            f"WHERE sv.date >= '2026-03-01' {title_filter}. "
            f"GROUP BY t.title_name, sv.subscriber_segment. "
            f"ORDER BY unique_viewers DESC. "
            f"LIMIT 20."
        )
        segment_data = self.data_agent.fetch(segment_q, verbose=False)
        segments_count = segment_data.get("row_count", 0)
        emit("progress", f"Segment data: {segments_count} rows")

        # ── Fetch 2: Churn correlation by segment + plan type ─
        churn_q = (
            f"Show churn rates for subscribers who watched title(s) over the last 30 days "
            f"(date >= '2026-03-01'), broken down by segment and plan type. "
            f"SELECT t.title_name, sv.subscriber_segment, sv.plan_type, "
            f"COUNT(DISTINCT sv.subscriber_id) as viewers, "
            f"SUM(CAST(sv.is_churned AS INTEGER)) as churned_count, "
            f"ROUND(AVG(CAST(sv.is_churned AS INTEGER)) * 100, 1) as churn_rate_pct. "
            f"FROM subscriber_viewing sv JOIN titles t ON sv.title_id = t.title_id "
            f"WHERE sv.date >= '2026-03-01' {title_filter}. "
            f"GROUP BY t.title_name, sv.subscriber_segment, sv.plan_type. "
            f"HAVING COUNT(DISTINCT sv.subscriber_id) >= 5. "
            f"ORDER BY churn_rate_pct ASC. "
            f"LIMIT 20."
        )
        churn_data = self.data_agent.fetch(churn_q, verbose=False)
        emit("progress", f"Churn data: {churn_data.get('row_count', 0)} rows")

        # ── Fetch 3: Completion depth by plan type ────────────
        completion_q = (
            f"Show watch depth and completion rates by plan type for subscribers "
            f"over the last 30 days (date >= '2026-03-01'). "
            f"SELECT t.title_name, sv.plan_type, "
            f"COUNT(DISTINCT sv.subscriber_id) as viewers, "
            f"ROUND(AVG(sv.watch_time_mins), 1) as avg_watch_time_mins, "
            f"ROUND(AVG(CAST(sv.completed AS INTEGER)) * 100, 1) as completion_pct, "
            f"SUM(CAST(sv.is_churned AS INTEGER)) as churned, "
            f"ROUND(AVG(CAST(sv.is_churned AS INTEGER)) * 100, 1) as churn_rate_pct. "
            f"FROM subscriber_viewing sv JOIN titles t ON sv.title_id = t.title_id "
            f"WHERE sv.date >= '2026-03-01' {title_filter}. "
            f"GROUP BY t.title_name, sv.plan_type. "
            f"ORDER BY viewers DESC. "
            f"LIMIT 15."
        )
        completion_data = self.data_agent.fetch(completion_q, verbose=False)
        emit("progress", f"Completion data: {completion_data.get('row_count', 0)} rows")

        # ── LLM: Subscriber behaviour analysis ───────────────
        formatted = _format_subscriber_data(question, segment_data, churn_data, completion_data)
        emit("analysing", "Synthesising subscriber behaviour via Claude Sonnet...")

        response = self.client.messages.create(
            model=self.model,
            max_tokens=900,
            system=SUBSCRIBER_SYSTEM,
            messages=[{
                "role": "user",
                "content": (
                    f"Produce a SUBSCRIBER BEHAVIOUR ANALYSIS for this Cat E question:\n\n{formatted}"
                ),
            }],
        )

        insight = response.content[0].text.strip()
        emit("done", f"{segments_count} segment rows analysed")

        return {
            "question":          question,
            "insight":           insight,
            "segments_analysed": segments_count,
            "model_used":        self.model,
            "error":             None,
        }


# ─────────────────────────────────────────────────────────────
# Self-test — 3 Cat E questions
# ─────────────────────────────────────────────────────────────
if __name__ == "__main__":
    from rich.console import Console
    from rich.table   import Table
    from rich         import box

    console   = Console()
    data_ag   = DataAgent()
    sub_ag    = SubscriberAgent()

    console.print("\n[bold cyan]Subscriber Agent Self-Test — WBD APAC[/bold cyan]")
    console.print("[dim]Testing Cat E segment + churn + completion analysis[/dim]\n")

    tests = [
        "Does watching House of the Dragon reduce churn risk?",
        "What subscriber segments watch The White Lotus the most?",
        "Which plan type has the best retention among subscribers who watch HBO Originals?",
    ]

    tbl = Table(box=box.SIMPLE, show_header=True, header_style="bold white")
    tbl.add_column("Question", width=56)
    tbl.add_column("Seg rows", width=8, justify="right")
    tbl.add_column("Words", width=6, justify="right")
    tbl.add_column("Status", width=8)

    passed = 0
    for q in tests:
        console.print(f"[dim]Running:[/dim] [white]{q[:70]}[/white]")
        primary = data_ag.fetch(q, verbose=False)

        def show(agent, event, detail=""):
            if event not in ("start", "done"):
                console.print(f"  [dim]{agent}:{event} {detail}[/dim]")

        result  = sub_ag.analyse(q, primary, on_status=show)
        ok      = result["error"] is None
        words   = str(len(result["insight"].split())) if ok else "—"
        segs    = str(result.get("segments_analysed", 0))
        status  = "[green]✅ pass[/green]" if ok else "[red]❌ fail[/red]"
        tbl.add_row(q[:56], segs, words, status)
        if ok:
            passed += 1
            console.print(f"  [dim]Preview: {result['insight'][:150]}...[/dim]\n")
        else:
            console.print(f"  [red]Error: {result['error']}[/red]\n")

    console.print(tbl)
    console.print(f"\n[bold]Result: {passed}/{len(tests)} passed[/bold]")
    if passed == len(tests):
        console.print("[bold green]✅ Subscriber Agent fully operational.[/bold green]\n")
    else:
        console.print(f"[bold yellow]⚠️  {len(tests)-passed} tests failed.[/bold yellow]\n")
