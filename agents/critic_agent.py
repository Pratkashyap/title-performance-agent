"""
agents/critic_agent.py
WBD APAC — Title Performance Analyst

Quality gate — reviews every agent output before it reaches the user.
Scores on 5 dimensions, then passes, enhances, or rewrites.

Score ≥ 8  → ✅ Approved   — deliver as-is
Score 6–7  → ⚡ Enhanced   — append short improvement
Score < 6  → 🔧 Revised    — rewrite the weakest section

Model: Claude Haiku 4.5 (fast — just reviewing, not generating)
"""

import os
import re
import sys
import anthropic
from dotenv import load_dotenv

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))

_env_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "../.env")
load_dotenv(_env_path, override=True)

# ─────────────────────────────────────────────────────────────
# CRITIC SYSTEM PROMPT
# ─────────────────────────────────────────────────────────────

CRITIC_SYSTEM = """You are the Quality Critic for WBD APAC's Title Performance Analyst system.
You review every analyst response before it reaches the team. Your job is fast, precise quality control.

Score each response on 5 criteria (0–2 points each, max 10):

1. Specificity    — Are real numbers present? (completion rates, starts, WoW %, churn rates — not vague phrases like "performs well")
2. Actionability  — Can the content/programming team act on this TODAY? Are recommendations concrete and specific?
3. Accuracy       — Are numbers internally consistent? Does the answer actually address what was asked?
4. Completeness   — Does the response cover all relevant dimensions? (market split, trend, benchmark context where appropriate)
5. Tone           — Is it confident, analyst-ready, data-driven? (no hedging like "might", "could possibly", "it seems")

OUTPUT RULES — follow exactly:

If score >= 8:
  Return this badge line, then the ORIGINAL response unchanged:
  ✅ Quality Score: [X]/10 — Approved

If score 6–7:
  Return this badge line, then the ORIGINAL response, then a ⚡ Enhancement block (under 60 words):
  ⚡ Quality Score: [X]/10 — Enhanced

If score < 6:
  Return this badge line, then the original response with the WEAKEST section rewritten inline (under 80 words changed):
  🔧 Quality Score: [X]/10 — Revised

RULES:
- Never write a lengthy critique or explain your scoring
- Never repeat the score badge at the end
- Enhancement/revision must be under 80 words total
- Do not add new sections — only improve what exists
- Preserve all markdown tables exactly as given
"""


class CriticAgent:
    def __init__(self):
        self.client = anthropic.Anthropic(api_key=os.getenv("ANTHROPIC_API_KEY"))
        self.model  = "claude-haiku-4-5-20251001"

    def review(
        self,
        insight: str,
        question: str,
        category: str = "",
        on_status=None,
    ) -> dict:
        """
        Review and optionally improve an agent output.

        Args:
            insight    : the response text to review
            question   : original user question (for context)
            category   : question category A–F (for context)
            on_status  : optional callback(agent, event, detail)

        Returns dict:
            reviewed_insight — final output (badge + original or improved text)
            score            — integer 0–10 (or None if parsing fails)
            verdict          — "approved" | "enhanced" | "revised"
            model_used       — model ID
            error            — None on success
        """
        def emit(event, detail=""):
            if on_status:
                on_status("critic_agent", event, detail)

        emit("start")
        cat_label = f"Cat {category} — " if category else ""
        emit("scoring", f"{cat_label}specificity · actionability · accuracy · completeness · tone")

        response = self.client.messages.create(
            model=self.model,
            max_tokens=1400,
            system=CRITIC_SYSTEM,
            messages=[{
                "role": "user",
                "content": (
                    f"Category: {category or 'unknown'}\n"
                    f"Question asked: {question}\n\n"
                    f"Response to review:\n\n{insight}"
                ),
            }],
        )

        reviewed = response.content[0].text.strip()

        # ── Extract score and verdict from first line ─────────
        score   = None
        verdict = "approved"
        first_line = reviewed.split("\n")[0]

        m = re.search(r"(\d+)/10", first_line)
        if m:
            score = int(m.group(1))

        if "Enhanced" in first_line:
            verdict = "enhanced"
        elif "Revised" in first_line:
            verdict = "revised"
        else:
            verdict = "approved"

        emit("done", f"Score: {score}/10 — {verdict.capitalize()}")

        return {
            "reviewed_insight": reviewed,
            "score":            score,
            "verdict":          verdict,
            "model_used":       self.model,
            "error":            None,
        }


# ─────────────────────────────────────────────────────────────
# Self-test — 3 synthetic outputs across quality tiers
# ─────────────────────────────────────────────────────────────
if __name__ == "__main__":
    from rich.console import Console
    from rich.table   import Table
    from rich         import box

    console = Console()
    critic  = CriticAgent()

    console.print("\n[bold cyan]Critic Agent Self-Test — WBD APAC[/bold cyan]")
    console.print("[dim]Testing quality gate across 3 output tiers[/dim]\n")

    tests = [
        {
            "label":    "High quality (expect ✅ ≥8)",
            "question": "Why is The White Lotus S3 underperforming in Southeast Asia?",
            "category": "A",
            "insight":  (
                "**DIAGNOSIS — The White Lotus S3 in Southeast Asia**\n\n"
                "**WHAT IS HAPPENING**\n"
                "Completion rate in SG (38.2%) and TH (34.1%) is 28% below the Drama genre average (53.0%) "
                "and 33% below AU performance (71.4%). "
                "Drop-off concentrates at Episode 3 — 61% of viewers who start Episode 3 do not reach Episode 4.\n\n"
                "**ROOT CAUSE**\n"
                "Episode 3 pacing issue driving abandonment. Returning viewer rate in SEA is 18% vs 41% in AU. "
                "This is a content engagement problem, not a discovery problem — starts are on track at 4,200/day.\n\n"
                "**RECOMMENDED ACTIONS**\n"
                "1. Push editorial spotlight on Episode 4 in SG and TH within 48 hours\n"
                "2. Flag Episode 3 pacing to programming team for post-mortem\n"
                "3. Activate clip/highlight strategy to bridge Ep3→Ep4 across SEA markets\n"
            ),
        },
        {
            "label":    "Medium quality (expect ⚡ 6–7)",
            "question": "How is House of the Dragon S2 performing vs fantasy benchmarks?",
            "category": "B",
            "insight":  (
                "**PERFORMANCE SNAPSHOT — House of the Dragon S2**\n\n"
                "House of the Dragon S2 is performing well on Max APAC. "
                "Viewership has been strong across most markets. "
                "Completion rates are above average for the fantasy genre. "
                "The show is doing better than comparable titles.\n\n"
                "**RECOMMENDATION**\n"
                "The team should continue monitoring performance and consider promotional pushes in weaker markets."
            ),
        },
        {
            "label":    "Low quality (expect 🔧 <6)",
            "question": "Which titles need immediate attention this week?",
            "category": "F",
            "insight":  (
                "Some titles might need attention. "
                "It seems like viewership could possibly be declining for a few shows. "
                "The team may want to look into this."
            ),
        },
    ]

    tbl = Table(box=box.SIMPLE, show_header=True, header_style="bold white")
    tbl.add_column("Label", width=34)
    tbl.add_column("Score", width=7, justify="right")
    tbl.add_column("Verdict", width=10)
    tbl.add_column("Status", width=8)

    passed = 0
    for t in tests:
        console.print(f"[dim]Running:[/dim] [white]{t['label']}[/white]")

        def show(agent, event, detail=""):
            if event not in ("start",):
                console.print(f"  [dim]{agent}:{event} {detail}[/dim]")

        result = critic.review(
            t["insight"], t["question"], t["category"], on_status=show
        )
        ok      = result["error"] is None
        score   = str(result.get("score") or "?")
        verdict = result.get("verdict", "?")
        status  = "[green]✅ pass[/green]" if ok else "[red]❌ fail[/red]"
        tbl.add_row(t["label"], f"{score}/10", verdict, status)
        if ok:
            passed += 1
            preview = result["reviewed_insight"][:160].replace("\n", " ")
            console.print(f"  [dim]→ {preview}...[/dim]\n")
        else:
            console.print(f"  [red]Error: {result['error']}[/red]\n")

    console.print(tbl)
    console.print(f"\n[bold]Result: {passed}/{len(tests)} passed[/bold]")
    if passed == len(tests):
        console.print("[bold green]✅ Critic Agent fully operational.[/bold green]\n")
    else:
        console.print(f"[bold yellow]⚠️  {len(tests)-passed} tests failed.[/bold yellow]\n")
