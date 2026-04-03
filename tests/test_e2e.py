"""
tests/test_e2e.py
WBD APAC — Title Performance Analyst

Phase 9 — End-to-end test suite.
Runs 5 canonical questions through the full pipeline:
  Orchestrator → Data Agent → Specialist (A–F) → Quality Critic

Pass criteria per test:
  1. No pipeline error
  2. Correct category classification
  3. Response length ≥ 150 chars
  4. Quality score present (critic ran and extracted a score)
  5. Elapsed time ≤ 90 seconds

Run:
    cd ~/Desktop/title-performance-agent
    source venv/bin/activate
    python3 tests/test_e2e.py
"""

import os
import re
import sys
import time

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))

from rich.console import Console
from rich.table   import Table
from rich.panel   import Panel
from rich         import box

from agents.orchestrator import Orchestrator

console = Console()

# ─────────────────────────────────────────────────────────────
# TEST CASES
# Each: (expected_category, question, description)
# ─────────────────────────────────────────────────────────────
TEST_CASES = [
    (
        "A",
        "Why is Euphoria S2 underperforming in Southeast Asia?",
        "Cat A — Diagnosis: root cause analysis for underperforming title in SEA markets",
    ),
    (
        "B",
        "How is House of the Dragon S2 performing vs comparable fantasy titles?",
        "Cat B — Snapshot: performance benchmark with peer group comparison",
    ),
    (
        "C",
        "Is The Last of Us S2 gaining or losing momentum week over week?",
        "Cat C — Trend: WoW momentum analysis across APAC",
    ),
    (
        "E",
        "Does watching House of the Dragon reduce churn risk for subscribers?",
        "Cat E — Subscriber: churn signal + segment breakdown",
    ),
    (
        "F",
        "Which titles need immediate attention this week?",
        "Cat F — Alerts: platform-wide proactive monitoring bulletin",
    ),
]

PASS_MIN_CHARS  = 150
PASS_MAX_SECS   = 90


# ─────────────────────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────────────────────

def extract_score(response: str):
    """Pull quality score from critic badge on first line."""
    if not response:
        return None
    m = re.search(r"(\d+)/10", response.split("\n")[0])
    return int(m.group(1)) if m else None


def extract_verdict(response: str) -> str:
    first = response.split("\n")[0] if response else ""
    if "Enhanced" in first:
        return "enhanced"
    if "Revised" in first:
        return "revised"
    if re.search(r"\d+/10", first):
        return "approved"
    return "none"


def run_test(orch: Orchestrator, expected_cat: str, question: str) -> dict:
    """Run one test case and return a result dict."""
    events = []

    def on_status(agent, event, detail=""):
        events.append((agent, event, detail))

    t0      = time.time()
    result  = orch.run(question, on_status=on_status)
    elapsed = round(time.time() - t0, 1)

    response = result.get("response", "")
    actual_cat = result.get("category", "?")
    error      = result.get("error")
    score      = extract_score(response)
    verdict    = extract_verdict(response)

    # ── Evaluate pass criteria ────────────────────────────────
    checks = {
        "no_error":      error is None,
        "correct_cat":   actual_cat == expected_cat,
        "response_len":  len(response) >= PASS_MIN_CHARS,
        "score_present": score is not None,
        "within_time":   elapsed <= PASS_MAX_SECS,
    }
    passed = all(checks.values())

    return {
        "question":     question,
        "expected_cat": expected_cat,
        "actual_cat":   actual_cat,
        "passed":       passed,
        "checks":       checks,
        "score":        score,
        "verdict":      verdict,
        "elapsed":      elapsed,
        "response_len": len(response),
        "error":        error,
        "response":     response,
        "events":       events,
    }


# ─────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────

def main():
    console.print()
    console.print(Panel.fit(
        "[bold white]WBD APAC — Title Performance Analyst[/bold white]\n"
        "[dim]Phase 9 · End-to-End Test Suite · 5 canonical test cases[/dim]",
        border_style="cyan",
    ))
    console.print()

    orch    = Orchestrator()
    results = []
    total   = len(TEST_CASES)

    for i, (expected_cat, question, description) in enumerate(TEST_CASES, 1):
        console.print(f"[dim]Test {i}/{total}:[/dim] [white]{description}[/white]")
        console.print(f"[dim]  Q: {question}[/dim]")

        r = run_test(orch, expected_cat, question)
        results.append(r)

        # inline status
        status = "[bold green]PASS[/bold green]" if r["passed"] else "[bold red]FAIL[/bold red]"
        cat_ok = "✓" if r["checks"]["correct_cat"] else f"✗ (got {r['actual_cat']})"
        console.print(
            f"  {status} · Cat {r['expected_cat']} {cat_ok} · "
            f"Score {r['score']}/10 · {r['verdict'].upper()} · "
            f"{r['response_len']} chars · {r['elapsed']}s"
        )
        if r["error"]:
            console.print(f"  [red]Error: {r['error']}[/red]")
        console.print()

    # ── Summary table ─────────────────────────────────────────
    tbl = Table(box=box.SIMPLE_HEAVY, show_header=True, header_style="bold white",
                title="[bold cyan]Test Results[/bold cyan]")
    tbl.add_column("#",           width=3,  justify="right")
    tbl.add_column("Category",    width=6,  justify="center")
    tbl.add_column("Question",    width=48)
    tbl.add_column("Score",       width=6,  justify="center")
    tbl.add_column("Verdict",     width=9,  justify="center")
    tbl.add_column("Time (s)",    width=8,  justify="right")
    tbl.add_column("Result",      width=7,  justify="center")

    verdict_icons = {"approved": "✅", "enhanced": "⚡", "revised": "🔧", "none": "—"}
    passed_count  = 0

    for i, r in enumerate(results, 1):
        ok     = r["passed"]
        status = "[green]PASS[/green]" if ok else "[red]FAIL[/red]"
        score  = f"{r['score']}/10" if r["score"] is not None else "—"
        vicon  = verdict_icons.get(r["verdict"], "—")
        if ok:
            passed_count += 1
        tbl.add_row(
            str(i),
            f"Cat {r['expected_cat']}",
            r["question"][:48],
            score,
            f"{vicon} {r['verdict']}",
            str(r["elapsed"]),
            status,
        )

    console.print(tbl)
    console.print()

    # ── Check breakdown ───────────────────────────────────────
    check_labels = {
        "no_error":      "No pipeline error",
        "correct_cat":   "Correct category",
        "response_len":  f"Response ≥ {PASS_MIN_CHARS} chars",
        "score_present": "Quality score extracted",
        "within_time":   f"Elapsed ≤ {PASS_MAX_SECS}s",
    }
    check_tbl = Table(box=box.SIMPLE, show_header=True, header_style="bold white",
                      title="[bold]Pass Criteria Breakdown[/bold]")
    check_tbl.add_column("Criterion",     width=30)
    check_tbl.add_column("Test 1", width=8, justify="center")
    check_tbl.add_column("Test 2", width=8, justify="center")
    check_tbl.add_column("Test 3", width=8, justify="center")
    check_tbl.add_column("Test 4", width=8, justify="center")
    check_tbl.add_column("Test 5", width=8, justify="center")

    for key, label in check_labels.items():
        row = [label]
        for r in results:
            row.append("[green]✓[/green]" if r["checks"][key] else "[red]✗[/red]")
        check_tbl.add_row(*row)

    console.print(check_tbl)
    console.print()

    # ── Final verdict ─────────────────────────────────────────
    if passed_count == total:
        console.print(Panel.fit(
            f"[bold green]✅  ALL {total}/{total} TESTS PASSED[/bold green]\n\n"
            "[white]The WBD APAC Title Performance Analyst is fully operational.[/white]\n"
            "[dim]All 9 agents active · Quality Critic gating all outputs · "
            "Dashboard live at: streamlit run dashboard_app.py[/dim]",
            border_style="green",
        ))
    else:
        failed = total - passed_count
        console.print(Panel.fit(
            f"[bold red]❌  {failed}/{total} TESTS FAILED[/bold red]\n"
            "[dim]Review the check breakdown above for details.[/dim]",
            border_style="red",
        ))

    console.print()
    return passed_count == total


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
