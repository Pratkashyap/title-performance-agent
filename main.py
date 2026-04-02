"""
main.py — WBD APAC Title Performance Analyst
Rich terminal UI showing every agent step in real time.

Usage:
  cd ~/Desktop/title-performance-agent
  source venv/bin/activate
  python3 main.py
"""

import os, sys, json, textwrap
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from rich.console import Console
from rich.panel   import Panel
from rich.text    import Text
from rich.rule    import Rule
from rich.table   import Table
from rich         import box

from agents.orchestrator import Orchestrator

console = Console()

# ── Colour palette ───────────────────────────────────────────
C_ORCH   = "bold cyan"
C_DATA   = "bold yellow"
C_ANAL   = "bold magenta"
C_OK     = "bold green"
C_ERR    = "bold red"
C_DIM    = "dim white"
C_LABEL  = "bold white"
C_RESP   = "bright_white"
C_HEADER = "bold bright_cyan"

AGENT_ICONS = {
    "orchestrator":        "🧠",
    "data_agent":          "📊",
    "performance_analyst": "🎬",
    "benchmark_agent":     "📈",
    "trend_agent":         "📉",
    "genre_catalog_agent": "🗂️",
    "subscriber_agent":    "👤",
    "alert_agent":         "🚨",
    "critic":              "⚖️",
}

AGENT_COLORS = {
    "orchestrator":        C_ORCH,
    "data_agent":          C_DATA,
    "performance_analyst": C_ANAL,
    "benchmark_agent":     "bold blue",
    "trend_agent":         "bold green",
    "genre_catalog_agent": "bold cyan",
    "subscriber_agent":    "bold yellow",
    "alert_agent":         "bold red",
    "critic":              "bold magenta",
}

AGENT_LABELS = {
    "orchestrator":        "ORCHESTRATOR",
    "data_agent":          "DATA AGENT",
    "performance_analyst": "PERFORMANCE ANALYST",
    "benchmark_agent":     "BENCHMARK AGENT",
    "trend_agent":         "TREND AGENT",
    "genre_catalog_agent": "GENRE & CATALOG AGENT",
    "subscriber_agent":    "SUBSCRIBER AGENT",
    "alert_agent":         "ALERT AGENT",
    "critic":              "QUALITY CRITIC",
}

# Track which agent headers have been printed this query
_printed_agents: set = set()


def _agent_header(agent: str):
    if agent in _printed_agents:
        return
    _printed_agents.add(agent)
    icon  = AGENT_ICONS.get(agent, "•")
    label = AGENT_LABELS.get(agent, agent.upper())
    color = AGENT_COLORS.get(agent, "white")
    console.print(f"\n  {icon}  [{color}]{label}[/{color}]")


def _step(agent: str, text: str, value: str = "", ok: bool = False, indent: int = 6):
    prefix = " " * indent + "├─ "
    suffix = "  [bold green]✓[/bold green]" if ok else ""
    if value:
        console.print(f"{prefix}[{C_LABEL}]{text}[/{C_LABEL}] [dim]{value}[/dim]{suffix}")
    else:
        console.print(f"{prefix}[{C_LABEL}]{text}[/{C_LABEL}]{suffix}")


def _step_last(agent: str, text: str, value: str = "", ok: bool = True, indent: int = 6):
    prefix = " " * indent + "└─ "
    suffix = "  [bold green]✓[/bold green]" if ok else ""
    if value:
        console.print(f"{prefix}[{C_LABEL}]{text}[/{C_LABEL}] [dim]{value}[/dim]{suffix}")
    else:
        console.print(f"{prefix}[{C_LABEL}]{text}[/{C_LABEL}]{suffix}")


def on_status(agent: str, event: str, detail: str):
    """Live pipeline display — called by Orchestrator for every step."""

    # ── ORCHESTRATOR ─────────────────────────────────────────
    if agent == "orchestrator":
        if event == "start":
            _agent_header(agent)

        elif event == "classifying":
            _step(agent, "Classifying question...")

        elif event == "classified":
            try:
                d = json.loads(detail)
            except Exception:
                d = {}
            cat    = d.get("category", "")
            scope  = d.get("scope", "")
            refined= d.get("refined_question", "")
            needs  = d.get("needs_analysis", True)

            cat_colors = {
                "A": "bold red",     "B": "bold cyan",
                "C": "bold green",   "D": "bold yellow",
                "E": "bold magenta", "F": "bold bright_red",
            }
            cat_names = {
                "A": "A — Diagnosis",  "B": "B — Snapshot",
                "C": "C — Trends",     "D": "D — Genre/Catalog",
                "E": "E — Subscriber", "F": "F — Alerts",
            }
            cc = cat_colors.get(cat, "white")
            cn = cat_names.get(cat, cat)

            _step(agent, "Category",
                  f"[{cc}]{cn}[/{cc}]", ok=True)

            if scope:
                _step(agent, "Scope",
                      textwrap.shorten(scope, width=60, placeholder="..."))

            if refined:
                _step(agent, "Refined question",
                      f"[italic]{textwrap.shorten(refined, width=60, placeholder='...')}[/italic]")

            _step(agent, "Analysis",
                  "[green]Performance Analyst[/green]" if needs else "[yellow]Data only (Phase 2)[/yellow]")

        elif event == "routing":
            _step_last(agent, "Routing", detail, ok=True)

        elif event == "out_of_scope":
            _step_last(agent, "Out of scope", detail, ok=False)

        elif event == "done":
            pass

    # ── DATA AGENT ───────────────────────────────────────────
    elif agent == "data_agent":
        if event == "start":
            _agent_header(agent)

        elif event == "classifying_query":
            _step(agent, detail or "Refining question for SQL...")

        elif event == "query_classified":
            try:
                d = json.loads(detail)
            except Exception:
                d = {}
            qtype  = d.get("query_type", "")
            window = d.get("time_window", "")
            cat    = d.get("category", "")
            if qtype:
                _step(agent, "Query type",  qtype, ok=True)
            if window:
                _step(agent, "Time window", window)

        elif event == "sql_generated":
            sql_preview = detail.replace("\n", " ").strip()
            _step(agent, "SQL generated",
                  f"[dim italic]{textwrap.shorten(sql_preview, width=60, placeholder='...')}[/dim italic]",
                  ok=True)

        elif event == "query_executed":
            _step(agent, "Query executed on DuckDB", ok=True)
            _step_last(agent, "Rows returned",
                       f"[bold green]{detail} rows[/bold green]", ok=True)

        elif event == "done":
            pass  # handled inline

        elif event == "error":
            _step_last(agent, "Error", f"[red]{detail}[/red]", ok=False)

    # ── PERFORMANCE ANALYST ──────────────────────────────────
    elif agent == "performance_analyst":
        if event == "start":
            _agent_header(agent)

        elif event == "preparing":
            _step(agent, "Data received", detail)
            _step(agent, "Model", "[bold]claude-sonnet-4-6[/bold]  (deep reasoning)")

        elif event == "analysing":
            _step(agent, "Generating analysis...",
                  "[dim]This takes ~10-15 seconds...[/dim]")

        elif event == "done":
            _step_last(agent, "Analysis complete", ok=True)

    # ── QUALITY CRITIC ───────────────────────────────────────
    elif agent == "critic":
        if event == "start":
            _agent_header(agent)
        elif event == "reviewing":
            _step(agent, "Scoring insight quality...", "")
        elif event == "done":
            _step_last(agent, "Quality check complete",
                       f"[bold green]{detail}[/bold green]", ok=True)


def print_banner():
    console.print()
    console.print(Panel(
        Text.assemble(
            ("  WBD APAC — Title Performance Analyst\n", "bold bright_white"),
            ("  Max / HBO Max Streaming — 10 APAC Markets\n", "bold cyan"),
            ("  ─────────────────────────────────────────────────────\n", "dim"),
            ("  Markets:   ", "dim"),
            ("AU  SG  HK  IN  JP  KR  TW  TH  PH  MY\n", "white"),
            ("  Genres:    ", "dim"),
            ("Drama  Fantasy  Sci-Fi  Crime  Thriller  Horror  K-Drama  Action\n", "white"),
            ("  Data:      ", "dim"),
            ("59 titles · 90 days · 10 markets · 2,200 subscribers\n", "white"),
            ("  ─────────────────────────────────────────────────────\n", "dim"),
            ("  Categories:  ", "dim"),
            ("A-Diagnosis  B-Snapshot  C-Trends  D-Genre  E-Subscriber  F-Alerts\n", "white"),
            ("  Agents:      ", "dim"),
            ("Orchestrator · Data · Performance Analyst  (Phase 2)\n", "white"),
            ("  ─────────────────────────────────────────────────────\n", "dim"),
            ("  Type a question  │  'demo' for examples  │  'queries' to browse  │  'quit' to exit", "dim italic"),
        ),
        box=box.DOUBLE_EDGE,
        border_style="cyan",
        padding=(0, 2),
    ))
    console.print()


DEMO_QUERIES = [
    # Cat A — Diagnosis
    "Why is The White Lotus S3 underperforming in Southeast Asia this month?",
    "Why does Euphoria S2 have low completion rates across APAC?",
    # Cat B — Snapshot
    "How is House of the Dragon S2 performing vs comparable fantasy titles?",
    "What is the current performance snapshot for The Last of Us S2?",
    # Cat D — Catalog
    "Which titles have high starts but low completion rates right now?",
    # Cat F — Alerts
    "Which titles need immediate attention this week?",
]

ALL_SAMPLE_QUERIES = {
    "🔴  CAT A — DIAGNOSIS (Why? Root cause)": [
        "Why is The White Lotus S3 underperforming in Southeast Asia?",
        "Why does Euphoria S2 have low completion rates across APAC?",
        "Why is The Idol struggling in all markets?",
        "What's causing the drop-off in House of the Dragon S2 episode 5?",
    ],
    "🔵  CAT B — SNAPSHOT (How is it doing?)": [
        "How is House of the Dragon S2 performing vs comparable fantasy titles?",
        "What is the performance snapshot for The Last of Us S2?",
        "How does Succession S4 compare to drama genre benchmarks?",
        "What is the completion rate for Dune 2 across all APAC markets?",
    ],
    "🟢  CAT C — TRENDS (Momentum)": [
        "Is The Last of Us S2 gaining or losing momentum week over week?",
        "How has House of the Dragon S2 trended over its first 30 days?",
        "Which markets are showing growth for White Lotus S3?",
    ],
    "🟡  CAT D — GENRE/CATALOG (Platform-wide)": [
        "Which titles have high starts but low completion rates?",
        "Which genre is overperforming on Max APAC this month?",
        "What are the top 5 titles by total starts this month?",
        "Which HBO Originals are underperforming vs licensed titles?",
    ],
    "🟣  CAT E — SUBSCRIBER (Behaviour)": [
        "Which subscriber segments watch House of the Dragon S2 the most?",
        "Does watching The Last of Us S1 reduce churn risk?",
        "What plan types are most common among TLOU S2 viewers?",
    ],
    "🚨  CAT F — ALERTS (Urgent)": [
        "Which titles need immediate attention this week?",
        "Which titles have dropped more than 15% starts week over week?",
        "Flag any titles that launched in the last 30 days below benchmark.",
    ],
}


def run_query(orch: Orchestrator, question: str):
    """Run one question through the pipeline with full live display."""
    global _printed_agents
    _printed_agents = set()

    console.print()
    console.rule(f"[bold white]{question}[/bold white]", style="dim cyan")
    console.print()
    console.print("  [dim]Pipeline starting...[/dim]")

    result = orch.run(question, on_status=on_status)

    # ── Response display ──────────────────────────────────────
    console.print()
    console.rule("[bold bright_cyan]  AGENT RESPONSE[/bold bright_cyan]", style="cyan")
    console.print()

    if result.get("error"):
        console.print(f"  [bold red]Error:[/bold red] {result['error']}")

    elif result.get("category") == "out_of_scope":
        console.print(Panel(result["response"], border_style="yellow", padding=(1, 2)))

    else:
        cat = result.get("category", "")
        cat_badge = {
            "A": "[bold red]Cat A — Diagnosis[/bold red]",
            "B": "[bold cyan]Cat B — Snapshot[/bold cyan]",
            "C": "[bold green]Cat C — Trends[/bold green]",
            "D": "[bold yellow]Cat D — Genre/Catalog[/bold yellow]",
            "E": "[bold magenta]Cat E — Subscriber[/bold magenta]",
            "F": "[bold bright_red]Cat F — Alert[/bold bright_red]",
        }.get(cat, "")

        if cat_badge:
            console.print(f"  {cat_badge}\n")

        for line in result["response"].split("\n"):
            stripped = line.strip()
            if stripped.startswith("**") and stripped.endswith("**"):
                console.print(f"  [bold bright_cyan]{stripped.strip('*')}[/bold bright_cyan]")
            elif stripped.startswith("- "):
                console.print(f"  [white]  •{stripped[1:]}[/white]")
            elif stripped.startswith("|") and stripped.endswith("|"):
                # Table row — dim styling
                console.print(f"  [dim]{stripped}[/dim]")
            elif stripped.startswith("1. ") or stripped.startswith("2. ") or stripped.startswith("3. "):
                console.print(f"  [bright_white]  {stripped}[/bright_white]")
            elif stripped.startswith("---") or stripped.startswith("───"):
                console.print(f"  [dim]{'─'*60}[/dim]")
            elif stripped.startswith("*[") and stripped.endswith("]*"):
                console.print(f"  [dim italic]{stripped}[/dim italic]")
            elif stripped:
                console.print(f"  {stripped}")

    console.print()


def print_queries():
    """Show all available sample queries grouped by category."""
    console.print()
    console.rule("[bold bright_cyan]  SAMPLE QUERIES — BY CATEGORY[/bold bright_cyan]", style="cyan")
    console.print()
    for cat_label, queries in ALL_SAMPLE_QUERIES.items():
        console.print(f"  [bold bright_white]{cat_label}[/bold bright_white]")
        for q in queries:
            console.print(f"    [dim]•[/dim] [white]{q}[/white]")
        console.print()
    console.print("  [dim italic]Paste any query above at the prompt.[/dim italic]")
    console.print()


def run_demo(orch: Orchestrator):
    console.print()
    console.rule(f"[bold cyan]DEMO — {len(DEMO_QUERIES)} Example Queries[/bold cyan]")
    console.print(f"  [dim]Covering: Diagnosis · Snapshot · Catalog · Alerts[/dim]\n")

    for i, q in enumerate(DEMO_QUERIES, 1):
        console.print(f"\n  [dim]Demo {i}/{len(DEMO_QUERIES)}[/dim]")
        run_query(orch, q)
        if i < len(DEMO_QUERIES):
            try:
                console.input("  [dim]Press Enter for next query...[/dim]")
            except (KeyboardInterrupt, EOFError):
                break


def main():
    print_banner()
    orch = Orchestrator()
    console.print("  [bold green]✓[/bold green] Agent ready — ask me anything.\n")

    while True:
        try:
            user_input = console.input("  [bold cyan]You:[/bold cyan] ").strip()
        except (KeyboardInterrupt, EOFError):
            console.print("\n  Goodbye!\n")
            break

        if not user_input:
            continue
        if user_input.lower() in ("quit", "exit", "bye", "q"):
            console.print("\n  Goodbye!\n")
            break
        if user_input.lower() == "demo":
            run_demo(orch)
            continue
        if user_input.lower() in ("queries", "query", "help", "?", "examples"):
            print_queries()
            continue

        run_query(orch, user_input)


if __name__ == "__main__":
    main()
