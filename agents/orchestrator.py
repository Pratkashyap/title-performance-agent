"""
agents/orchestrator.py
WBD APAC — Title Performance Analyst

Central routing agent. Receives all user questions, classifies into A/B/C/D/E/F,
routes to the correct specialist agents, and synthesises the final answer.

Phase 4 routes:
  Cat A → Data Agent (x2) → Performance Analyst
  Cat B → Data Agent (x1) → BenchmarkAgent + Performance Analyst → combined response
  Cat C → Data Agent (x1) → TrendAgent → trend report
  Cat D → Data Agent (x1) → GenreCatalogAgent → catalog + genre health report
  Cat E/F → Data Agent (x1) → data_only formatted table (Phase 5–6 agents not built yet)

Model: Claude Haiku 4.5 (fast classification)
"""

import os
import sys
import json
import re
import anthropic
from dotenv import load_dotenv

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))
from agents.data_agent            import DataAgent
from agents.performance_analyst   import PerformanceAnalyst
from agents.benchmark_agent       import BenchmarkAgent
from agents.trend_agent           import TrendAgent
from agents.genre_catalog_agent   import GenreCatalogAgent

_env_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "../.env")
load_dotenv(_env_path, override=True)

# ─────────────────────────────────────────────────────────────
# ORCHESTRATOR SYSTEM PROMPT
# ─────────────────────────────────────────────────────────────

ORCHESTRATOR_SYSTEM = """You are the Orchestrator for WBD APAC's Title Performance Analyst system.
Your job: classify incoming questions and decide how to handle them.

Context:
  Platform: Max / HBO Max streaming — APAC region
  Today: 2026-03-31
  Markets: AU, SG, HK, IN, JP, KR, TW, TH, PH, MY

Question categories:
  A — Diagnosis    : "Why" questions — root cause, underperformance, what's wrong
  B — Snapshot     : "How is it doing" — current performance, benchmarks, comparisons
  C — Trends       : WoW momentum, gaining/losing audience, time-series patterns
  D — Genre/Catalog: Genre health, catalog-wide analysis, high starts/low completion
  E — Subscriber   : Subscriber segment behaviour, churn risk, plan type analysis
  F — Alerts       : Proactive flags — which titles need attention, urgent drops

Routing rules:
  "why", "underperforming", "low", "struggling", "not working", "what's wrong" → A
  "how is", "performance", "snapshot", "vs", "compare", "benchmark", "doing" → B
  "trend", "momentum", "WoW", "week over week", "gaining", "losing", "growing" → C
  "genre", "catalog", "which titles", "platform-wide", "top titles", "list" → D
  "subscriber", "churn", "segment", "plan type", "loyal", "at-risk subscriber" → E
  "alert", "attention", "urgent", "need", "watch out", "this week", "flags" → F

Respond with ONLY a JSON object (no other text):
{
  "category":         "A|B|C|D|E|F",
  "needs_analysis":   true|false,
  "scope":            "brief description of what data is needed",
  "refined_question": "cleaner version of the question with explicit title names and markets",
  "out_of_scope":     false,
  "out_of_scope_reason": ""
}

Set needs_analysis=true for categories A and B.
Set needs_analysis=false for C, D, E, F.
Set out_of_scope=true ONLY for questions completely unrelated to streaming/content performance.
When in doubt, default to category B, needs_analysis=true."""


class Orchestrator:
    def __init__(self):
        self.client               = anthropic.Anthropic(api_key=os.getenv("ANTHROPIC_API_KEY"))
        self.model                = "claude-haiku-4-5-20251001"
        self.data_agent           = DataAgent()
        self.performance_analyst  = PerformanceAnalyst()
        self.benchmark_agent      = BenchmarkAgent()
        self.trend_agent          = TrendAgent()
        self.genre_catalog_agent  = GenreCatalogAgent()

    # ── Classification ────────────────────────────────────────

    def _classify(self, question: str) -> dict:
        """Classify the question and return routing metadata."""
        response = self.client.messages.create(
            model=self.model,
            max_tokens=400,
            system=ORCHESTRATOR_SYSTEM,
            messages=[{"role": "user", "content": question}],
        )
        text  = response.content[0].text.strip()
        match = re.search(r'\{.*\}', text, re.DOTALL)
        if match:
            try:
                return json.loads(match.group())
            except Exception:
                pass
        return {
            "category":         "B",
            "needs_analysis":   True,
            "scope":            question,
            "refined_question": question,
            "out_of_scope":     False,
        }

    # ── Helper responses ──────────────────────────────────────

    def _out_of_scope_response(self, question: str, reason: str) -> dict:
        return {
            "question": question,
            "category": "out_of_scope",
            "response": (
                "I'm focused on WBD APAC title performance analytics. "
                f"That question ({reason or 'unrelated to streaming'}) is outside my scope.\n\n"
                "Try asking about:\n"
                "• Why a title is underperforming (Cat A)\n"
                "• How a title is performing vs benchmarks (Cat B)\n"
                "• Trends and momentum (Cat C)\n"
                "• Genre health across the catalog (Cat D)\n"
                "• Subscriber behaviour (Cat E)\n"
                "• Titles needing immediate attention (Cat F)"
            ),
            "data": None, "error": None,
        }

    def _empty_response(self) -> dict:
        return {
            "question": "",
            "category": "empty",
            "response": (
                "Please ask a question about title performance.\n\n"
                "Examples:\n"
                "  • \"Why is The White Lotus S3 underperforming in Southeast Asia?\"\n"
                "  • \"How is House of the Dragon S2 performing vs fantasy benchmarks?\"\n"
                "  • \"Which titles have high starts but low completions?\"\n"
                "  • \"Is The Last of Us S2 gaining or losing momentum WoW?\"\n"
                "  • \"Which titles need immediate attention this week?\"\n\n"
                "Type 'demo' to run example queries. Type 'quit' to exit."
            ),
            "data": None, "error": None,
        }

    def _data_only_response(self, question: str, category: str, data_result: dict) -> dict:
        """Return formatted data table for Cat C/D/E/F (Phase 3+ not yet built)."""
        data = data_result.get("data")
        if data is not None and not data.empty:
            cols   = list(data.columns)
            header = "| " + " | ".join(str(c) for c in cols) + " |"
            sep    = "| " + " | ".join(["---"] * len(cols)) + " |"
            rows   = [
                "| " + " | ".join(str(v) for v in row) + " |"
                for _, row in data.iterrows()
            ]
            table  = "\n".join([header, sep] + rows)
            label  = {
                "C": "Trend data",
                "D": "Genre/catalog data",
                "E": "Subscriber data",
                "F": "Alert scan data",
            }.get(category, "Data")
            response_text = (
                f"**{label} — {len(data)} rows**\n\n"
                f"{table}\n\n"
                f"*[Cat {category} specialist agent coming in a future phase — "
                f"showing raw data for now.]*"
            )
        else:
            response_text = "No data returned for that query."

        return {
            "question": question,
            "category": category,
            "response": response_text,
            "data":     data,
            "sql":      data_result.get("sql"),
            "error":    None,
        }

    # ── Main entry point ──────────────────────────────────────

    def run(self, question: str, on_status=None) -> dict:
        """
        Route one question through the full pipeline.

        Args:
            question  : plain English question
            on_status : optional callback(agent, event, detail)

        Returns dict:
            question  — original question
            category  — A/B/C/D/E/F / out_of_scope
            response  — final answer (markdown string)
            data      — pandas DataFrame (or None)
            sql       — generated SQL (or None)
            error     — error string (or None)
        """
        def emit(agent, event, detail=""):
            if on_status:
                on_status(agent, event, detail)

        question = question.strip()
        if not question:
            return self._empty_response()

        # ── Step 1: Classify ──────────────────────────────────
        emit("orchestrator", "start")
        emit("orchestrator", "classifying", "Determining category and routing plan...")

        clf      = self._classify(question)
        category = clf.get("category", "B")
        refined  = clf.get("refined_question", question)
        scope    = clf.get("scope", "")
        needs_analysis = clf.get("needs_analysis", True)

        emit("orchestrator", "classified", json.dumps({
            "category":         category,
            "needs_analysis":   needs_analysis,
            "scope":            scope,
            "refined_question": refined,
        }))

        if clf.get("out_of_scope"):
            emit("orchestrator", "out_of_scope",
                 clf.get("out_of_scope_reason", ""))
            return self._out_of_scope_response(
                question, clf.get("out_of_scope_reason", ""))

        # ── Step 2: Routing label ─────────────────────────────
        cat_labels = {
            "A": "Cat A — Diagnosis  → Data Agent (x2) → Performance Analyst",
            "B": "Cat B — Snapshot   → Data Agent (x1) → Benchmark Agent + Performance Analyst",
            "C": "Cat C — Trends     → Data Agent (x2) → Trend Agent",
            "D": "Cat D — Genre/Cat  → Data Agent (x1) → Genre & Catalog Agent",
            "E": "Cat E — Subscriber → Data Agent (data only — Phase 5)",
            "F": "Cat F — Alerts     → Data Agent (data only — Phase 6)",
        }
        emit("orchestrator", "routing", cat_labels.get(category, "→ Data Agent"))

        # ── Step 3: Data fetching ─────────────────────────────
        emit("data_agent", "start")

        primary = self.data_agent.fetch(refined, verbose=False, on_status=on_status)

        if primary.get("error"):
            emit("data_agent", "error", primary["error"])
            return {
                "question": question, "category": category,
                "response": f"Data retrieval failed: {primary['error']}",
                "data": None, "error": primary["error"],
            }

        emit("data_agent", "done",
             str(primary.get("row_count", 0)))

        # ── Step 4: Cat E/F → data only (Phase 5–6 not built yet) ─
        if category in ("E", "F"):
            emit("orchestrator", "done")
            return self._data_only_response(question, category, primary)

        # ── Step 4b: Cat D → Genre & Catalog Agent ────────────
        if category == "D":
            emit("genre_catalog_agent", "start")
            catalog_result = self.genre_catalog_agent.analyse(
                question=question,
                primary_data=primary,
                on_status=on_status,
            )
            if catalog_result.get("error"):
                return {
                    "question": question, "category": category,
                    "response": f"Catalog analysis failed: {catalog_result['error']}",
                    "data":     primary.get("data"),
                    "error":    catalog_result["error"],
                }
            emit("orchestrator", "done")
            return {
                "question": question,
                "category": category,
                "response": catalog_result["insight"],
                "data":     primary.get("data"),
                "sql":      primary.get("sql"),
                "genres":   catalog_result.get("genres_analysed"),
                "error":    None,
            }

        # ── Step 5: Cat C → Trend Agent ──────────────────────
        if category == "C":
            emit("trend_agent", "start")
            trend_result = self.trend_agent.analyse(
                question=question,
                primary_data=primary,
                on_status=on_status,
            )
            if trend_result.get("error"):
                return {
                    "question": question, "category": category,
                    "response": f"Trend analysis failed: {trend_result['error']}",
                    "data":     primary.get("data"),
                    "error":    trend_result["error"],
                }
            emit("orchestrator", "done")
            return {
                "question": question,
                "category": category,
                "response": trend_result["insight"],
                "data":     primary.get("data"),
                "sql":      primary.get("sql"),
                "trend":    trend_result.get("trend_direction"),
                "error":    None,
            }

        # ── Step 6: Cat A/B → fetch benchmark supplement ─────
        data_results = [primary]

        if category == "A":
            # Diagnosis: also fetch genre/benchmark context
            bench_q = (
                f"What are the genre average completion rate, "
                f"average starts day 7, and average starts day 30 "
                f"for titles in the same genre as "
                f"{refined.split(' in ')[0].replace('Why is ', '').replace(' underperforming', '').strip()}?"
            )
            emit("data_agent", "start")
            bench_r = self.data_agent.fetch(bench_q, verbose=False, on_status=on_status)
            if bench_r.get("data") is not None and not bench_r["data"].empty:
                data_results.append(bench_r)
                emit("data_agent", "done",
                     f"+ {bench_r.get('row_count', 0)} benchmark rows")

        # ── Step 7: Performance Analyst (Cat A + B) ───────────
        emit("performance_analyst", "start")
        analyst_result = self.performance_analyst.analyse(
            question=question,
            category=category,
            data_results=data_results,
            on_status=on_status,
        )

        if analyst_result.get("error"):
            return {
                "question": question, "category": category,
                "response": f"Analysis failed: {analyst_result['error']}",
                "data":     primary.get("data"),
                "error":    analyst_result["error"],
            }

        # ── Step 8: Cat B → Benchmark Agent enrichment ────────
        final_response = analyst_result["insight"]

        if category == "B":
            emit("benchmark_agent", "start")
            bench_result = self.benchmark_agent.analyse(
                question=question,
                primary_data=primary,
                on_status=on_status,
            )
            if bench_result.get("insight") and not bench_result.get("error"):
                # Append peer comparison section below the snapshot
                final_response = (
                    analyst_result["insight"]
                    + "\n\n"
                    + bench_result["insight"]
                )

        emit("orchestrator", "done")

        return {
            "question": question,
            "category": category,
            "response": final_response,
            "data":     primary.get("data"),
            "sql":      primary.get("sql"),
            "error":    None,
        }


# ─────────────────────────────────────────────────────────────
# End-to-end test — all 6 categories
# ─────────────────────────────────────────────────────────────
if __name__ == "__main__":
    from rich.console import Console
    from rich.table   import Table
    from rich         import box

    console = Console()
    orch    = Orchestrator()

    console.print("\n[bold cyan]Orchestrator End-to-End Test — WBD APAC[/bold cyan]")
    console.print("[dim]All 6 question categories[/dim]\n")

    tests = [
        ("A", "Why is The White Lotus S3 underperforming in Southeast Asia?"),
        ("B", "How is House of the Dragon S2 performing vs comparable fantasy titles?"),
        ("C", "Is The Last of Us S2 gaining or losing momentum week over week?"),
        ("D", "Which titles have high starts but low completions right now?"),
        ("E", "What subscriber segments watch House of the Dragon S2 the most?"),
        ("F", "Which titles need immediate attention this week?"),
    ]

    tbl = Table(box=box.SIMPLE, show_header=True, header_style="bold white")
    tbl.add_column("Cat", width=4)
    tbl.add_column("Question", width=52)
    tbl.add_column("Routed to", width=22)
    tbl.add_column("Status", width=8)

    passed = 0
    for expected_cat, q in tests:
        console.print(f"[dim]Running:[/dim] [white]{q[:65]}[/white]")

        def show(agent, event, detail=""):
            if event not in ("start", "done"):
                console.print(f"  [dim]{agent}:{event}[/dim]")

        result = orch.run(q, on_status=show)

        ok     = result.get("error") is None
        cat    = result.get("category", "?")
        routed = {
            "A": "PerfAnalyst",
            "B": "Bench+Analyst",
            "C": "TrendAgent",
            "D": "CatalogAgent",
        }.get(cat, "DataOnly")
        status = "[green]✅[/green]" if ok else "[red]❌[/red]"

        tbl.add_row(cat, q[:52], routed, status)
        if ok:
            passed += 1
            preview = result["response"][:120].replace("\n", " ")
            console.print(f"  [dim]→ {preview}...[/dim]\n")
        else:
            console.print(f"  [red]Error: {result['error']}[/red]\n")

    console.print(tbl)
    console.print(f"\n[bold]Result: {passed}/{len(tests)} passed[/bold]")
    if passed == len(tests):
        console.print("[bold green]✅ Orchestrator fully operational. Phase 4 complete.[/bold green]\n")
    else:
        console.print(f"[bold yellow]⚠️  {len(tests)-passed} tests failed.[/bold yellow]\n")
