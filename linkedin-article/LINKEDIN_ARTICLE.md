# LinkedIn Article — Title Performance Analyst
## Series: Build AI Raw | Batch 2 of 5
## Core Message: AI agents as a streaming analytics team
## Status: Ready to publish

---

# TITLE:
# I built a streaming analytics team for Max APAC that answers questions your BI team takes 3 days to answer. Here's the 9-agent system behind it.

---

Content teams at streaming companies are sitting on a data problem nobody talks about publicly.

They have the data. Gigabytes of it. Daily viewership, episode drop-off curves, subscriber churn signals, market-by-market completion rates, week-over-week momentum for every title across every market.

What they don't have is an analyst fast enough to use it.

By the time a content strategist gets a question answered — *why did episode 3 cause this drop-off in Southeast Asia? which subscriber segment churns after watching this genre?* — the window to act has usually closed.

So I built the analyst. Nine of them, actually.

---

## The System: Title Performance Analyst

**Title Performance Analyst** is a 9-agent AI system built for APAC streaming analytics — specifically designed to answer the category of question that sits between "I can see it in the dashboard" and "I need a data team to investigate."

It runs against a live DuckDB database: 5 tables, real simulated APAC viewership data across 10 markets (AU, SG, HK, IN, JP, KR, TW, TH, PH, MY), multiple titles, subscriber segments, episode-level drop-off data.

Ask it anything. It classifies, routes, queries, analyses, and delivers a structured answer in under 75 seconds.

---

## Meet the 9-Agent Team

Every agent has exactly one job. That constraint is not a limitation — it's the architecture.

---

**🧠 The Director — Orchestrator Agent**

The only agent the user talks to. Receives every question, classifies it into one of six categories (A through F — more on that below), routes to the right specialist, gates the output through Quality Critic, and delivers the final answer.

The Orchestrator's system prompt has one strict rule: it must never query data, never analyse, never recommend. Route and synthesise only. Without that constraint, it drifts into doing everything — and the specialists become redundant.

---

**📊 The Data Engineer — Data Agent**

Converts natural language questions into DuckDB SQL and returns structured results. Knows the schema cold. Handles fuzzy title names, time window ambiguity, multi-market joins.

When any other agent needs data, they go through here. Nobody else touches the database.

---

**🔍 The Diagnostician — Performance Analyst Agent**

Takes a title, a market, a time window — and produces a structured diagnostic. Completion rate vs benchmark. Episode drop-off pattern. Anomaly detection. WoW trajectory.

Every claim must reference specific numbers from the data. Vague language ("seems to be underperforming") is forbidden in the system prompt.

---

**📐 The Benchmarker — Benchmark Agent**

Answers comparative questions. How does House of the Dragon S2 compare to other fantasy titles at Day 30? Where does The Last of Us sit in the Drama genre distribution for APAC?

Produces peer-group comparisons with rank, percentile, and variance from genre median.

---

**📈 The Momentum Tracker — Trend Agent**

Week-over-week analysis. Is a title accelerating or decelerating? Which markets are growing fastest? Where is viewership momentum reversing?

Returns structured momentum signals: Accelerating / Stable / Decelerating — with the numbers behind each call.

---

**🎭 The Catalog Strategist — Genre & Catalog Agent**

Answers broad catalog questions. Genre health across APAC. Which titles have high starts but low completions (a specific signal — audience reaching but not retaining). Originals vs. licensed performance. Language breakdown.

---

**👤 The Subscriber Analyst — Subscriber Agent**

Links viewing behaviour to churn risk. Does watching a specific title correlate with lower churn? Which subscriber segments are most at risk? What do high-value subscribers watch?

Operates on subscriber_viewing data with churn flags. Returns churn signal thresholds: green (<5%), amber (5–12%), red (>12%).

---

**🚨 The Early Warning System — Alert Agent**

Runs three automated scans on every query:
- WoW viewership drops >10% (Critical: >30% drop)
- Completion rates below 50% with significant audience (>100 starts in last 7 days)
- New launch vs. Day-7 genre benchmark comparison

Returns severity-graded alerts before problems become crises.

---

**⚖️ The Editor — Quality Critic Agent**

Every output — before it reaches the user — is scored 0–10 across five dimensions:
- **Specificity** — Are real numbers present?
- **Actionability** — Can a content lead act on this today?
- **Accuracy** — Are benchmarks correctly referenced?
- **Completeness** — Does it answer what was actually asked?
- **Tone** — Is it executive-ready or full of hedging?

Score 8+: Approved. 6–7: Enhanced. Below 6: Revised with the weakest section rewritten.

**This single agent raised average response quality from ~6.5/10 to 8.9/10.** An AI reviewing AI output. That's quality control in a multi-agent system.

---

## The Six Question Categories

The Orchestrator classifies every incoming question before routing. Six categories, each with a different specialist path:

| Cat | Type | Example |
|-----|------|---------|
| A | Diagnosis | Why is Euphoria S2 underperforming in Southeast Asia? |
| B | Snapshot | How is House of the Dragon S2 vs fantasy benchmarks? |
| C | Trend | Is The Last of Us gaining or losing momentum WoW? |
| D | Genre & Catalog | Which titles have high starts but low completions? |
| E | Subscriber | Does watching HotD reduce churn risk? |
| F | Alerts | Which titles need immediate attention this week? |

The classification determines the routing. The routing determines who gets called. The specialist calls determine the answer. The Critic determines whether that answer gets delivered or revised.

---

## How It Works — A Real Example

Ask: *"Why is Euphoria S2 underperforming in Southeast Asia?"*

```
You → Orchestrator        (classifies: Cat A — Diagnosis)
   → Data Agent           (pulls title + market viewership data)
   → Performance Analyst  (completion rate 41% vs 63% genre avg,
                           episode 3 drop-off: 34% abandoned)
   → Quality Critic       (Quality Score: 9/10 — Approved)
   → You                  (structured diagnosis with numbers)

Total time: ~52 seconds.
```

The answer your content team would have had on Thursday.

---

## What Broke — The Failures Nobody Publishes

**The WoW scan that returned zero results.**
The Alert Agent's week-over-week scan uses CASE-based week aggregation with a HAVING clause filtering for drops >10%. In simulated data, all titles were stable. The scan returned zero alerts — not because it was broken, but because there was nothing to flag. Took an hour to confirm it wasn't a bug. Always test your "no alerts" state deliberately, not just the alert-firing state.

**The SQL injection the filter bar caused.**
The Alerts tab had a churn query that opened with `WHERE {gen_sql}`. When no genre was selected, `gen_sql` was an empty string — valid. When a genre *was* selected, `gen_sql` was `"AND t.genre = 'Drama'"` — producing `WHERE AND t.genre = 'Drama'`. Invalid SQL. No Python error. Just a silent database exception. Fixed by prepending `WHERE 1=1` and appending all filters after it.

**The input that wouldn't let users type.**
Streamlit reruns the entire script on every interaction. Using both `value=` and `key=` on the same `st.text_input` locks the input to the default value on every rerun. Users could click Send — but whatever they'd typed was gone. Fixed with a prefill + key-counter pattern: a separate `ai_prefill` state variable and an `ai_inp_key` counter that increments to force widget re-creation without locking the value.

**The session state exception on Clear.**
Setting `st.session_state.ai_input = ""` after the widget with key `ai_input` had already been instantiated throws a `StreamlitAPIException`. Streamlit owns that key once the widget exists. Fixed by separating the display key from the state variable — never write to a session state key that belongs to a live widget.

**The meta-lesson:**
Streamlit's rerun model is both its superpower and its trap. Every UI interaction starts the script from zero. Design your state machine first — then build the UI around it.

---

## The Numbers

- **9 agents** — each with one explicitly bounded role
- **2 Claude models** — Sonnet 4.6 for deep reasoning; Haiku 4.5 for fast classification and quality scoring
- **5 DuckDB tables** — titles, viewership_daily, viewership_episode, subscriber_viewing, title_benchmarks
- **10 APAC markets** — AU, SG, HK, IN, JP, KR, TW, TH, PH, MY
- **6 question categories** — A through F, each with a dedicated routing path
- **~$0.04 per query** — API cost at current usage
- **5/5 end-to-end tests passing** — all scoring 8–9/10 Approved
- **~2,800 lines of Python** — agents, orchestration, dashboard, tests

---

## Why Streaming Specifically

Streaming content performance has a property that makes it uniquely suited to AI agents: **the questions are structured, but the answers require reasoning**.

"Which titles are underperforming?" is a SQL question. "Why is this title underperforming, and what does that mean for our content strategy?" is a reasoning question. Most analytics tools handle the first. Almost none handle the second at speed.

The agent system handles both — the Data Agent runs the SQL, the specialist interprets it, the Critic ensures the interpretation is rigorous. That's the gap it fills.

---

## What's Next

```
✅ Batch 1 (+4 weeks)  → Marketing Analytics Agent — 7 agents live
✅ Batch 2 (NOW)       → Title Performance Analyst — 9 agents live, APAC streaming
🔨 Batch 3 (+2 weeks)  → Dashboard V2: live pipeline trace, richer visualisations
🔨 Batch 4 (+5 weeks)  → Memory: cross-session context, learning from past queries
🔨 Batch 5 (+8 weeks)  → Use Case #3: same architecture, different industry
```

Each system uses the same orchestration pattern. Different domain. Different agents. Same supervisor-with-critic architecture. The framework is the product — the agents are the configuration.

---

If you work in streaming or media analytics and have a question that's been sitting in your backlog for three days — drop it in the comments. I'll run it through the system.

Follow for Batch 3.

---

**[SCREENSHOTS TO ATTACH BEFORE PUBLISHING:]**
- [ ] thumbnail.png — header image
- [ ] agent_pipeline.png — architecture overview
- [ ] agent_chat_demo.png — real agent output with pipeline
- [ ] how_i_built_it.png — 9-step build diagram
- [ ] dashboard_overview.png — overview tab

---

*#StreamingAnalytics #AIAgents #MultiAgentAI #ContentAnalytics #ClaudeAPI #BuildingInPublic #MediaTech #APAC #MaxStreaming*

---
*Build AI Raw | Batch 2*
*Word count: ~1,700 | Read time: ~7 min*
