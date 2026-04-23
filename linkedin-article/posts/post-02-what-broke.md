POST 02 — WHAT BROKE
Publish: Wednesday
Screenshot to attach: agent_chat_demo.png
Hashtags: first comment only — not in post body
NOTE: Highest comment potential
-----------------------------------------------------

4 things broke while building this.
Nobody publishes these. I will.

Failure 1 — The alert scan that returned zero results
The WoW drop scan was technically correct. All titles were stable in the data. Zero alerts fired.
Took an hour to confirm it wasn't a bug — it was working perfectly.
Always test your "no alerts" state deliberately, not just the alert-firing state.

Failure 2 — Silent SQL with an invalid WHERE clause
Genre filter injected SQL directly into the query. When empty: valid. When selected: WHERE AND t.genre = 'Drama'
No error. No crash. Silent database exception.
Fix: always open with WHERE 1=1 and append every filter after it.

Failure 3 — Users couldn't type in the input box
Streamlit reruns the entire script on every interaction. Using value= and key= together locked the field on every rerun.
Users clicked Send. Whatever they'd typed was gone.
Fix: separate the display key from the prefill state. Never bind both.

Failure 4 — Session state exception on Clear
Setting session_state.ai_input = "" after the widget existed throws an API exception.
Streamlit owns that key once the widget is live. You cannot write to it.
Fix: separate the state variable from the widget key entirely.

The meta-lesson: design your state machine first. Then build the UI around it.

Which one of these have you hit in your own builds?

-----------------------------------------------------
FIRST COMMENT (post within 60 seconds of publishing):

Full build on GitHub:
https://github.com/Pratkashyap/title-performance-agent

#AIAgents #BuildingInPublic #AIEngineering #Python #MultiAgentAI #ClaudeAPI #StreamingAnalytics #Streamlit #MediaTech
