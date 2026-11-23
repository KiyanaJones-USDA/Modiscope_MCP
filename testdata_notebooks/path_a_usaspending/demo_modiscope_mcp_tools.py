# Databricks notebook source
# DBTITLE 1,Modiscope MCP demo
# MAGIC %md
# MAGIC # Modiscope: MCP contract simulation demo
# MAGIC Every step below is a **real MCP tool call** (FastMCP client -> server tools in `apps/mcp-modiscope-server/server/tools`).
# MAGIC The server runs in-process here; the same tools are served by the Databricks App when it is deployed.
# MAGIC
# MAGIC | Step | MCP tool | What it does |
# MAGIC |---|---|---|
# MAGIC | 1 | `fetch_usaspending_piid` | Pull the real award from USAspending (cached 5 days) |
# MAGIC | 2 | `create_scenario` | Build a what-if scenario: funding change, POP extension, burn-rate change |
# MAGIC | 3a | `fetch_award_patterns` | Pricing type (FFP, T&M, CPFF...) used by similar USAspending awards with the same NAICS + PSC + size tier (cached 30 days) |
# MAGIC | 3b | `generate_synthetic_clins` | Base + option-period CLINs; calls `fetch_award_patterns` for the pricing type |
# MAGIC | 4 | `compare_scenario_to_baseline` | Baseline award vs scenario: funding, POP, burn, estimate at completion |
# MAGIC | 5 | `generate_risk_alerts` | Checks **funding**, **POP**, and **burn rate** |
# MAGIC | 6 | `explain_impact` | Plain-language impact briefing (Databricks-hosted LLM) |
# MAGIC
# MAGIC Set the scenario levers in the widgets at the top, then **Run all**.

# COMMAND ----------

# DBTITLE 1,Install MCP server requirements
# MAGIC %pip install -q -r "/Workspace/Users/kiyana.jones@usda.gov/MCP Prototype/mcp-ocio-modiscope/mcp-modiscope/apps/mcp-modiscope-server/requirements.txt"
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# DBTITLE 1,Scenario levers (widgets)
dbutils.widgets.text("piid", "12314418F0020", "1. PIID")
dbutils.widgets.text("funding_change_pct", "-30", "2. Funding change %")
dbutils.widgets.text("pop_extension_months", "9", "3. POP extension (months)")
dbutils.widgets.text("burn_rate_multiplier", "4.0", "4. Burn rate multiplier")
dbutils.widgets.text("as_of_date", "", "5. As-of date (blank = today)")

PIID = dbutils.widgets.get("piid").strip().upper()
LEVERS = {
    "funding_change_pct": float(dbutils.widgets.get("funding_change_pct")),
    "pop_extension_months": int(dbutils.widgets.get("pop_extension_months")),
    "burn_rate_multiplier": float(dbutils.widgets.get("burn_rate_multiplier")),
    "as_of_date": dbutils.widgets.get("as_of_date").strip() or None,  # None -> tools use today
}
from datetime import date
print("Evaluating as of:", LEVERS["as_of_date"] or f"today ({date.today().isoformat()})")
print(PIID, LEVERS)

# COMMAND ----------

# DBTITLE 1,Start MCP server and client
import sys, json
import pandas as pd
from IPython.display import Markdown, display as ipy_display

APP_DIR = "/Workspace/Users/kiyana.jones@usda.gov/MCP Prototype/mcp-ocio-modiscope/mcp-modiscope/apps/mcp-modiscope-server"
if APP_DIR not in sys.path:
    sys.path.insert(0, APP_DIR)

# Always load the latest tool code (drop cached server.* modules from earlier runs)
for _m in [m for m in sys.modules if m == "server" or m.startswith("server.")]:
    del sys.modules[_m]

from fastmcp import FastMCP, Client
from server.tools import load_tools

# Same registration the deployed app uses (server/tools/__init__.py)
mcp = FastMCP("mcp-modiscope-server")
load_tools(mcp)

async def call(tool: str, args: dict) -> dict:
    """Call one tool through the MCP protocol and return its JSON result."""
    async with Client(mcp) as client:
        res = await client.call_tool(tool, args)
    return res.structured_content or res.data

async with Client(mcp) as client:
    tools = await client.list_tools()
def _summary(desc: str) -> str:
    """First paragraph of the tool docstring (stops before Args:)."""
    return " ".join((desc or "").split("Args:")[0].split())

with pd.option_context("display.max_colwidth", None):
    display(pd.DataFrame([{"tool": t.name, "description": _summary(t.description),
                           "inputs": ", ".join((getattr(t, "input_schema", None) or t.inputSchema).get("properties", {}).keys())} for t in tools]))

# COMMAND ----------

# DBTITLE 1,Step 1: fetch_usaspending_piid
award_resp = await call("fetch_usaspending_piid", {"piid": PIID})
award = award_resp["results"][0]
print("cache:", award_resp.get("_cache"))
display(pd.DataFrame([{
    "PIID": award["Award ID"], "Recipient": award["Recipient Name"], "Agency": award["Awarding Agency"],
    "NAICS": f"{award['NAICS']['code']} {award['NAICS']['description']}",
    "PSC": f"{award['PSC']['code']} {award['PSC']['description']}",
    "Award Amount": award["Award Amount"], "Total Outlays": award["Total Outlays"],
    "POP Start": award["Start Date"], "POP End": award["End Date"], "Potential End": award.get("Potential End Date"),
}]))

# COMMAND ----------

# DBTITLE 1,Step 2: create_scenario
scenario = await call("create_scenario", {"piid": PIID, **LEVERS, "scenario_name": "Demo what-if"})
SCENARIO_ID = scenario["scenario_id"]
print("scenario_id:", SCENARIO_ID)
print(json.dumps(scenario["what_if"], indent=2))

# COMMAND ----------

# DBTITLE 1,Step 3: generate_synthetic_clins
clins = await call("generate_synthetic_clins", {"scenario_id": SCENARIO_ID})
print(f"{clins['clin_count']} CLINs | total ${clins['total_obligated']:,.2f} (award ${clins['award_amount']:,.2f}) "
      f"| pricing {clins['pricing_type']} from {clins['pricing_source']}")
display(pd.DataFrame(clins["periods"]))
display(pd.DataFrame(clins["sample_clins"]))

# COMMAND ----------

# DBTITLE 1,Step 4: compare_scenario_to_baseline
cmp = await call("compare_scenario_to_baseline", {"scenario_id": SCENARIO_ID})
display(pd.DataFrame([{"metric": k, **v} for k, v in cmp["deltas"].items()]))
display(pd.DataFrame(cmp["periods"]))  # obligations per option period, incl. unfunded extension

# COMMAND ----------

# DBTITLE 1,Step 5: generate_risk_alerts
risk = await call("generate_risk_alerts", {"scenario_id": SCENARIO_ID})
print(f"Overall risk: {risk['overall_risk']}  ({risk['alert_count']} alerts; checks: {', '.join(risk['checks'])})")
display(pd.DataFrame(risk["alerts"])[["severity", "category", "title", "detail", "recommendation", "new_vs_baseline"]])

# COMMAND ----------

# DBTITLE 1,Step 6: explain_impact
impact = await call("explain_impact", {"scenario_id": SCENARIO_ID})
print("source:", impact["explanation_source"])
try:
    import markdown  # render the briefing as formatted HTML
    displayHTML(markdown.markdown(impact["explanation"], extensions=["tables"]))
except ImportError:
    print(impact["explanation"])

# COMMAND ----------

# DBTITLE 1,Step 7: agent picks the tools
# MAGIC %md
# MAGIC ---
# MAGIC ## Step 7: An AI agent picks the tools itself
# MAGIC Above, *we* called each tool in order. Here, the Databricks-hosted model gets only the **MCP tool list** (names, descriptions, input schemas) and a plain-English question.
# MAGIC It decides which tools to call, in what order, and with what arguments. Each call goes through the MCP protocol, and the model answers only from the tool results.
# MAGIC
# MAGIC Type any question in the **6. Ask the agent** widget and re-run the cell below.

# COMMAND ----------

# DBTITLE 1,Agent loop over MCP tools
import warnings
from databricks.sdk import WorkspaceClient

warnings.filterwarnings("ignore", category=DeprecationWarning)

AGENT_MODEL = "databricks-claude-sonnet-4-6"
AGENT_SYSTEM = (
    "You are Modiscope, a federal contract analyst assistant. Use the MCP tools to answer. "
    "For what-if questions: create_scenario, then generate_synthetic_clins, compare_scenario_to_baseline, "
    "generate_risk_alerts and explain_impact. Never invent numbers; only use tool results. "
    "Finish with a short answer: key numbers, top risks, recommended actions."
)

async def ask_agent(question: str, max_steps: int = 10):
    """Tool-calling loop: the LLM chooses MCP tools; each call runs through the MCP client."""
    llm = WorkspaceClient().serving_endpoints.get_open_ai_client()
    async with Client(mcp) as client:
        tools = await client.list_tools()
        # MCP tool list -> LLM function-calling schema (no hand-written tool definitions)
        schemas = [{"type": "function", "function": {
            "name": t.name, "description": t.description or "",
            "parameters": getattr(t, "input_schema", None) or t.inputSchema}} for t in tools]
        msgs = [{"role": "system", "content": AGENT_SYSTEM}, {"role": "user", "content": question}]
        trace = []
        for step in range(1, max_steps + 1):
            m = llm.chat.completions.create(model=AGENT_MODEL, messages=msgs, tools=schemas,
                                            max_tokens=2000, temperature=0).choices[0].message
            if not m.tool_calls:
                return m.content, trace
            msgs.append({"role": "assistant", "content": m.content or "", "tool_calls": [
                {"id": tc.id, "type": "function",
                 "function": {"name": tc.function.name, "arguments": tc.function.arguments}} for tc in m.tool_calls]})
            for tc in m.tool_calls:
                args = json.loads(tc.function.arguments or "{}")
                print(f"[step {step}] agent called {tc.function.name}({args})")
                try:
                    res = await client.call_tool(tc.function.name, args)
                    out = res.structured_content or res.data
                except Exception as e:
                    out = {"error": str(e)}
                trace.append({"step": step, "tool": tc.function.name, "arguments": json.dumps(args)})
                msgs.append({"role": "tool", "tool_call_id": tc.id, "content": json.dumps(out, default=str)[:12000]})
        return "Stopped: step limit reached.", trace

dbutils.widgets.text("agent_question",
    "What happens to contract 12314418F0020 if funding is cut 20% and the POP is extended 4 months?",
    "6. Ask the agent")
question = dbutils.widgets.get("agent_question")
print("Question:", question, "\n")

answer, trace = await ask_agent(question)
if trace:
    display(pd.DataFrame(trace))
else:
    print("(agent answered without calling any tools)\n")
print(answer)