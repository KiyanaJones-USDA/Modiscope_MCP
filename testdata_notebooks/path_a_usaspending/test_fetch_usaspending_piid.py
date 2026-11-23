# Databricks notebook source
# DBTITLE 1,Install server requirements
# MAGIC %pip install -q -r "/Workspace/Users/kiyana.jones@usda.gov/MCP Prototype/mcp-ocio-modiscope/mcp-modiscope/apps/mcp-modiscope-server/requirements.txt"

# COMMAND ----------

# DBTITLE 1,Restart Python
dbutils.library.restartPython()

# COMMAND ----------

# DBTITLE 1,Import the tool from the app source
import sys, json

# Make the app folder importable so `server.*` imports resolve like they do in the deployed app
APP_DIR = "/Workspace/Users/kiyana.jones@usda.gov/MCP Prototype/mcp-ocio-modiscope/mcp-modiscope/apps/mcp-modiscope-server"
if APP_DIR not in sys.path:
    sys.path.insert(0, APP_DIR)

from server.tools.fetch_usaspending_piid import fetch_usaspending_piid, fetch_usaspending_piid_tool

TEST_PIID = "AGSPECD170025"

# COMMAND ----------

# DBTITLE 1,Call the function directly (view only)
# Runs as YOU (notebook identity), not the app service principal
result = fetch_usaspending_piid(TEST_PIID)

print("cache:", result.get("_cache"))
print("error:", result.get("error"))
print(json.dumps((result.get("results") or [None])[0], indent=2))

# COMMAND ----------

# DBTITLE 1,Second call should be a cache hit
again = fetch_usaspending_piid(TEST_PIID.lower())  # lowercase on purpose: tool uppercases the PIID
print("cache:", again.get("_cache"))
assert again.get("_cache", {}).get("hit") is True, "Expected a cache hit on the second call"

# COMMAND ----------

# DBTITLE 1,Call through an in-memory MCP client
# Exercises the tool through the MCP protocol (schema, argument validation, serialization)
from fastmcp import FastMCP, Client

mcp = FastMCP("test-modiscope")
mcp.add_tool(fetch_usaspending_piid_tool)

async with Client(mcp) as client:
    tools = await client.list_tools()
    print("tools:", [t.name for t in tools])
    res = await client.call_tool("fetch_usaspending_piid", {"piid": TEST_PIID})

print(json.dumps(res.structured_content or res.data, indent=2, default=str)[:2000])

# COMMAND ----------

