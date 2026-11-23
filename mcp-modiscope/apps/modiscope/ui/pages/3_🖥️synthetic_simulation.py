import os
import json
import traceback
import streamlit as st
from databricks.sdk import WorkspaceClient
from databricks_mcp import DatabricksMCPClient

st.header("USAspening - Synthetic Simulation")


# Base URL of the mcp-modiscope-server app (no /mcp), set in app.yaml env
MCP_APP_URL = os.environ.get("MCP_APP_URL", "").rstrip("/")
if not MCP_APP_URL or "REPLACE_WITH" in MCP_APP_URL:
    st.error("MCP_APP_URL is not configured. Set it in app.yaml to the mcp-modiscope-server app URL.")
    st.stop()
st.caption(f"MCP server: {MCP_APP_URL}/mcp")

piid = st.text_input("Enter PIID", value="ex.FA1234-24-C-0001")
create_simulation = st.checkbox(
    "Create simulation scenario",
    value=False,
    help="Leave unchecked to only view the award data.",
)

if st.button("Fetch Award via MCP"):
    try:
        w = WorkspaceClient()  # uses workspace identity context
        mcp = DatabricksMCPClient(server_url=f"{MCP_APP_URL}/mcp", workspace_client=w)
        result = mcp.call_tool(
            "fetch_usaspending_piid",
            arguments={"piid": piid, "create_simulation": create_simulation},
        )
        st.success("Tool call succeeded")

        for block in getattr(result, "content", []) or []:
            text = getattr(block, "text", None)
            if text is None:
                st.write(block)
                continue
            try:
                st.json(json.loads(text))
            except Exception:
                st.write(text)
    except Exception as e:
        st.error(f"Tool call failed: {e}")
        st.code(traceback.format_exc())
