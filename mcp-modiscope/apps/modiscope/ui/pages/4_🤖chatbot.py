import json
import os
import traceback

import streamlit as st
from databricks.sdk import WorkspaceClient
from databricks_mcp import DatabricksMCPClient

# ---- Config (set in app.yaml env) ----
MCP_APP_URL = os.environ.get("MCP_APP_URL", "").rstrip("/")
MODEL_ENDPOINT = os.environ.get("CHAT_MODEL_ENDPOINT", "databricks-claude-sonnet-4-6")
MAX_TOOL_ROUNDS = 5          # max LLM -> tool -> LLM loops per user message
MAX_TOOL_RESULT_CHARS = 20000  # truncate large tool outputs before sending to the model

SYSTEM_PROMPT = (
    "You are the Modiscope assistant for USDA contract (CLIN) analysis. "
    "Use the available MCP tools to look up data instead of guessing. "
    "PIIDs are uppercase. Summarize tool results clearly and concisely."
)

st.title("💬 Modiscope Chatbot")
st.caption(f"🚀 Powered by MCP ({MODEL_ENDPOINT})")

if not MCP_APP_URL:
    st.error("MCP_APP_URL is not configured. Set it in app.yaml to the mcp-modiscope-server app URL.")
    st.stop()


@st.cache_resource
def get_clients():
    """LLM client (Databricks-hosted model, OpenAI-compatible) + MCP client."""
    w = WorkspaceClient()
    llm = w.serving_endpoints.get_open_ai_client()
    mcp = DatabricksMCPClient(server_url=f"{MCP_APP_URL}/mcp", workspace_client=w)
    return llm, mcp


@st.cache_data(ttl=300)
def get_tool_specs():
    """Discover tools from the MCP server (refreshed every 5 min, so new tools appear automatically)."""
    _, mcp = get_clients()
    specs = []
    for t in mcp.list_tools():
        specs.append(
            {
                "type": "function",
                "function": {
                    "name": t.name,
                    "description": t.description or "",
                    # field name differs by client version (inputSchema vs input_schema)
                    "parameters": getattr(t, "inputSchema", None) or getattr(t, "input_schema", None)
                    or {"type": "object", "properties": {}},
                },
            }
        )
    return specs


def call_mcp_tool(name, args):
    """Call an MCP tool and return its result as text for the model."""
    _, mcp = get_clients()
    try:
        result = mcp.call_tool(name, args)
        parts = []
        for block in getattr(result, "content", []) or []:
            text = getattr(block, "text", None)
            parts.append(text if text is not None else str(block))
        return "\n".join(parts) or "(tool returned no content)"
    except Exception as e:
        return f"Tool error: {e}"


def run_chat(history):
    """Send the conversation to the model, executing any tool calls it requests."""
    llm, _ = get_clients()
    tools = get_tool_specs()
    msgs = [{"role": "system", "content": SYSTEM_PROMPT}] + history
    kwargs = {"tools": tools} if tools else {}

    for _ in range(MAX_TOOL_ROUNDS):
        resp = llm.chat.completions.create(model=MODEL_ENDPOINT, messages=msgs, **kwargs)
        msg = resp.choices[0].message
        if not msg.tool_calls:
            return msg.content or ""

        msgs.append(
            {
                "role": "assistant",
                "content": msg.content or "",
                "tool_calls": [
                    {
                        "id": tc.id,
                        "type": "function",
                        "function": {"name": tc.function.name, "arguments": tc.function.arguments},
                    }
                    for tc in msg.tool_calls
                ],
            }
        )
        for tc in msg.tool_calls:
            try:
                args = json.loads(tc.function.arguments or "{}")
            except json.JSONDecodeError:
                args = {}
            with st.status(f"Calling tool `{tc.function.name}`", expanded=False) as status:
                st.json(args)
                output = call_mcp_tool(tc.function.name, args)
                st.code(output[:3000])
                status.update(state="error" if output.startswith("Tool error") else "complete")
            msgs.append({"role": "tool", "tool_call_id": tc.id, "content": output[:MAX_TOOL_RESULT_CHARS]})

    return "I stopped after too many tool calls. Please try rephrasing your question."


# ---- Chat UI ----
if "messages" not in st.session_state:
    st.session_state["messages"] = [{"role": "assistant", "content": "How can I help you?"}]

with st.sidebar:
    if st.button("Clear chat"):
        st.session_state["messages"] = [{"role": "assistant", "content": "How can I help you?"}]
        st.rerun()
    try:
        st.caption("MCP tools: " + ", ".join(t["function"]["name"] for t in get_tool_specs()))
    except Exception as e:
        st.caption(f"Could not list MCP tools: {e}")

for msg in st.session_state.messages:
    st.chat_message(msg["role"]).write(msg["content"])

if prompt := st.chat_input("Ask about a contract, e.g. 'Look up PIID AGSPECD170025'"):
    st.session_state.messages.append({"role": "user", "content": prompt})
    st.chat_message("user").write(prompt)

    with st.chat_message("assistant"):
        try:
            answer = run_chat(st.session_state.messages)
        except Exception as e:
            answer = f"Chat failed: {e}"
            st.code(traceback.format_exc())
        st.write(answer)
    st.session_state.messages.append({"role": "assistant", "content": answer})
