

# server/tools/__init__.py
from importlib import import_module
from pathlib import Path
import inspect

def _register(mcp_server, tool_obj):
    # FastMCP commonly exposes add_tool; some builds use register_tool
    if hasattr(mcp_server, "add_tool"):
        mcp_server.add_tool(tool_obj)
    elif hasattr(mcp_server, "register_tool"):
        mcp_server.register_tool(tool_obj)
    else:
        raise RuntimeError("FastMCP server does not support add_tool/register_tool")

def load_tools(mcp_server):
    """
    Auto-load and register tools from this package.

    Supports:
      - Explicit exports: TOOL (single) or TOOLS (list)
      - Object-style tools (Tool instances)
      - Decorator-style callable tools (functions with FastMCP markers)
    """
    tools_dir = Path(__file__).parent

    for file in tools_dir.glob("*.py"):
        if file.name == "__init__.py":
            continue

        module_name = f"{__name__}.{file.stem}"  # e.g., server.tools.fetch_usaspending_piid
        module = import_module(module_name)

        # 1) Explicit exports
        if hasattr(module, "TOOLS"):
            for t in getattr(module, "TOOLS"):
                _register(mcp_server, t)
        if hasattr(module, "TOOL"):
            _register(mcp_server, getattr(module, "TOOL"))

        # 2) Object-style Tool instances
        for attr_name, attr_val in vars(module).items():
            if getattr(attr_val.__class__, "__name__", "") == "Tool":
                _register(mcp_server, attr_val)

        # 3) Decorator-style callable tools (FastMCP markers vary by version)
        for attr_name, attr_val in vars(module).items():
            if inspect.isfunction(attr_val) and (
                getattr(attr_val, "_fastmcp_tool", False) or
                getattr(attr_val, "__fastmcp_tool__", False)
            ):
                _register(mcp_server, attr_val)
