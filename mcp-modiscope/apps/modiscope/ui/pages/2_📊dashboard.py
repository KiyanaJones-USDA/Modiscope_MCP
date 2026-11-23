import os
import json
import traceback
import streamlit as st
from databricks.sdk import WorkspaceClient
from databricks_mcp import DatabricksMCPClient

st.header("Dashboard")
