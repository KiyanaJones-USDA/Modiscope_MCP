# server/tools/create_simulation_scenario.py
"""Private helper: build and store a simulation scenario from a USAspending award.

This module is intentionally NOT an MCP tool (no Tool objects, no TOOLS list), so
load_tools() imports it but registers nothing. It only runs when
fetch_usaspending_piid is called with create_simulation=True.

Storage: one JSON file per scenario in a UC volume (no SQL warehouse needed):
    <SIMULATION_DIR>/scenarios/<scenario_id>.json
A notebook loads these files into users.kiyana_jones.main_simulation_scenarios.
"""
import io
import json
import os
import uuid
from datetime import datetime, timedelta, timezone

from server.utils import get_workspace_client

SIMULATION_DIR = os.environ.get(
    "SIMULATION_DIR",
    "/Volumes/users/kiyana_jones/bronze/simulations",
).rstrip("/")
SIMULATION_TTL_DAYS = int(os.environ.get("SIMULATION_TTL_DAYS", "7"))  # deleted by cleanup_simulation_runs job


def _code_desc(value) -> tuple[str | None, str | None]:
    """USAspending returns NAICS/PSC as {code, description}; tolerate plain strings."""
    if isinstance(value, dict):
        return value.get("code"), value.get("description")
    return value, None


def _money(value) -> str | None:
    """Keep money as a 2-decimal string so the notebook can cast to DECIMAL(18,2) exactly."""
    return None if value is None else f"{float(value):.2f}"


def build_scenario(piid: str, award: dict, parameters: dict | None = None) -> dict:
    """Map one USAspending spending_by_award result to a main_simulation_scenarios row.

    Pure Python (no Spark/SDK) so it can be reused in a notebook or another tool.
    """
    created_at = datetime.now(timezone.utc)
    naics, naics_desc = _code_desc(award.get("NAICS"))
    psc, psc_desc = _code_desc(award.get("PSC"))
    return {
        "scenario_id": str(uuid.uuid4()),
        "scenario_type": "usaspending",
        "piid": piid,
        "award_id": award.get("generated_internal_id"),
        "recipient_name": award.get("Recipient Name"),
        "awarding_agency": award.get("Awarding Agency"),
        "awarding_subagency": award.get("Awarding Sub Agency"),
        "contract_award_type": award.get("Contract Award Type"),
        "naics": naics,
        "naics_description": naics_desc,
        "psc": psc,
        "psc_description": psc_desc,
        "award_amount": _money(award.get("Award Amount")),
        "total_outlays": _money(award.get("Total Outlays")),
        # spending_by_award uses "Start Date"/"End Date"; potential end comes from the award detail call
        "pop_start_date": award.get("Start Date") or award.get("POP Start Date"),  # YYYY-MM-DD or None
        "pop_current_end_date": award.get("End Date") or award.get("POP Current End Date"),
        "pop_potential_end_date": award.get("Potential End Date"),
        "created_at": created_at.isoformat(),
        "expires_at": (created_at + timedelta(days=SIMULATION_TTL_DAYS)).isoformat(),
        "parameters": json.dumps(parameters or {}),
    }


def save_scenario(scenario: dict) -> str:
    """Write the scenario JSON to the volume as the app service principal; returns the path."""
    path = f"{SIMULATION_DIR}/scenarios/{scenario['scenario_id']}.json"
    get_workspace_client().files.upload(
        path,
        io.BytesIO(json.dumps(scenario).encode("utf-8")),
        overwrite=True,
    )
    return path


def write_json(path: str, data) -> str:
    """Write any JSON-serialisable object to a UC volume path."""
    get_workspace_client().files.upload(
        path, io.BytesIO(json.dumps(data, default=str).encode("utf-8")), overwrite=True
    )
    return path


def read_json(path: str):
    """Read a JSON file from a UC volume; returns None if it does not exist."""
    try:
        raw = get_workspace_client().files.download(path).contents.read()
    except Exception:
        return None
    return json.loads(raw)


def load_scenario(scenario_id: str) -> dict | None:
    """Load a saved scenario by id (parameters decoded to a dict)."""
    s = read_json(f"{SIMULATION_DIR}/scenarios/{scenario_id}.json")
    if s and isinstance(s.get("parameters"), str):
        s["parameters"] = json.loads(s["parameters"] or "{}")
    return s


def create_simulation_scenario(piid: str, usaspending_response: dict, parameters: dict | None = None) -> dict:
    """Build + save a scenario from a fetch_usaspending_piid response. Returns a summary dict."""
    results = usaspending_response.get("results") or []
    if not results:
        return {"created": False, "reason": f"No USAspending award found for PIID {piid}"}
    scenario = build_scenario(piid, results[0], parameters)
    path = save_scenario(scenario)
    return {
        "created": True,
        "scenario_id": scenario["scenario_id"],
        "expires_at": scenario["expires_at"],
        "path": path,
    }
