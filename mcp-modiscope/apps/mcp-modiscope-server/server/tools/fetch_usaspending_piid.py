
# server/tools/fetch_usaspending_piid.py
import io
import json
import logging
import os
import re
from datetime import datetime, timedelta, timezone

import requests
from databricks.sdk.errors import NotFound
from fastmcp.tools import Tool

from server.utils import get_workspace_client
from server.tools.create_simulation_scenario import create_simulation_scenario
from server.tools.fetch_award_patterns import fetch_award_patterns

logger = logging.getLogger(__name__)

# Cache settings: one JSON file per PIID in a Unity Catalog volume (no SQL warehouse needed)
CACHE_DIR = os.environ.get(
    "USASPENDING_CACHE_DIR",
    "/Volumes/users/kiyana_jones/bronze/usaspending_cache",
).rstrip("/")
CACHE_TTL_DAYS = int(os.environ.get("USASPENDING_CACHE_TTL_DAYS", "5"))  # expires_at = fetched_at + 5 days


def _cache_path(piid: str) -> str:
    """Volume file path for a PIID (only safe filename characters kept)."""
    safe = re.sub(r"[^A-Z0-9_-]", "_", piid)
    return f"{CACHE_DIR}/{safe}.json"


def _read_cache(piid: str) -> dict | None:
    """Return the cached payload if it exists and has not expired, else None.

    Expired files are deleted on read so the volume does not grow indefinitely.
    """
    w = get_workspace_client()  # app service principal
    path = _cache_path(piid)
    try:
        raw = w.files.download(path).contents.read()
    except NotFound:
        return None

    entry = json.loads(raw)
    expires_at = datetime.fromisoformat(entry["expires_at"])
    if expires_at <= datetime.now(timezone.utc):
        try:
            w.files.delete(path)
        except Exception as e:
            logger.warning("Could not delete expired cache file %s: %s", path, e)
        return None

    data = entry["award_data"]
    data["_cache"] = {
        "hit": True,
        "fetched_at": entry["fetched_at"],
        "expires_at": entry["expires_at"],
    }
    return data


def _write_cache(piid: str, data: dict) -> dict:
    """Write (overwrite) the PIID's cache file with expires_at = fetched_at + CACHE_TTL_DAYS."""
    fetched_at = datetime.now(timezone.utc)
    entry = {
        "piid": piid,
        "fetched_at": fetched_at.isoformat(),
        "expires_at": (fetched_at + timedelta(days=CACHE_TTL_DAYS)).isoformat(),
        "award_data": data,
    }
    get_workspace_client().files.upload(
        _cache_path(piid),
        io.BytesIO(json.dumps(entry).encode("utf-8")),
        overwrite=True,
    )
    return entry


def _get_award(piid: str) -> dict:
    """Return the USAspending response for a PIID, from the volume cache when fresh."""
    cache_enabled = bool(CACHE_DIR)

    # 1) Serve from cache if a non-expired file exists
    if cache_enabled:
        try:
            cached = _read_cache(piid)
            if cached is not None:
                return cached
        except Exception as e:  # cache problems should never block the API call
            logger.warning("Cache read failed for %s: %s", piid, e)

    url = "https://api.usaspending.gov/api/v2/search/spending_by_award/"
    payload = {
        "filters": {
            "award_type_codes": ["A", "B", "C", "D"],   # contract awards
            "award_ids": [piid]                         # exact match
        },
        "fields": [
            "Award ID",
            "Recipient Name",
            "Award Amount",  # the absolute ceiling/clin allocations must be equal to this exact dollar amount
            "Total Outlays", # the clin actual burn
            "Awarding Agency",
            "Awarding Sub Agency",
            "Contract Award Type",
            "NAICS",
            "PSC",
            "Start Date",
            "End Date"  # current end date
        ],
        "limit": 1,
        "page": 1
    }

    resp = requests.post(url, json=payload, timeout=30)
    if resp.status_code != 200:
        return {"error": f"USAspending returned {resp.status_code}", "details": resp.text}

    data = resp.json()

    # Potential end date is only available from the award detail endpoint
    results = data.get("results") or []
    if results:
        try:
            award_id = results[0]["generated_internal_id"]
            detail = requests.get(
                f"https://api.usaspending.gov/api/v2/awards/{award_id}/", timeout=30
            )
            detail.raise_for_status()
            potential = (detail.json().get("period_of_performance") or {}).get("potential_end_date")
            results[0]["Potential End Date"] = potential[:10] if potential else None
        except Exception as e:
            logger.warning("Award detail lookup failed for %s: %s", piid, e)

    # 2) Cache only successful, non-empty results (a missing PIID may appear later)
    cache_info = {"hit": False, "ttl_days": CACHE_TTL_DAYS, "enabled": cache_enabled}
    if cache_enabled and data.get("results"):
        try:
            entry = _write_cache(piid, data)
            cache_info["fetched_at"] = entry["fetched_at"]
            cache_info["expires_at"] = entry["expires_at"]
        except Exception as e:
            logger.warning("Cache write failed for %s: %s", piid, e)

    data["_cache"] = cache_info
    return data


#define fetch usaspending
def fetch_usaspending_piid(
    piid: str,
    create_simulation: bool = False,
    simulation_parameters: dict | None = None,
) -> dict:
    """Fetch award metadata from USAspending using a PIID.

    Results are cached as JSON in a Unity Catalog volume for 5 days
    (expires_at = fetched_at + 5 days).

    Args:
        piid: Procurement Instrument Identifier (contract award ID), exact match.
        create_simulation: If True, also create a simulation scenario from the award.
            Defaults to False (view award data only).
        simulation_parameters: Optional scenario settings stored with the scenario
            (e.g. number of CLINs, option years, generation method).
    """
    piid = piid.strip().upper()  # USAspending PIID match is case-sensitive
    data = _get_award(piid)

    # Scenario creation only happens when the caller explicitly asks for it
    if create_simulation and "error" not in data:
        try:
            payload = {k: v for k, v in data.items() if k != "_cache"}
            data["simulation"] = create_simulation_scenario(piid, payload, simulation_parameters)
        except Exception as e:
            logger.warning("Scenario creation failed for %s: %s", piid, e)
            data["simulation"] = {"created": False, "reason": f"Scenario creation failed: {e}"}

        # Pre-fetch pricing patterns so the CLIN notebook gets a cache hit
        try:
            results = data.get("results") or []
            if results:
                naics_val = results[0].get("NAICS") or {}
                psc_val = results[0].get("PSC") or {}
                naics_code = naics_val.get("code") if isinstance(naics_val, dict) else naics_val
                psc_code = psc_val.get("code") if isinstance(psc_val, dict) else psc_val
                award_amt = results[0].get("Award Amount")
                if naics_code and psc_code:
                    data["simulation"]["award_patterns"] = fetch_award_patterns(
                        naics=str(naics_code), psc=str(psc_code), award_amount=award_amt
                    )
        except Exception as e:
            logger.warning("Pattern pre-fetch failed for %s: %s", piid, e)

    return data


fetch_usaspending_piid_tool = Tool.from_function(
    fetch_usaspending_piid,
    name="fetch_usaspending_piid",
    description=(
        "Fetch federal contract award data from USAspending by PIID: ceiling (Award Amount), "
        "Total Outlays, agency, NAICS, PSC and POP dates. Cached for 5 days. "
        "Set create_simulation=true only when the user asks to create simulation data; "
        "by default the award is only viewed."
    ),
)

# Optional explicit export to help simple loaders
TOOLS = [fetch_usaspending_piid_tool]


