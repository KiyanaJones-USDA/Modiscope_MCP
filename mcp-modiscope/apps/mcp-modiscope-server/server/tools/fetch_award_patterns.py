# server/tools/fetch_award_patterns.py
"""MCP tool: fetch contract pricing patterns from USASpending for a NAICS + PSC.

Returns the real-world pricing-type distribution (FFP, T&M, CPFF ...) for
federal awards matching the given NAICS code and PSC code, filtered to the
same procurement size tier as the award_amount.  Results are cached in a
Unity Catalog volume for 30 days.

Usage (via MCP agent):
  - Standalone: "What pricing type is typical for NAICS 541330, PSC C211?"
  - Automatic:  called by fetch_usaspending_piid when create_simulation=True
"""
import io
import json
import logging
import os
from datetime import datetime, timedelta, timezone

import requests
from databricks.sdk.errors import NotFound
from fastmcp.tools import Tool

from server.utils import get_workspace_client

logger = logging.getLogger(__name__)

CACHE_DIR = os.environ.get(
    "USASPENDING_CACHE_DIR",
    "/Volumes/users/kiyana_jones/bronze/usaspending_cache",
).rstrip("/") + "/patterns"

CACHE_TTL_DAYS = 30

# ---------------------------------------------------------------------------
# Award-size tiers (aligned with federal procurement thresholds)
# ---------------------------------------------------------------------------
_TIERS = [
    ("micro",  0,          250_000),
    ("small",  250_000,    2_000_000),
    ("medium", 2_000_000,  10_000_000),
    ("large",  10_000_000, 100_000_000),
    ("major",  100_000_000, None),
]


def _award_tier(amount):
    amt = float(amount or 0)
    for name, lo, hi in _TIERS:
        if hi is None or amt <= hi:
            return name, lo, hi
    return _TIERS[-1]


# ---------------------------------------------------------------------------
# USASpending pricing normalisation
# ---------------------------------------------------------------------------
_PRICING_NAMES = {
    "firm fixed price": "FFP", "fixed price redetermination": "FFP",
    "fixed price level of effort": "FFP",
    "fixed price with economic price adjustment": "FFP",
    "fixed price incentive": "FFP", "fixed price award fee": "FPAF",
    "time and materials": "T&M", "labor hours": "T&M",
    "cost plus fixed fee": "CPFF", "cost plus award fee": "CPAF",
    "cost plus incentive fee": "CPIF", "cost no fee": "CPFF",
    "cost sharing": "CPFF", "combination": "FFP", "other": "FFP",
    "order dependent": "FFP",
}
_PRICING_CODES = {
    "A": "FFP", "B": "FFP", "J": "FFP", "K": "FFP", "L": "FFP", "M": "FPAF",
    "R": "CPAF", "S": "CPFF", "T": "CPFF", "U": "CPFF", "V": "CPIF",
    "Y": "T&M", "Z": "T&M", "1": "FFP", "2": "FFP", "3": "FFP",
}


def _normalise_pricing(raw):
    if not raw:
        return "FFP"
    s = raw.strip()
    if s in _PRICING_CODES:
        return _PRICING_CODES[s]
    low = s.lower()
    for name, abbrev in _PRICING_NAMES.items():
        if name in low:
            return abbrev
    return "FFP"


# ---------------------------------------------------------------------------
# Volume cache (uses app service-principal via w.files)
# ---------------------------------------------------------------------------
def _cache_path(naics: str, psc: str, tier: str = "all") -> str:
    return f"{CACHE_DIR}/{str(naics)[:4]}_{str(psc)[:1].upper()}_{tier}.json"


def _read_cache(path: str) -> dict | None:
    w = get_workspace_client()
    try:
        raw = w.files.download(path).contents.read()
    except NotFound:
        return None
    entry = json.loads(raw)
    expires = datetime.fromisoformat(entry["expires_at"])
    if expires.tzinfo is None:  # older cache files (written by the notebook) are naive UTC
        expires = expires.replace(tzinfo=timezone.utc)
    if expires <= datetime.now(timezone.utc):
        try:
            w.files.delete(path)
        except Exception as e:
            logger.warning("Could not delete expired cache %s: %s", path, e)
        return None
    return entry


def _write_cache(path: str, data: dict) -> None:
    now = datetime.now(timezone.utc)
    data["fetched_at"] = now.isoformat()
    data["expires_at"] = (now + timedelta(days=CACHE_TTL_DAYS)).isoformat()
    get_workspace_client().files.upload(
        path,
        io.BytesIO(json.dumps(data, default=str).encode("utf-8")),
        overwrite=True,
    )


# ---------------------------------------------------------------------------
# USASpending API helpers
# ---------------------------------------------------------------------------
_API_URL = "https://api.usaspending.gov/api/v2/search/spending_by_award/"
_API_FIELDS = [
    "Award ID", "Award Amount", "Type of Contract Pricing",
    "NAICS Code", "NAICS Description",
    "PSC Code", "Product or Service Code (PSC) Description",
]
MIN_SAMPLE = 10


def _query(naics_codes, psc_code=None, lo=None, hi=None):
    filters = {"naics_codes": naics_codes, "award_type_codes": ["A", "B", "C", "D"]}
    if psc_code and psc_code.isalpha():
        filters["psc_codes"] = [psc_code]
    if lo is not None or hi is not None:
        amt_f = {}
        if lo:  amt_f["lower_bound"] = lo
        if hi:  amt_f["upper_bound"] = hi
        filters["award_amounts"] = [amt_f]
    payload = {"filters": filters, "fields": _API_FIELDS,
               "limit": 100, "page": 1, "sort": "Award Amount", "order": "desc"}
    r = requests.post(_API_URL, json=payload, timeout=30)
    r.raise_for_status()
    body = r.json()
    return body.get("results", []), body.get("page_metadata", {}).get("total", 0)


def _aggregate(results):
    pricing_counts, amounts = {}, []
    naics_desc = psc_desc = None
    for a in results:
        abbr = _normalise_pricing(a.get("Type of Contract Pricing"))
        pricing_counts[abbr] = pricing_counts.get(abbr, 0) + 1
        amt = a.get("Award Amount")
        if amt and float(amt) > 0:
            amounts.append(float(amt))
        if not naics_desc:
            naics_desc = a.get("NAICS Description")
        if not psc_desc:
            psc_desc = a.get("Product or Service Code (PSC) Description")
    return pricing_counts, amounts, naics_desc, psc_desc


# ---------------------------------------------------------------------------
# Main tool function
# ---------------------------------------------------------------------------
def fetch_award_patterns(
    naics: str,
    psc: str,
    award_amount: float | None = None,
) -> dict:
    """Fetch contract pricing patterns from USASpending for a NAICS + PSC.

    Returns the dominant pricing type (FFP, T&M, CPFF ...) and distribution
    for similar federal awards, filtered to the same procurement size tier.
    Results are cached for 30 days.

    Progressive broadening:
      1. NAICS 4-digit + PSC + size tier  (most specific)
      2. NAICS 4-digit + PSC, all sizes   (drop tier)
      3. NAICS 2-digit sector, all sizes   (broadest)

    Args:
        naics: NAICS code (e.g. "541330").
        psc:   PSC code (e.g. "C211").
        award_amount: Optional dollar amount to select the right size tier.
    """
    naics = str(naics or "").strip()
    psc = str(psc or "").strip().upper()
    if not naics or not psc:
        return {"error": "Both naics and psc are required"}

    if award_amount:
        tier_name, tier_lo, tier_hi = _award_tier(award_amount)
    else:
        tier_name, tier_lo, tier_hi = "all", None, None

    path = _cache_path(naics, psc, tier_name)
    cached = _read_cache(path)
    if cached:
        cached["_cache"] = {"hit": True, "expires_at": cached.get("expires_at")}
        return cached

    naics_4, psc_1 = naics[:4], psc[:1]
    results = None
    query_desc = ""

    try:
        # Pass 1: NAICS 4-digit + PSC + size tier
        if tier_lo is not None or tier_hi is not None:
            results, total = _query([naics_4], psc_1, tier_lo, tier_hi)
            query_desc = f"NAICS {naics_4} + PSC {psc_1} + tier {tier_name}"

        # Pass 2: drop size tier
        if not results or len(results) < MIN_SAMPLE:
            results, total = _query([naics_4], psc_1)
            query_desc = f"NAICS {naics_4} + PSC {psc_1} (all sizes)"

        # Pass 3: broaden to 2-digit sector
        if not results or len(results) < MIN_SAMPLE:
            results, total = _query([naics[:2]])
            query_desc = f"NAICS sector {naics[:2]} (all sizes)"
    except Exception as e:
        logger.warning("USASpending API error for %s/%s: %s", naics, psc, e)
        return {"error": f"USASpending API error: {e}"}

    if not results:
        return {"error": f"No awards found for NAICS {naics} / PSC {psc}"}

    pricing_counts, amounts, naics_desc, psc_desc = _aggregate(results)
    n = sum(pricing_counts.values())
    dominant = max(pricing_counts, key=pricing_counts.get)
    sa = sorted(amounts) if amounts else []

    pattern = {
        "naics_queried": naics_4,
        "psc_queried": psc_1,
        "tier": tier_name,
        "query": query_desc,
        "sample_size": len(results),
        "total_matched": total,
        "pricing_type": dominant,
        "pricing_distribution": {
            k: {"count": v, "pct": round(v / n * 100, 1)}
            for k, v in sorted(pricing_counts.items(), key=lambda x: -x[1])
        },
        "naics_description": naics_desc,
        "psc_description": psc_desc,
        "award_stats": {
            "min": sa[0] if sa else None,
            "max": sa[-1] if sa else None,
            "median": sa[len(sa) // 2] if sa else None,
            "mean": round(sum(sa) / len(sa), 2) if sa else None,
            "count": len(sa),
        },
    }

    try:
        _write_cache(path, pattern)
    except Exception as e:
        logger.warning("Cache write failed for %s/%s: %s", naics, psc, e)

    pattern["_cache"] = {"hit": False, "ttl_days": CACHE_TTL_DAYS}
    return pattern


# ---------------------------------------------------------------------------
# Tool registration (auto-discovered by __init__.py)
# ---------------------------------------------------------------------------
fetch_award_patterns_tool = Tool.from_function(
    fetch_award_patterns,
    name="fetch_award_patterns",
    description=(
        "Fetch contract pricing patterns from USASpending for a NAICS code + PSC code. "
        "Returns the dominant pricing type (FFP, T&M, CPFF, etc.) and distribution "
        "based on similar federal awards, filtered to the same size tier when an "
        "award_amount is provided. Use when asked about typical pricing for a type "
        "of federal contract, or to validate a pricing assumption."
    ),
)

TOOLS = [fetch_award_patterns_tool]
