# Databricks notebook source
# MAGIC %md
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #Notes
# MAGIC from __future__ import annotations
# MAGIC
# MAGIC #Tools
# MAGIC from modiscope_server.tools import (
# MAGIC     calculate_caf_spending,
# MAGIC     calculate_clin_balance,
# MAGIC     calculate_burn_rate,
# MAGIC     simulate_pop_extend,
# MAGIC     identify_missing_clins,
# MAGIC     usaspending_award_link
# MAGIC
# MAGIC )
# MAGIC
# MAGIC
# MAGIC def register_tools(mcp):
# MAGIC     calculate_caf_spending.register(mcp)
# MAGIC     calculate_clin_balance.register(mcp)
# MAGIC     calculate_burn_rate.register(mcp)
# MAGIC     simulate_pop_extend.register(mcp)
# MAGIC     identify_missing_clins.register(mcp)
# MAGIC     usaspending_award_link.register(mcp)
# MAGIC
# MAGIC
# MAGIC
# MAGIC __all__ = ["register_tools"]

# COMMAND ----------

# MAGIC %md
# MAGIC # tools/burn_rate.py
# MAGIC from typing import Annotated, List, Dict
# MAGIC from fastmcp import FastMCP
# MAGIC
# MAGIC def register(mcp: FastMCP):
# MAGIC     @mcp.tool(
# MAGIC         name="calculate_burn_rate",
# MAGIC         description="Compute daily and monthly burn rates for a CLIN from a recent spend time series.",
# MAGIC         annotations={"title": "Burn Rate"},
# MAGIC     )
# MAGIC     async def calculate_burn_rate(
# MAGIC         contract_number: Annotated[str, "Contract #"],
# MAGIC         clin_number: Annotated[str, "CLIN #"],
# MAGIC         amounts_by_day: Annotated[List[Dict], "List of {date: 'YYYY-MM-DD', amount: float}"],
# MAGIC         window_days: Annotated[int, "Trailing window in days (e.g., 90)"] = 90
# MAGIC     ) -> dict:
# MAGIC         # Simple trailing average
# MAGIC         if not amounts_by_day:
# MAGIC             return {"contract_number": contract_number, "clin_number": clin_number,
# MAGIC                     "burn_rate_daily": 0.0, "burn_rate_monthly": 0.0, "window_days": window_days}
# MAGIC
# MAGIC         total = sum(x.get("amount", 0.0) for x in amounts_by_day)
# MAGIC         days = min(window_days, len(amounts_by_day)) or 1
# MAGIC         burn_rate_daily = total / days
# MAGIC         burn_rate_monthly = burn_rate_daily * 30.0
# MAGIC
# MAGIC         return {
# MAGIC             "contract_number": contract_number,
# MAGIC             "clin_number": clin_number,
# MAGIC             "burn_rate_daily": round(burn_rate_daily, 2),
# MAGIC             "burn_rate_monthly": round(burn_rate_monthly, 2),
# MAGIC             "window_days": days,
# MAGIC         }

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # tools/calculate_caf_fee.py
# MAGIC from typing import Annotated
# MAGIC from fastmcp import FastMCP
# MAGIC
# MAGIC def register(mcp: FastMCP):
# MAGIC
# MAGIC     @mcp.tool(
# MAGIC         name="calculate_caf_spending",
# MAGIC         description="Calculate the Contract Access Fee (CAF) based on obligation and CAF rate.",
# MAGIC         annotations={"title": "CAF Fee Calculator"},
# MAGIC     )
# MAGIC     async def calculate_caf_spending(
# MAGIC         obligation_amount: Annotated[float, "Obligated dollar amount (positive or negative)"],
# MAGIC         caf_rate: Annotated[float, "CAF rate as a percentage (e.g., 0.34 for 0.34%)"],
# MAGIC         caf_cap: Annotated[float, "Optional fee cap (0 = no cap)"] = 0.0
# MAGIC     ) -> dict:
# MAGIC         
# MAGIC         # Convert rate from % to decimal
# MAGIC         rate_decimal = caf_rate / 100.0
# MAGIC         
# MAGIC         raw_fee = obligation_amount * rate_decimal
# MAGIC         
# MAGIC         # Apply optional cap if provided (> 0)
# MAGIC         caf_fee = min(raw_fee, caf_cap) if caf_cap > 0 else raw_fee
# MAGIC         
# MAGIC         total_with_fee = obligation_amount + caf_fee
# MAGIC         
# MAGIC         return {
# MAGIC             "obligation_amount": obligation_amount,
# MAGIC             "caf_rate_percent": caf_rate,
# MAGIC             "caf_fee": round(caf_fee, 2),
# MAGIC             "total_with_fee": round(total_with_fee, 2),
# MAGIC             "cap_applied": caf_cap > 0
# MAGIC         }
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # src/modiscope_server/tools/calculate_clin_balance.py
# MAGIC from typing import Annotated
# MAGIC from fastmcp import FastMCP
# MAGIC
# MAGIC def register(mcp: FastMCP):
# MAGIC     @mcp.tool(
# MAGIC         name="calculate_clin_balance",
# MAGIC         description="Calculate CLIN balance based on obligation and total obligation.",
# MAGIC         annotations={"title": "CLIN Balances"},
# MAGIC     )
# MAGIC     async def calculate_clin_balance(
# MAGIC         contract_number: Annotated[str, "Contract number"],
# MAGIC         clin_number: Annotated[str, "CLIN number"],
# MAGIC         obligation: Annotated[float, "Obligation amount (can be positive or negative)"],
# MAGIC         total_obligation: Annotated[float, "Total obligation amount"]
# MAGIC     ) -> dict:
# MAGIC
# MAGIC         # Calculate the current balance
# MAGIC         current_balance = total_obligation - obligation
# MAGIC
# MAGIC         return {
# MAGIC             "contract_number": contract_number,
# MAGIC             "clin_number": clin_number,
# MAGIC             "obligation": obligation,
# MAGIC             "total_obligation": total_obligation,
# MAGIC             "current_balance": current_balance,
# MAGIC         }
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # tools/missing_clins.py
# MAGIC from typing import Annotated, List, Dict
# MAGIC from fastmcp import FastMCP
# MAGIC from datetime import date
# MAGIC
# MAGIC def register(mcp: FastMCP):
# MAGIC     @mcp.tool(
# MAGIC         name="identify_missing_clins",
# MAGIC         description="Identify CLINs in a requisition/quote that are not in the CLIN tracking log.",
# MAGIC         annotations={"title": "Missing CLINs"},
# MAGIC     )
# MAGIC     async def identify_missing_clins(
# MAGIC         contract_number: Annotated[str, "Contract #"],
# MAGIC         proposed_items: Annotated[List[Dict], "List of {clin_number: str, item_desc: str}"],
# MAGIC         known_clins: Annotated[List[str], "List of CLINs already on the log"],
# MAGIC         option_periods: Annotated[List[Dict], "Optional: {clin_number, pop_end:'YYYY-MM-DD'}"] = None
# MAGIC     ) -> dict:
# MAGIC         proposed = {x["clin_number"] for x in proposed_items if "clin_number" in x}
# MAGIC         known = set(known_clins)
# MAGIC         missing = sorted(list(proposed - known))
# MAGIC         present = sorted(list(proposed & known))
# MAGIC
# MAGIC         expiring = []
# MAGIC         if option_periods:
# MAGIC             today = date.today()
# MAGIC             for op in option_periods:
# MAGIC                 try:
# MAGIC                     pop_end = date.fromisoformat(op.get("pop_end"))
# MAGIC                 except Exception:
# MAGIC                     continue
# MAGIC                 if today <= pop_end <= date.fromordinal(today.toordinal() + 60):
# MAGIC                     expiring.append(op["clin_number"])
# MAGIC
# MAGIC         return {
# MAGIC             "contract_number": contract_number,
# MAGIC             "clins_on_log": present,
# MAGIC             "clins_not_detected": missing,
# MAGIC             "option_periods_expiring_within_60_days": sorted(set(expiring)),
# MAGIC             "prompt_add_new_clins": bool(missing),
# MAGIC         }
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC
# MAGIC # tools/pop_simulation.py
# MAGIC from typing import Annotated
# MAGIC from fastmcp import FastMCP
# MAGIC
# MAGIC def register(mcp: FastMCP):
# MAGIC     @mcp.tool(
# MAGIC         name="simulate_pop_extension",
# MAGIC         description="Simulate the impact on CLIN balance from extending the POP.",
# MAGIC         annotations={"title": "POP Extension Simulation"},
# MAGIC     )
# MAGIC     async def simulate_pop_extension(
# MAGIC         contract_number: Annotated[str, "Contract #"],
# MAGIC         clin_number: Annotated[str, "CLIN #"],
# MAGIC         current_balance: Annotated[float, "Current balance"],
# MAGIC         burn_rate_monthly: Annotated[float, "Monthly burn rate"],
# MAGIC         months_extension: Annotated[int, "Months to extend"]
# MAGIC     ) -> dict:
# MAGIC         projected_spend = burn_rate_monthly * months_extension
# MAGIC         post_balance = current_balance - projected_spend
# MAGIC         risk = "overspend" if post_balance < 0 else "ok"
# MAGIC
# MAGIC         return {
# MAGIC             "contract_number": contract_number,
# MAGIC             "clin_number": clin_number,
# MAGIC             "months_extension": months_extension,
# MAGIC             "projected_spend": round(projected_spend, 2),
# MAGIC             "post_extension_balance": round(post_balance, 2),
# MAGIC             "status": risk,
# MAGIC         }

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # tools/usaspending_search_by_piid.py
# MAGIC from typing import Annotated, Optional
# MAGIC from fastmcp import FastMCP
# MAGIC import httpx
# MAGIC
# MAGIC USASPENDING_API_BASE = "https://api.usaspending.gov"
# MAGIC USASPENDING_WEB_AWARD_URL = "https://www.usaspending.gov/award/{award_id}"
# MAGIC
# MAGIC def register(mcp: FastMCP):
# MAGIC
# MAGIC     @mcp.tool(
# MAGIC         name="usaspending_search_by_piid",
# MAGIC         description=(
# MAGIC             "Search USAspending using a PIID (contract number). "
# MAGIC             "Returns matching award(s) and direct public links."
# MAGIC         ),
# MAGIC         annotations={"title": "USAspending Search by PIID"},
# MAGIC     )
# MAGIC     async def usaspending_search_by_piid(
# MAGIC         piid: Annotated[str, "PIID / Contract Number"],
# MAGIC         expected_funding_agency: Annotated[str, "Agency name to match"] = "Department of Agriculture",
# MAGIC         verify_funding_agency: Annotated[bool, "Check if the award's funding agency matches expectation"] = False,
# MAGIC         limit: Annotated[int, "Max results"] = 5,
# MAGIC         timeout_seconds: Annotated[int, "HTTP timeout"] = 20
# MAGIC     ) -> dict:
# MAGIC
# MAGIC         search_payload = {
# MAGIC             "fields": ["Award ID", "Recipient Name", "Award Amount", "Awarding Agency", "Award Type"],
# MAGIC             "filters": {
# MAGIC                 "keywords": [piid]
# MAGIC             },
# MAGIC             "limit": limit,
# MAGIC             "page": 1
# MAGIC         }
# MAGIC
# MAGIC         # --- Step 1: Run PIID keyword search ---
# MAGIC         async with httpx.AsyncClient(base_url=USASPENDING_API_BASE, timeout=timeout_seconds) as client:
# MAGIC             try:
# MAGIC                 resp = await client.post("/api/v2/search/spending_by_award/", json=search_payload)
# MAGIC                 resp.raise_for_status()
# MAGIC                 data = resp.json()
# MAGIC             except Exception as e:
# MAGIC                 return {
# MAGIC                     "piid": piid,
# MAGIC                     "status": "error",
# MAGIC                     "error_detail": str(e),
# MAGIC                     "message": "Failed to query USAspending search endpoint."
# MAGIC                 }
# MAGIC
# MAGIC         results = data.get("results", [])
# MAGIC
# MAGIC         if not results:
# MAGIC             return {
# MAGIC                 "piid": piid,
# MAGIC                 "status": "no_results",
# MAGIC                 "message": "No awards found for this PIID.",
# MAGIC                 "awards": []
# MAGIC             }
# MAGIC
# MAGIC         enriched_awards = []
# MAGIC
# MAGIC         # --- Step 2: For each result, enrich with public link + optional agency check ---
# MAGIC         for award in results:
# MAGIC             award_id = award.get("Award ID") or award.get("generated_id")
# MAGIC
# MAGIC             if not award_id:
# MAGIC                 continue
# MAGIC
# MAGIC             public_link = USASPENDING_WEB_AWARD_URL.format(award_id=award_id)
# MAGIC
# MAGIC             agency_match = None
# MAGIC             actual_agency = award.get("Awarding Agency")
# MAGIC
# MAGIC             if verify_funding_agency:
# MAGIC                 if actual_agency:
# MAGIC                     agency_match = (actual_agency.strip().lower() == expected_funding_agency.strip().lower())
# MAGIC                 else:
# MAGIC                     agency_match = False
# MAGIC
# MAGIC             enriched_awards.append({
# MAGIC                 "award_id": award_id,
# MAGIC                 "recipient": award.get("Recipient Name"),
# MAGIC                 "award_amount": award.get("Award Amount"),
# MAGIC                 "awarding_agency": actual_agency,
# MAGIC                 "award_type": award.get("Award Type"),
# MAGIC                 "usaspending_link": public_link,
# MAGIC                 "funding_agency_matches": agency_match,
# MAGIC             })
# MAGIC
# MAGIC         return {
# MAGIC             "piid": piid,
# MAGIC             "status": "ok",
# MAGIC             "count": len(enriched_awards),
# MAGIC             "awards": enriched_awards
# MAGIC         }

# COMMAND ----------

# MAGIC %md
# MAGIC