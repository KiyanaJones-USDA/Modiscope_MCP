# server/tools/simulation_analysis.py
"""MCP tools for what-if contract simulation:

  create_scenario               -> what-if scenario from a real USAspending award
  compare_scenario_to_baseline  -> baseline (award as-is) vs scenario metrics + per-period CLIN view
  generate_risk_alerts          -> funding, period-of-performance (POP) and burn-rate checks
  explain_impact                -> plain-language impact summary (LLM if available, rules otherwise)

Baseline = the award exactly as reported by USAspending.
Scenario = baseline with what-if levers applied:
  funding_change_pct   (e.g. -15 = 15% funding cut)
  pop_extension_months (e.g. 6 = extend POP 6 months past last option)
  burn_rate_multiplier (e.g. 1.3 = spending 30% faster than today)
  as_of_date           (evaluation date; default today)

Assumption: USAspending only gives cumulative outlays *today*. If as_of_date is before
the POP end, outlays are prorated linearly to as_of_date (flagged in the output).
"""
import os
from datetime import date, datetime, timedelta, timezone

from dateutil.relativedelta import relativedelta
from fastmcp.tools import Tool

from server.tools.create_simulation_scenario import build_scenario, load_scenario, save_scenario
from server.tools.fetch_usaspending_piid import fetch_usaspending_piid
from server.tools.generate_synthetic_clins import load_clins
from server.utils import get_workspace_client

DAYS_PER_MONTH = 30.4375
CHAT_MODEL_ENDPOINT = os.environ.get("CHAT_MODEL_ENDPOINT", "databricks-claude-sonnet-4-6")


def _d(s):
    return date.fromisoformat(str(s)[:10]) if s else None


def _months(a: date, b: date) -> float:
    return max(0.0, (b - a).days / DAYS_PER_MONTH)


def _r(x, n=2):
    return None if x is None else round(float(x), n)


# ---------------------------------------------------------------------------
# Tool 1: create_scenario
# ---------------------------------------------------------------------------
def create_scenario(piid: str, funding_change_pct: float = 0.0, pop_extension_months: int = 0,
                    burn_rate_multiplier: float = 1.0, as_of_date: str | None = None,
                    scenario_name: str | None = None) -> dict:
    """Create a what-if simulation scenario from a real USAspending award (by PIID).

    Args:
        piid: contract PIID, e.g. "AGSPECD170025".
        funding_change_pct: funding change in percent (-15 = 15% cut, 10 = 10% plus-up).
        pop_extension_months: months to extend the POP beyond the last option end date.
        burn_rate_multiplier: spending speed vs current burn rate (1.3 = 30% faster).
        as_of_date: evaluation date YYYY-MM-DD (default today).
        scenario_name: optional label.
    """
    piid = str(piid or "").strip().upper()
    resp = fetch_usaspending_piid(piid)
    results = resp.get("results") or []
    if not results:
        return {"error": f"No USAspending award found for PIID {piid}", "details": resp.get("error")}
    params = {
        "scenario_name": scenario_name or f"{piid} what-if",
        "funding_change_pct": float(funding_change_pct),
        "pop_extension_months": int(pop_extension_months),
        "burn_rate_multiplier": float(burn_rate_multiplier),
        "as_of_date": as_of_date or date.today().isoformat(),
    }
    scenario = build_scenario(piid, results[0], params)
    path = save_scenario(scenario)
    return {
        "scenario_id": scenario["scenario_id"], "piid": piid,
        "recipient_name": scenario["recipient_name"], "awarding_agency": scenario["awarding_agency"],
        "naics": f"{scenario['naics']} {scenario['naics_description'] or ''}".strip(),
        "psc": f"{scenario['psc']} {scenario['psc_description'] or ''}".strip(),
        "award_amount": float(scenario["award_amount"]),
        "total_outlays": float(scenario["total_outlays"] or 0),
        "pop": {"start": scenario["pop_start_date"], "current_end": scenario["pop_current_end_date"],
                "potential_end": scenario["pop_potential_end_date"]},
        "what_if": params, "expires_at": scenario["expires_at"], "path": path,
    }


# ---------------------------------------------------------------------------
# Metrics engine (shared)
# ---------------------------------------------------------------------------
def _metrics(s: dict, apply_what_if: bool) -> dict:
    p = s.get("parameters") or {}
    as_of = _d(p.get("as_of_date")) or date.today()
    start = _d(s["pop_start_date"])
    base_end = _d(s.get("pop_potential_end_date")) or _d(s["pop_current_end_date"])
    award = float(s["award_amount"])
    outlays_now = float(s.get("total_outlays") or 0)

    funded, end, mult = award, base_end, 1.0
    if apply_what_if:
        funded = award * (1 + float(p.get("funding_change_pct", 0)) / 100)
        end = base_end + relativedelta(months=int(p.get("pop_extension_months", 0)))
        mult = float(p.get("burn_rate_multiplier", 1.0))

    # Outlays as of the evaluation date (prorated if as_of is inside the original POP)
    observed_end = min(date.today(), base_end)
    obs_months = _months(start, observed_end)
    elapsed = _months(start, min(as_of, base_end))
    prorated = as_of < observed_end and obs_months > 0
    outlays = outlays_now * (elapsed / obs_months) if prorated else outlays_now

    actual_burn = outlays / elapsed if elapsed > 0 else 0.0
    burn = actual_burn * mult
    total_months = _months(start, end) or 1.0
    planned_burn = funded / total_months
    remaining = _months(max(as_of, start), end)
    eac = outlays + burn * remaining                      # estimate at completion
    balance = funded - outlays
    runway = balance / burn if burn > 0 else None
    exhaust = (max(as_of, start) + timedelta(days=runway * DAYS_PER_MONTH)) if runway is not None else None
    if exhaust and exhaust > end:
        exhaust = None  # funds outlast the POP -> no exhaustion date to report
    return {
        "as_of_date": as_of.isoformat(), "pop_start": start.isoformat(), "pop_end": end.isoformat(),
        "days_remaining": (end - as_of).days, "pop_ended": as_of > end,
        "funded_amount": _r(funded), "outlays_to_date": _r(outlays), "outlays_prorated": prorated,
        "unspent_balance": _r(balance),
        "pct_funds_spent": _r(100 * outlays / funded if funded else 0, 1),
        "pct_time_elapsed": _r(100 * min(1.0, elapsed / total_months), 1),
        "monthly_burn_rate": _r(burn), "planned_monthly_burn": _r(planned_burn),
        "burn_ratio": _r(burn / planned_burn if planned_burn else 0, 2),
        "months_remaining": _r(remaining, 1),
        "estimate_at_completion": _r(eac), "projected_variance": _r(funded - eac),
        "funds_exhausted_date": exhaust.isoformat() if exhaust else None,
        "extension_months": int(p.get("pop_extension_months", 0)) if apply_what_if else 0,
    }


def _load(scenario_id):
    s = load_scenario(scenario_id)
    if not s:
        raise ValueError(f"Scenario {scenario_id} not found")
    return s


# ---------------------------------------------------------------------------
# Tool 2: compare_scenario_to_baseline
# ---------------------------------------------------------------------------
def _compare(s: dict) -> dict:
    base, scen = _metrics(s, False), _metrics(s, True)
    keys = ["funded_amount", "pop_end", "monthly_burn_rate", "burn_ratio", "estimate_at_completion",
            "projected_variance", "funds_exhausted_date", "days_remaining", "pct_funds_spent"]
    deltas = {}
    for k in keys:
        b, c = base[k], scen[k]
        if isinstance(b, (int, float)) and isinstance(c, (int, float)):
            deltas[k] = {"baseline": b, "scenario": c, "change": _r(c - b)}
        else:
            deltas[k] = {"baseline": b, "scenario": c}

    periods = []
    clins = load_clins(s["scenario_id"]) or []
    if clins:
        factor = scen["funded_amount"] / base["funded_amount"] if base["funded_amount"] else 1
        agg = {}
        for c in clins:
            a = agg.setdefault(c["option_year_series"], {"series": c["option_year_series"], "period": c.get("period"),
                               "pop_start": c["pop_start_date"], "pop_end": c["pop_end_date"], "baseline": 0.0})
            a["baseline"] += float(c["obligated_amount"])
        for a in agg.values():
            a["scenario"] = _r(a["baseline"] * factor)
            a["baseline"] = _r(a["baseline"])
            a["change"] = _r(a["scenario"] - a["baseline"])
            periods.append(a)
        if scen["extension_months"]:
            ext_cost = scen["monthly_burn_rate"] * scen["extension_months"]
            periods.append({"series": "EXT", "period": f"POP extension ({scen['extension_months']} mo, unfunded)",
                            "pop_start": (_d(base["pop_end"]) + timedelta(days=1)).isoformat(),
                            "pop_end": scen["pop_end"], "baseline": 0.0,
                            "scenario": _r(ext_cost), "change": _r(ext_cost)})
    return {"scenario_id": s["scenario_id"], "piid": s["piid"], "what_if": s.get("parameters"),
            "baseline": base, "scenario": scen, "deltas": deltas, "periods": periods,
            "clins_available": bool(clins)}


def compare_scenario_to_baseline(scenario_id: str) -> dict:
    """Compare a what-if scenario to the baseline award: funding, POP, burn rate,
    estimate at completion, and (if CLINs were generated) obligations per option period.

    Args:
        scenario_id: id returned by create_scenario.
    """
    try:
        return _compare(_load(scenario_id))
    except ValueError as e:
        return {"error": str(e)}


# ---------------------------------------------------------------------------
# Tool 3: generate_risk_alerts
# ---------------------------------------------------------------------------
def _alert(category, severity, title, detail, recommendation):
    return {"category": category, "severity": severity, "title": title,
            "detail": detail, "recommendation": recommendation}


def _alerts_for(m: dict) -> list[dict]:
    out = []
    funded, eac = m["funded_amount"], m["estimate_at_completion"]
    # ---- Funding ----
    if eac > funded:
        over = eac - funded
        out.append(_alert("FUNDING", "HIGH", f"Projected overrun ${over:,.0f} ({100 * over / funded:.1f}%)",
                          f"Estimate at completion ${eac:,.0f} exceeds funded ${funded:,.0f}.",
                          "Request incremental funding or descope before funds run out."))
    if m["unspent_balance"] < 0 and not m["pop_ended"]:
        out.append(_alert("FUNDING", "HIGH", f"Spending already exceeds funding by ${-m['unspent_balance']:,.0f}",
                          f"Outlays to date ${m['outlays_to_date']:,.0f} vs funded ${funded:,.0f} as of {m['as_of_date']}.",
                          "Stop work on unfunded effort and notify the CO immediately (Anti-Deficiency Act risk)."))
    elif m["funds_exhausted_date"] and m["funds_exhausted_date"] < m["pop_end"] and not m["pop_ended"]:
        gap = (_d(m["pop_end"]) - _d(m["funds_exhausted_date"])).days
        out.append(_alert("FUNDING", "HIGH", f"Funds exhausted {gap} days before POP end",
                          f"At ${m['monthly_burn_rate']:,.0f}/month funds run out on {m['funds_exhausted_date']}; "
                          f"POP ends {m['pop_end']}.",
                          "Notify the CO; plan a funding modification (Anti-Deficiency Act risk)."))
    if funded and (funded - eac) / funded > 0.20:
        out.append(_alert("FUNDING", "MEDIUM", f"${funded - eac:,.0f} projected unspent at completion",
                          f"Only {100 * eac / funded:.0f}% of funds are projected to be used.",
                          "Review for deobligation / reallocation before funds expire."))
    # ---- POP ----
    if m["pop_ended"]:
        sev = "HIGH" if m["unspent_balance"] > 0 else "LOW"
        out.append(_alert("POP", sev, "Period of performance has ended",
                          f"POP ended {m['pop_end']}; unliquidated balance ${m['unspent_balance']:,.0f}.",
                          "Start closeout and deobligate remaining funds."))
    elif m["days_remaining"] <= 30:
        out.append(_alert("POP", "HIGH", f"POP ends in {m['days_remaining']} days",
                          f"POP end {m['pop_end']}.", "Exercise option / extension now or prepare follow-on."))
    elif m["days_remaining"] <= 120:
        out.append(_alert("POP", "MEDIUM", f"POP ends in {m['days_remaining']} days",
                          f"POP end {m['pop_end']}.", "Start option exercise or follow-on acquisition planning."))
    if m["extension_months"]:
        sev = "HIGH" if m["extension_months"] > 6 else "MEDIUM"
        out.append(_alert("POP", sev, f"{m['extension_months']}-month extension beyond last option",
                          "Extending past the potential end date needs a modification.",
                          "Up to 6 months may use FAR 52.217-8; longer needs a J&A / new action."))
    # ---- Burn rate ----
    br = m["burn_ratio"]
    if m["pop_ended"]:
        br = 1.0  # spending pace is not actionable once the POP has ended
    if br > 1.25:
        out.append(_alert("BURN_RATE", "HIGH", f"Burn rate {br:.2f}x plan",
                          f"${m['monthly_burn_rate']:,.0f}/mo vs planned ${m['planned_monthly_burn']:,.0f}/mo.",
                          "Validate invoices and staffing; consider a spend plan cap."))
    elif br > 1.10:
        out.append(_alert("BURN_RATE", "MEDIUM", f"Burn rate {br:.2f}x plan",
                          f"${m['monthly_burn_rate']:,.0f}/mo vs planned ${m['planned_monthly_burn']:,.0f}/mo.",
                          "Monitor monthly; confirm work is ahead of schedule, not over cost."))
    elif 0 < br < 0.75:
        out.append(_alert("BURN_RATE", "MEDIUM", f"Under-burning at {br:.2f}x plan",
                          f"${m['monthly_burn_rate']:,.0f}/mo vs planned ${m['planned_monthly_burn']:,.0f}/mo "
                          f"({m['pct_funds_spent']}% spent, {m['pct_time_elapsed']}% of time elapsed).",
                          "Check for delivery delays; candidate for deobligation or reallocation."))
    return out


def _risk(s: dict) -> dict:
    base, scen = _metrics(s, False), _metrics(s, True)
    # Alert type = category + its (fixed) recommendation text; numbers in titles vary by scenario
    base_types = {(a["category"], a["recommendation"]) for a in _alerts_for(base)}
    alerts = _alerts_for(scen)
    for a in alerts:
        a["new_vs_baseline"] = (a["category"], a["recommendation"]) not in base_types
    order = {"HIGH": 0, "MEDIUM": 1, "LOW": 2}
    alerts.sort(key=lambda a: order[a["severity"]])
    overall = alerts[0]["severity"] if alerts else "NONE"
    return {"scenario_id": s["scenario_id"], "piid": s["piid"], "overall_risk": overall,
            "alert_count": len(alerts), "alerts": alerts,
            "checks": ["FUNDING", "POP", "BURN_RATE"], "metrics": scen,
            "evaluated_at": datetime.now(timezone.utc).isoformat()}


def generate_risk_alerts(scenario_id: str) -> dict:
    """Run risk checks on a scenario: FUNDING (overrun, funds exhausted before POP end,
    unspent funds), POP (ended / ending soon / extension beyond last option) and BURN_RATE
    (over- or under-spending vs plan). Each alert has severity, detail and recommendation,
    and is flagged if it is new compared to the baseline award.

    Args:
        scenario_id: id returned by create_scenario.
    """
    try:
        return _risk(_load(scenario_id))
    except ValueError as e:
        return {"error": str(e)}


# ---------------------------------------------------------------------------
# Tool 4: explain_impact
# ---------------------------------------------------------------------------
def _rule_based_explanation(s, cmp, risk) -> str:
    b, c, p = cmp["baseline"], cmp["scenario"], s.get("parameters") or {}
    levers = []
    if p.get("funding_change_pct"):
        levers.append(f"funding {p['funding_change_pct']:+.0f}%")
    if p.get("pop_extension_months"):
        levers.append(f"POP extended {p['pop_extension_months']} months")
    if p.get("burn_rate_multiplier", 1) != 1:
        levers.append(f"spending {p['burn_rate_multiplier']}x faster than the contractor's actual burn rate")
    lines = [
        f"Scenario for {s['piid']} ({s.get('recipient_name')}), evaluated as of {c['as_of_date']}: "
        f"{', '.join(levers) or 'no changes (baseline)'}.",
        f"Funding moves from ${b['funded_amount']:,.0f} to ${c['funded_amount']:,.0f}; projected cost at completion "
        f"moves from ${b['estimate_at_completion']:,.0f} to ${c['estimate_at_completion']:,.0f}, leaving a "
        f"{'surplus' if c['projected_variance'] >= 0 else 'shortfall'} of ${abs(c['projected_variance']):,.0f}.",
        f"Monthly burn is ${c['monthly_burn_rate']:,.0f} ({c['burn_ratio']:.2f}x plan); POP ends {c['pop_end']}"
        + (f", but funds run out {c['funds_exhausted_date']}." if c['funds_exhausted_date'] and c['funds_exhausted_date'] < c['pop_end'] else "."),
        f"Overall risk: {risk['overall_risk']} ({risk['alert_count']} alert(s)).",
    ]
    for a in risk["alerts"]:
        lines.append(f"- [{a['severity']}] {a['category']}: {a['title']}. {a['recommendation']}")
    return "\n".join(lines)


def explain_impact(scenario_id: str, use_llm: bool = True) -> dict:
    """Explain, in plain language for a contracting officer / program manager, the impact of a
    what-if scenario versus the baseline award, including the risk alerts and recommended actions.

    Args:
        scenario_id: id returned by create_scenario.
        use_llm: use the Databricks-hosted model for the narrative (falls back to rules).
    """
    try:
        s = _load(scenario_id)
    except ValueError as e:
        return {"error": str(e)}
    cmp, risk = _compare(s), _risk(s)
    facts = _rule_based_explanation(s, cmp, risk)
    explanation, source = facts, "rules"
    if use_llm:
        try:
            client = get_workspace_client().serving_endpoints.get_open_ai_client()
            resp = client.chat.completions.create(
                model=CHAT_MODEL_ENDPOINT, max_tokens=1500, temperature=0.2,
                messages=[
                    {"role": "system", "content": "You are a federal contract financial analyst. Using ONLY the facts "
                     "given, write a concise impact briefing for a contracting officer: 1) what changed, "
                     "2) financial impact, 3) schedule (POP) impact, 4) top risks, 5) recommended actions. "
                     "Use short bullets and dollar figures. Do not invent numbers."},
                    {"role": "user", "content": facts},
                ])
            explanation, source = resp.choices[0].message.content, f"llm:{CHAT_MODEL_ENDPOINT}"
        except Exception as e:  # no endpoint access -> deterministic text
            source = f"rules (LLM unavailable: {type(e).__name__})"
    return {"scenario_id": scenario_id, "piid": s["piid"], "overall_risk": risk["overall_risk"],
            "explanation": explanation, "explanation_source": source,
            "key_numbers": cmp["deltas"], "alerts": risk["alerts"]}


create_scenario_tool = Tool.from_function(create_scenario)
compare_scenario_to_baseline_tool = Tool.from_function(compare_scenario_to_baseline)
generate_risk_alerts_tool = Tool.from_function(generate_risk_alerts)
explain_impact_tool = Tool.from_function(explain_impact)
TOOLS = [create_scenario_tool, compare_scenario_to_baseline_tool, generate_risk_alerts_tool, explain_impact_tool]
