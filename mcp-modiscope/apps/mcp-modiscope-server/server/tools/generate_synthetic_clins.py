# server/tools/generate_synthetic_clins.py
"""MCP tool: generate synthetic CLINs for a saved simulation scenario.

Pure-Python port of testdata_notebooks/path_a_usaspending/02_generate_synthetic_clin
(no Spark, so it runs inside the Databricks App). CLINs are written as one JSON file
per scenario: <SIMULATION_DIR>/clins/<scenario_id>.json

Rules:
  * POP split into a base period (series 1000) + option periods (1600, 1700, ...).
  * NAICS -> labor template, PSC -> extra lines, + ODC line for services.
  * Pricing type from USASpending patterns (fetch_award_patterns), fallback PSC rules.
  * award_amount split by days per segment, then by line weight; rounding remainder
    goes to the largest CLIN so totals tie out exactly.
"""
from datetime import date, timedelta
from decimal import Decimal, ROUND_HALF_UP

from dateutil.relativedelta import relativedelta
from fastmcp.tools import Tool

from server.tools.create_simulation_scenario import (
    SIMULATION_DIR, load_scenario, read_json, write_json,
)
from server.tools.fetch_award_patterns import fetch_award_patterns

_GOODS = [("Supplies/Equipment", "item", 0.85, 1250.00), ("Shipping and Handling", "lot", 0.15, None)]
_GENERIC_SERVICES = [("Program Manager", "labor", 0.20, 150.00),
                     ("Specialist", "labor", 0.50, 100.00),
                     ("Support Staff", "labor", 0.30, 60.00)]
NAICS_RULES = {
    "5413": [("Program Manager", "labor", 0.15, 185.00), ("Senior Engineer", "labor", 0.30, 165.00),
             ("Engineer II", "labor", 0.30, 125.00), ("Designer/CAD Technician", "labor", 0.15, 90.00),
             ("Field Survey Technician", "labor", 0.10, 80.00)],
    "5415": [("Project Manager", "labor", 0.15, 175.00), ("Systems Architect", "labor", 0.20, 190.00),
             ("Software Developer", "labor", 0.45, 150.00), ("Help Desk Specialist", "labor", 0.20, 75.00)],
    "5416": [("Engagement Manager", "labor", 0.20, 210.00), ("Senior Consultant", "labor", 0.45, 170.00),
             ("Analyst", "labor", 0.35, 120.00)],
    "5417": [("Principal Investigator", "labor", 0.20, 200.00), ("Research Scientist", "labor", 0.50, 155.00),
             ("Lab Technician", "labor", 0.30, 85.00)],
    "54":   [("Project Manager", "labor", 0.20, 170.00), ("Senior Professional", "labor", 0.35, 150.00),
             ("Professional", "labor", 0.30, 115.00), ("Support Staff", "labor", 0.15, 70.00)],
    "51":   [("Project Manager", "labor", 0.15, 165.00), ("Data Engineer", "labor", 0.45, 155.00),
             ("Data Analyst", "labor", 0.40, 120.00)],
    "56":   [("Site Supervisor", "labor", 0.20, 70.00), ("Technician", "labor", 0.45, 50.00),
             ("General Laborer", "labor", 0.35, 40.00)],
    "23":   [("Mobilization/Demobilization", "lot", 0.05, None), ("Craft Labor", "labor", 0.45, 75.00),
             ("Construction Materials", "lot", 0.40, None), ("Equipment Rental", "lot", 0.10, None)],
    "61":   [("Instructor", "labor", 0.50, 95.00), ("Curriculum Developer", "labor", 0.30, 110.00),
             ("Training Materials", "lot", 0.20, None)],
    "62":   [("Clinician", "labor", 0.40, 160.00), ("Registered Nurse", "labor", 0.40, 95.00),
             ("Medical Supplies", "lot", 0.20, None)],
    "31": _GOODS, "32": _GOODS, "33": _GOODS, "42": _GOODS, "44": _GOODS, "45": _GOODS,
    "11": [("Agricultural Services Labor", "labor", 0.60, 55.00), ("Agricultural Supplies", "lot", 0.40, None)],
}
PSC_RULES = {
    "A": ("Research and Development", "CPFF", [("Technical Reports and Data (CDRL)", "lot", 0.05, None)]),
    "B": ("Special Studies and Analysis", "FFP", [("Study Report", "lot", 0.05, None)]),
    "C": ("Architect and Engineering", "FFP", [("Design Deliverables and Drawings", "lot", 0.05, None)]),
    "D": ("IT and Telecom (legacy)", "T&M", [("Software Licenses", "item", 0.10, 2500.00)]),
    "J": ("Maintenance and Repair of Equipment", "FFP", [("Replacement Parts", "item", 0.15, 450.00)]),
    "R": ("Professional Support Services", "T&M", []),
    "Y": ("Construction of Structures and Facilities", "FFP", [("Bonds and Insurance", "lot", 0.02, None)]),
    "Z": ("Maintenance of Real Property", "FFP", [("Repair Materials", "lot", 0.10, None)]),
}
UOM = {"labor": "HR", "item": "EA", "lot": "LO"}
ODC_LINE = ("Other Direct Costs / Travel", "lot", 0.03, None)
CENT = Decimal("0.01")


def _naics_rule(naics):
    code = str(naics or "").strip()
    for n in range(len(code), 1, -1):
        if code[:n] in NAICS_RULES:
            return code[:n], NAICS_RULES[code[:n]]
    return "default", _GENERIC_SERVICES


def _psc_rule(psc):
    code = str(psc or "").strip().upper()
    first = code[:1]
    if first.isdigit():
        return "product", "Product (FSC)", "FFP", [(f"Product per PSC {code}", "item", 0.30, 1250.00)]
    if first in PSC_RULES:
        name, pricing, extra = PSC_RULES[first]
        return first, name, pricing, extra
    return "other_service", "Other Services", "FFP", []


def build_line_template(naics, psc, patterns=None):
    n_key, n_lines = _naics_rule(naics)
    p_key, _, fallback_pricing, p_lines = _psc_rule(psc)
    if patterns and patterns.get("pricing_type"):
        pricing, src = patterns["pricing_type"], "usaspending"
    else:
        pricing, src = fallback_pricing, "fallback"
    lines = list(n_lines)
    if p_key == "product" and any(k == "item" for _, k, _, _ in n_lines):
        p_lines = []
    lines += p_lines
    if p_key != "product":
        lines.append(ODC_LINE)
    total_w = sum(w for _, _, w, _ in lines)
    lines = [(lbl, kind, w / total_w, price) for lbl, kind, w, price in lines]
    method = f"rules_v2|naics_rule={n_key}|psc_group={p_key}|pricing={pricing}|src={src}"
    return lines, method, pricing


def make_segments(pop_start, pop_end, months_per_segment=12):
    """Contiguous base + option segments covering pop_start..pop_end (last may be a stub)."""
    segs = []
    base_end = min(pop_end, pop_start + relativedelta(months=months_per_segment) - timedelta(days=1))
    segs.append(("1000", pop_start, base_end))
    last, i = base_end, 0
    while last < pop_end:
        start = last + timedelta(days=1)
        nxt = min(pop_end, start + relativedelta(months=months_per_segment) - timedelta(days=1))
        segs.append((str(1600 + 100 * i), start, nxt))
        last, i = nxt, i + 1
    return segs


def _clin_suffix(n):
    n -= 1
    return chr(ord("A") + n // 26) + chr(ord("A") + n % 26)


def _d(s):
    return date.fromisoformat(str(s)[:10]) if s else None


def build_clins(scenario: dict, months_per_segment: int = 12, escalation_rate: float = 0.03,
                patterns: dict | None = None) -> tuple[list[dict], str, str]:
    """Pure function: scenario dict -> list of CLIN dicts (amounts as Decimal)."""
    pop_start = _d(scenario.get("pop_start_date"))
    pop_end = _d(scenario.get("pop_potential_end_date")) or _d(scenario.get("pop_current_end_date"))
    if not pop_start or not pop_end:
        raise ValueError("Scenario is missing POP start/end dates")
    template, method, pricing = build_line_template(scenario["naics"], scenario["psc"], patterns)
    segments = make_segments(pop_start, pop_end, months_per_segment)
    award_amount = Decimal(str(scenario["award_amount"]))
    total_days = sum((e - s).days + 1 for _, s, e in segments)
    psc_desc = (scenario.get("psc_description") or "").title()
    rows = []
    for idx, (series, s, e) in enumerate(segments):
        share = Decimal((e - s).days + 1) / Decimal(total_days)
        esc = Decimal(str((1 + escalation_rate) ** idx))
        period = "Base Period" if idx == 0 else f"Option Period {idx}"
        for line_no, (label, kind, weight, price) in enumerate(template, start=1):
            amt = (award_amount * share * Decimal(str(weight))).quantize(CENT, ROUND_HALF_UP)
            rows.append({
                "scenario_id": scenario["scenario_id"], "piid": scenario["piid"],
                "option_year_series": series, "clin_number": f"{series}{_clin_suffix(line_no)}",
                "period": period, "clin_description": f"{period} - {label} ({pricing}) - {psc_desc}",
                "unit_of_measure": UOM[kind],
                "unit_price": None if price is None else (Decimal(str(price)) * esc).quantize(CENT, ROUND_HALF_UP),
                "obligated_amount": amt,
                "pop_start_date": s.isoformat(), "pop_end_date": e.isoformat(),
                "generation_method": method,
            })
    remainder = award_amount - sum(r["obligated_amount"] for r in rows)
    if remainder:
        max(rows, key=lambda r: r["obligated_amount"])["obligated_amount"] += remainder
    for r in rows:
        if r["unit_of_measure"] == "LO" or r["unit_price"] is None:
            r["unit_price"] = r["obligated_amount"]
    return rows, method, pricing


def clins_path(scenario_id: str) -> str:
    return f"{SIMULATION_DIR}/clins/{scenario_id}.json"


def load_clins(scenario_id: str) -> list[dict] | None:
    data = read_json(clins_path(scenario_id))
    return data.get("clins") if data else None


def generate_synthetic_clins(scenario_id: str, months_per_segment: int = 12,
                             escalation_rate: float = 0.03) -> dict:
    """Generate synthetic CLINs (base + option periods) for a saved simulation scenario.

    Uses USASpending pricing patterns for similar awards (NAICS + PSC + size tier)
    and NAICS/PSC templates for labor categories. Totals tie out to the award amount.

    Args:
        scenario_id: id returned by create_scenario.
        months_per_segment: length of base/option periods in months (default 12).
        escalation_rate: annual unit-price escalation (default 0.03 = 3%).
    """
    scenario = load_scenario(scenario_id)
    if not scenario:
        return {"error": f"Scenario {scenario_id} not found"}
    patterns = fetch_award_patterns(scenario["naics"], scenario["psc"], float(scenario["award_amount"]))
    if patterns.get("error"):
        patterns = None
    rows, method, pricing = build_clins(scenario, months_per_segment, escalation_rate, patterns)
    path = write_json(clins_path(scenario_id), {"scenario_id": scenario_id, "clins": rows})
    by_period = {}
    for r in rows:
        p = by_period.setdefault(r["option_year_series"], {
            "period": r["period"], "pop_start": r["pop_start_date"], "pop_end": r["pop_end_date"],
            "clins": 0, "obligated": Decimal(0)})
        p["clins"] += 1
        p["obligated"] += r["obligated_amount"]
    return {
        "scenario_id": scenario_id, "piid": scenario["piid"], "clin_count": len(rows),
        "total_obligated": float(sum(r["obligated_amount"] for r in rows)),
        "award_amount": float(scenario["award_amount"]), "pricing_type": pricing,
        "pricing_source": "usaspending" if patterns else "fallback", "generation_method": method,
        "periods": [{"series": k, **{**v, "obligated": float(v["obligated"])}} for k, v in by_period.items()],
        "sample_clins": [{k: (float(v) if isinstance(v, Decimal) else v) for k, v in r.items()
                          if k in ("clin_number", "clin_description", "unit_of_measure", "unit_price", "obligated_amount")}
                         for r in rows[:8]],
        "path": path,
    }


generate_synthetic_clins_tool = Tool.from_function(generate_synthetic_clins)
TOOLS = [generate_synthetic_clins_tool]
