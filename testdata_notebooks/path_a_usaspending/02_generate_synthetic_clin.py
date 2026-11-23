# Databricks notebook source
# DBTITLE 1,Review Scenarios Created by fetch_usaspending_piid
# MAGIC %sql
# MAGIC --Need to read the new scenario that was created into the scenarios table
# MAGIC -- Lists scenarios loaded by 01_setup_simulation_schema (from fetch_usaspending_piid(create_simulation=True)), newest first.
# MAGIC SELECT
# MAGIC   scenario_id, piid, recipient_name, awarding_agency,
# MAGIC   naics, naics_description, psc, psc_description,
# MAGIC   award_amount, pop_start_date, pop_current_end_date, pop_potential_end_date,
# MAGIC   created_at, expires_at,
# MAGIC   expires_at < current_timestamp() AS is_expired
# MAGIC FROM users.kiyana_jones.main_simulation_scenarios
# MAGIC ORDER BY created_at DESC
# MAGIC
# MAGIC

# COMMAND ----------

# DBTITLE 1,Load Selected Scenario
# Picks one scenario to generate CLINs for.
# scenario_id widget blank -> most recently created scenario.
from pyspark.sql import functions as F

SCENARIOS = "users.kiyana_jones.main_simulation_scenarios"
CLINS = "users.kiyana_jones.main_simulation_synthetic_clins"

dbutils.widgets.text("scenario_id", "", "Scenario ID (blank = latest)")
dbutils.widgets.text("months_per_segment", "12", "Months per segment")
dbutils.widgets.text("escalation_rate", "0.03", "Annual price escalation")

scenario_id = dbutils.widgets.get("scenario_id").strip()
months_per_segment = int(dbutils.widgets.get("months_per_segment") or 12)
escalation_rate = float(dbutils.widgets.get("escalation_rate") or 0.03)

sdf = spark.table(SCENARIOS).withColumn("is_expired", F.col("expires_at") < F.current_timestamp())
if scenario_id:
    sdf = sdf.filter(F.col("scenario_id") == scenario_id)
else:
    sdf = sdf.orderBy(F.col("created_at").desc()).limit(1)

rows = sdf.collect()
assert rows, f"No scenario found in {SCENARIOS}" + (f" for scenario_id={scenario_id}" if scenario_id else "")
scenario = rows[0].asDict()

for col in ["scenario_id", "piid", "naics", "psc", "award_amount", "pop_start_date"]:
    assert scenario[col] is not None, f"Scenario is missing required field: {col}"

# CLINs cover the full potential POP (base + all options); fall back to current end date
pop_start = scenario["pop_start_date"]
pop_end = scenario["pop_potential_end_date"] or scenario["pop_current_end_date"]
assert pop_end is not None and pop_end >= pop_start, "Scenario needs a valid POP end date"

if scenario["is_expired"]:
    print(f"WARNING: scenario {scenario['scenario_id']} expired at {scenario['expires_at']}; "
          "its CLINs will be removed by the next cleanup run.")

print(f"Scenario {scenario['scenario_id']} | PIID {scenario['piid']} | NAICS {scenario['naics']} | "
      f"PSC {scenario['psc']} | ${scenario['award_amount']:,} | POP {pop_start} -> {pop_end}")
display(sdf)

# COMMAND ----------

# DBTITLE 1,Define POP Segments (Base + Option Years)
from datetime import timedelta
from dateutil.relativedelta import relativedelta

def month_add(d, months):
    return d + relativedelta(months=months)

#define the segments
def make_segments(pop_start_date, pop_end_date, months_per_segment=12, options=None):
    """Returns [(series, start, end), ...]. Segments are contiguous and non-overlapping
    (next start = previous end + 1 day). options=None -> as many option segments as
    needed to reach pop_end_date; the last one may be shorter (e.g. a 52.217-8 extension)."""
    segs = []
    # Base period -> '1000' series
    base_end = min(pop_end_date, month_add(pop_start_date, months_per_segment) - timedelta(days=1))
    segs.append(("1000", pop_start_date, base_end))
    #options --> '1600', '1700, ...(one-year segments for demo)

    last = base_end
    i = 0
    while last < pop_end_date and (options is None or i < options):
        series = str(1600 + 100 * i)
        start = last + timedelta(days=1)
        nxt = min(pop_end_date, month_add(start, months_per_segment) - timedelta(days=1))
        segs.append((series, start, nxt))
        last = nxt
        i += 1
    return segs

segments = make_segments(pop_start, pop_end, months_per_segment)
for s in segments:
    print(s[0], s[1], "->", s[2], f"({(s[2] - s[1]).days + 1} days)")


# COMMAND ----------

# DBTITLE 1,Fetch USASpending Pricing Patterns
# ---------------------------------------------------------------------------
# Query USASpending for federal awards matching the scenario's NAICS + PSC.
# Aggregates the real-world pricing-type distribution (FFP, T&M, CPFF …) so
# CLIN generation uses data-driven guardrails — no code updates needed.
# Results are cached to UC volume with a 30-day TTL.
# ---------------------------------------------------------------------------
import requests, json, os
from datetime import datetime, timedelta as td, timezone

PATTERN_CACHE = "/Volumes/users/kiyana_jones/bronze/usaspending_cache/patterns"
PATTERN_TTL = 30  # days

# ---- USASpending pricing-type normalisation --------------------------------
# The API returns either FPDS single-letter codes or full descriptions;
# this maps both forms to the standard abbreviations used in CLIN generation.
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
_PRICING_CODES = {  # FPDS single-letter codes
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


# ---- Award-size tiers (aligned with federal procurement thresholds) --------
_TIERS = [
    ("micro",  0,          250_000),     # micro-purchases / simplified acquisition
    ("small",  250_000,    2_000_000),    # above SAT, small contracts
    ("medium", 2_000_000,  10_000_000),   # typical task orders
    ("large",  10_000_000, 100_000_000),  # large contracts
    ("major",  100_000_000, None),        # major systems / IDIQs
]

def _award_tier(amount):
    """Classify an award amount into a procurement size tier."""
    amt = float(amount or 0)
    for name, lo, hi in _TIERS:
        if hi is None or amt <= hi:
            return name, lo, hi
    return _TIERS[-1]

# ---- Volume cache helpers --------------------------------------------------
def _cache_path(naics, psc, tier="all"):
    return f"{PATTERN_CACHE}/{str(naics)[:4]}_{str(psc)[:1].upper()}_{tier}.json"

def _read_cache(path):
    try:
        with open(path) as f:
            d = json.load(f)
        if datetime.fromisoformat(d["expires_at"]) > datetime.now(timezone.utc).replace(tzinfo=None):
            return d
    except (FileNotFoundError, json.JSONDecodeError, KeyError):
        pass
    return None

def _write_cache(path, data):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    now = datetime.now(timezone.utc).replace(tzinfo=None)
    data["fetched_at"] = now.isoformat()
    data["expires_at"] = (now + td(days=PATTERN_TTL)).isoformat()
    with open(path, "w") as f:
        json.dump(data, f, indent=2, default=str)


# ---- Main fetch function ---------------------------------------------------
_API_URL = "https://api.usaspending.gov/api/v2/search/spending_by_award/"
_API_FIELDS = ["Award ID", "Award Amount", "Type of Contract Pricing",
               "NAICS Code", "NAICS Description",
               "PSC Code", "Product or Service Code (PSC) Description"]
MIN_SAMPLE = 10  # broaden if fewer results than this

def _query_usaspending(naics_codes, psc_code=None, lo=None, hi=None):
    """Fire one USASpending query; return (results_list, total_count)."""
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
    """Return (pricing_counts, amounts, naics_desc, psc_desc) from raw results."""
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


def fetch_award_patterns(naics, psc, award_amount=None):
    """Query USASpending for similar contracts in the same size tier.

    Progressive broadening:
      1. NAICS 4-digit + PSC + size tier  (most specific)
      2. NAICS 4-digit + PSC, all sizes   (drop tier)
      3. NAICS 2-digit sector, all sizes   (broadest)
    """
    if award_amount:
        tier_name, tier_lo, tier_hi = _award_tier(award_amount)
    else:
        tier_name, tier_lo, tier_hi = "all", None, None

    path = _cache_path(naics, psc, tier_name)
    cached = _read_cache(path)
    if cached:
        print(f"  [cache hit] tier={tier_name}, expires {cached['expires_at'][:10]}")
        return cached

    naics_4, psc_1 = str(naics)[:4], str(psc)[:1].upper()
    results = None
    query_desc = ""

    try:
        # Pass 1: NAICS 4-digit + PSC + size tier
        if tier_lo is not None or tier_hi is not None:
            tier_hi_str = f"${tier_hi:,.0f}" if tier_hi else "∞"
            print(f"  Pass 1: NAICS {naics_4} + PSC {psc_1} + tier {tier_name} (${tier_lo:,.0f}–{tier_hi_str}) …")
            results, total = _query_usaspending([naics_4], psc_1, tier_lo, tier_hi)
            query_desc = f"NAICS {naics_4} + PSC {psc_1} + tier {tier_name}"

        # Pass 2: drop size tier (all amounts)
        if not results or len(results) < MIN_SAMPLE:
            if results:
                print(f"  Only {len(results)} results in tier; broadening to all sizes …")
            print(f"  Pass 2: NAICS {naics_4} + PSC {psc_1}, all sizes …")
            results, total = _query_usaspending([naics_4], psc_1)
            query_desc = f"NAICS {naics_4} + PSC {psc_1} (all sizes)"

        # Pass 3: broaden to 2-digit NAICS sector
        if not results or len(results) < MIN_SAMPLE:
            naics_2 = str(naics)[:2]
            print(f"  Pass 3: broadening to NAICS sector {naics_2}, all sizes …")
            results, total = _query_usaspending([naics_2])
            query_desc = f"NAICS sector {naics_2} (all sizes)"
    except Exception as e:
        print(f"  API error ({e}); will use fallback rules")
        return None

    if not results:
        print("  No awards found across all passes; will use fallback rules")
        return None

    # ---- Aggregate ----
    pricing_counts, amounts, naics_desc, psc_desc = _aggregate(results)
    n = sum(pricing_counts.values())
    dominant = max(pricing_counts, key=pricing_counts.get)
    sa = sorted(amounts) if amounts else []

    pattern = {
        "naics_queried": naics_4, "psc_queried": psc_1,
        "tier": tier_name, "query": query_desc,
        "sample_size": len(results), "total_matched": total,
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

    _write_cache(path, pattern)
    dist_str = ", ".join(f"{k}: {v['pct']}%" for k, v in pattern["pricing_distribution"].items())
    print(f"  {len(results)} awards via: {query_desc}")
    print(f"  Dominant pricing: {dominant}")
    print(f"  Distribution: {dist_str}")
    if sa:
        print(f"  Award range: ${sa[0]:,.0f} – ${sa[-1]:,.0f} (median ${sa[len(sa)//2]:,.0f})")
    return pattern


# ---- Run for current scenario ----
amt = scenario["award_amount"]
print(f"Pricing patterns for NAICS {scenario['naics']} / PSC {scenario['psc']} (award ${float(amt):,.0f}):")
award_patterns = fetch_award_patterns(scenario["naics"], scenario["psc"], amt)

if award_patterns:
    print(f"\n→ Recommended pricing: {award_patterns['pricing_type']} "
          f"(from {award_patterns['sample_size']} similar awards, tier={award_patterns.get('tier','all')})")
    print(f"  Matched via: {award_patterns.get('query', 'n/a')}")
    stats = award_patterns.get("award_stats")
    if stats and stats.get("median"):
        print(f"  Sample award range: ${stats['min']:,.0f} – ${stats['max']:,.0f} "
              f"(median ${stats['median']:,.0f})")
else:
    print("\n→ No USASpending patterns available; CLIN generation will use hardcoded defaults")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ### NAICS Labor Templates & PSC Rules
# MAGIC USASpending (above) provides the **pricing type** — the most impactful guardrail.  
# MAGIC The templates below provide the **CLIN structure** (labor categories, weights, unit prices) that the API does not carry.

# COMMAND ----------

# DBTITLE 1,NAICS Labor Templates & PSC Rules
# ===========================================================================
# Domain-knowledge templates: NAICS -> labor categories, PSC -> extra lines.
# USASpending determines the pricing type; these determine the CLIN structure.
# ===========================================================================
NAICS_SECTORS = {
    "11": "Agriculture, Forestry, Fishing and Hunting",
    "21": "Mining, Quarrying, and Oil and Gas Extraction",
    "22": "Utilities", "23": "Construction",
    "31": "Manufacturing", "32": "Manufacturing", "33": "Manufacturing",
    "42": "Wholesale Trade",
    "44": "Retail Trade", "45": "Retail Trade",
    "48": "Transportation and Warehousing", "49": "Transportation and Warehousing",
    "51": "Information", "52": "Finance and Insurance",
    "53": "Real Estate and Rental and Leasing",
    "54": "Professional, Scientific, and Technical Services",
    "55": "Management of Companies and Enterprises",
    "56": "Administrative and Support and Waste Management and Remediation Services",
    "61": "Educational Services", "62": "Health Care and Social Assistance",
    "71": "Arts, Entertainment, and Recreation",
    "72": "Accommodation and Food Services",
    "81": "Other Services (except Public Administration)",
    "92": "Public Administration",
}

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

def naics_rule(naics):
    """Most specific matching prefix (6 -> 2 digits); generic services if none."""
    code = str(naics or "").strip()
    for n in range(len(code), 1, -1):
        if code[:n] in NAICS_RULES:
            return code[:n], NAICS_RULES[code[:n]]
    return "default", _GENERIC_SERVICES

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

def psc_rule(psc):
    code = str(psc or "").strip().upper()
    first = code[:1]
    if first.isdigit():
        return "product", "Product (FSC)", "FFP", [(f"Product per PSC {code}", "item", 0.30, 1250.00)]
    if first in PSC_RULES:
        name, pricing, extra = PSC_RULES[first]
        return first, name, pricing, extra
    return "other_service", "Other Services", "FFP", []

UOM = {"labor": "HR", "item": "EA", "lot": "LO"}
ODC_LINE = ("Other Direct Costs / Travel", "lot", 0.03, None)

def build_line_template(naics, psc, patterns=None):
    """Combines NAICS lines + PSC extra lines (+ ODC for services); weights normalized to 1.
    Pricing type comes from USASpending patterns when available (Layer 2 guardrails)."""
    n_key, n_lines = naics_rule(naics)
    p_key, p_name, fallback_pricing, p_lines = psc_rule(psc)

    # Pricing: prefer real-world USASpending data over hardcoded PSC rules
    if patterns and patterns.get("pricing_type"):
        pricing = patterns["pricing_type"]
        pricing_src = "usaspending"
        if patterns.get("psc_description"):
            p_name = patterns["psc_description"]
    else:
        pricing = fallback_pricing
        pricing_src = "fallback"

    lines = list(n_lines)
    if p_key == "product" and any(k == "item" for _, k, _, _ in n_lines):
        p_lines = []
    lines += p_lines
    if p_key != "product":
        lines.append(ODC_LINE)
    total_w = sum(w for _, _, w, _ in lines)
    lines = [(lbl, kind, w / total_w, price) for lbl, kind, w, price in lines]
    method = f"rules_v2|naics_rule={n_key}|psc_group={p_key}|pricing={pricing}|src={pricing_src}"
    return lines, method, pricing, p_name

# ---- Build template for current scenario ----
template, generation_method, pricing, psc_group_name = build_line_template(scenario["naics"], scenario["psc"], award_patterns)
sector = NAICS_SECTORS.get(str(scenario["naics"])[:2], "Unknown sector")
print(f"NAICS {scenario['naics']} -> sector {str(scenario['naics'])[:2]} ({sector})")
pricing_src = "USASpending" if (award_patterns and award_patterns.get("pricing_type")) else "fallback rules"
print(f"PSC {scenario['psc']} -> {psc_group_name}, pricing {pricing} (source: {pricing_src})")
print(generation_method)
for lbl, kind, w, price in template:
    print(f"  {lbl:<38} {UOM[kind]}  weight={w:.3f}  base_price={price}")

# COMMAND ----------

# DBTITLE 1,Generate Synthetic CLINs
# One CLIN per template line per segment. CLIN number = series + two-letter suffix
# (e.g. 1000AA, 1000AB, 1600AA) following the FAR sub-CLIN convention.
# Dollars: award_amount split across segments by days, then across lines by weight.
# Rounding remainder goes to the largest CLIN so the total equals award_amount exactly.
from decimal import Decimal, ROUND_HALF_UP
from pyspark.sql.types import StructType, StructField, StringType, DecimalType, DateType, TimestampType

def _clin_suffix(n):
    """Convert 1-based line number to two-letter suffix: 1->AA, 2->AB, …, 26->AZ, 27->BA."""
    n -= 1  # zero-index
    return chr(ord('A') + n // 26) + chr(ord('A') + n % 26)

CENT = Decimal("0.01")
award_amount = Decimal(scenario["award_amount"])
total_days = sum((e - s).days + 1 for _, s, e in segments)
psc_desc = (scenario["psc_description"] or "").title()

clin_rows = []
for seg_idx, (series, seg_start, seg_end) in enumerate(segments):
    seg_share = Decimal((seg_end - seg_start).days + 1) / Decimal(total_days)
    escalation = Decimal(str((1 + escalation_rate) ** seg_idx))
    period = "Base Period" if seg_idx == 0 else f"Option Period {seg_idx}"
    for line_no, (label, kind, weight, base_price) in enumerate(template, start=1):
        amount = (award_amount * seg_share * Decimal(str(weight))).quantize(CENT, ROUND_HALF_UP)
        clin_rows.append({
            "scenario_id": scenario["scenario_id"],
            "piid": scenario["piid"],
            "option_year_series": series,
            "clin_number": f"{series}{_clin_suffix(line_no)}",
            "department": scenario["awarding_agency"],
            "naics": scenario["naics"],
            "psc": scenario["psc"],
            "clin_description": f"{period} - {label} ({pricing}) - {psc_desc}",
            "unit_of_measure": UOM[kind],
            "unit_price": None if base_price is None else (Decimal(str(base_price)) * escalation).quantize(CENT, ROUND_HALF_UP),
            "obligated_amount": amount,
            "pop_start_date": seg_start,
            "pop_end_date": seg_end,
            "generation_method": generation_method,
            "expires_at": scenario["expires_at"],
        })

# Lots are priced as quantity 1
for r in clin_rows:
    if r["unit_price"] is None:
        r["unit_price"] = r["obligated_amount"]

# Push rounding remainder onto the largest CLIN
remainder = award_amount - sum(r["obligated_amount"] for r in clin_rows)
if remainder:
    biggest = max(clin_rows, key=lambda r: r["obligated_amount"])
    biggest["obligated_amount"] += remainder
    if biggest["unit_of_measure"] == "LO":
        biggest["unit_price"] = biggest["obligated_amount"]

CLIN_SCHEMA = StructType([
    StructField("scenario_id", StringType()), StructField("piid", StringType()),
    StructField("option_year_series", StringType()), StructField("clin_number", StringType()),
    StructField("department", StringType()), StructField("naics", StringType()),
    StructField("psc", StringType()), StructField("clin_description", StringType()),
    StructField("unit_of_measure", StringType()), StructField("unit_price", DecimalType(18, 2)),
    StructField("obligated_amount", DecimalType(18, 2)), StructField("pop_start_date", DateType()),
    StructField("pop_end_date", DateType()), StructField("generation_method", StringType()),
    StructField("expires_at", TimestampType()),
])

clins_df = (
    spark.createDataFrame(clin_rows, CLIN_SCHEMA)
    .withColumn("created_at", F.current_timestamp())
    .select(*[f.name for f in spark.table(CLINS).schema])  # match target column order
)
print(f"Generated {len(clin_rows)} CLINs across {len(segments)} segments; remainder adjusted: {remainder}")
display(clins_df.orderBy("clin_number"))

# COMMAND ----------

# DBTITLE 1,Write CLINs to main_simulation_synthetic_clins
# Replaces only this scenario's CLINs in one atomic Delta write (re-runs are safe;
# other scenarios are untouched).
sid = scenario["scenario_id"].replace("'", "''")
(
    clins_df.write.format("delta")
    .mode("overwrite")
    .option("replaceWhere", f"scenario_id = '{sid}'")
    .saveAsTable(CLINS)
)
print(f"Wrote {clins_df.count()} CLINs for scenario {scenario['scenario_id']} to {CLINS}")

# COMMAND ----------

# DBTITLE 1,Validate CLINs Against Scenario
# Checks read back from the table: totals tie to award_amount, POP coverage, unique CLIN numbers.
written = spark.table(CLINS).filter(F.col("scenario_id") == scenario["scenario_id"])

check = written.agg(
    F.count("*").alias("clin_count"),
    F.countDistinct("clin_number").alias("distinct_clins"),
    F.sum("obligated_amount").alias("total_obligated"),
    F.min("pop_start_date").alias("min_start"),
    F.max("pop_end_date").alias("max_end"),
).first()

results = {
    "total equals award_amount": check["total_obligated"] == award_amount,
    "CLIN numbers unique": check["clin_count"] == check["distinct_clins"],
    "starts at POP start": check["min_start"] == pop_start,
    "ends at POP end": check["max_end"] == pop_end,
    "no negative amounts": written.filter(F.col("obligated_amount") < 0).count() == 0,
}
for name, ok in results.items():
    print(f"{'PASS' if ok else 'FAIL'}  {name}")
print(f"Total obligated {check['total_obligated']:,} vs award_amount {award_amount:,}")

display(
    written.groupBy("option_year_series")
    .agg(F.min("pop_start_date").alias("start"), F.max("pop_end_date").alias("end"),
         F.count("*").alias("clins"), F.sum("obligated_amount").alias("obligated"))
    .orderBy("start")
)
assert all(results.values()), "CLIN validation failed"