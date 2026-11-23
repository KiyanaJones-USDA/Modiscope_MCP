# Databricks notebook source
import requests
import json
import pandas as pd

def test_usaspending_deobligations(
    funding_agency: str = "Department of Agriculture",
    fiscal_year: int = 2025,
    sample_size: int = 100
):
    url = "https://api.usaspending.gov/api/v2/search/spending_by_transaction/"
    start_date: str = f"{fiscal_year}-01-01"
    end_date: str = f"{fiscal_year}-12-31"
    payload = {
        "filters": {
            "award_type_codes": ["A", "B", "C", "D"],
            "agencies": [
                {
                    "type": "funding",
                    "tier": "toptier",
                    "name": funding_agency,
                }
            ],
            "time_period": [
                {
                    "start_date": start_date,
                    "end_date": end_date,
                }
            ],
        },
        "fields": [
            "Award ID",
            "Mod",
            "Action Type",
            "Action Date",
            "Transaction Amount",
            "Recipient Name",
            "Awarding Agency",
            "Funding Agency",
            "Funding Sub Agency",
            "Transaction Description"
        ],
        "limit": min(sample_size, 100),
        "page": 1,
        "sort": "Transaction Amount",
        "order": "asc"
    }

    print(f"Querying USAspending API for: {funding_agency} (FY{fiscal_year})...\n")
    response = requests.post(url, json=payload, timeout=12)
    response.raise_for_status()
    raw_results = response.json().get("results", [])

    action_type_map = {
        "A": "New Award",
        "B": "Continuation",
        "C": "Revision/Amendment",
        "D": "Deleted",
        "E": "Provisional",
        "F": "Termination",
        "G": "Estimated",
        "H": "Interim",
        "J": "Definitive Change",
        "K": "Closeout",
        "L": "Definitization",
        "M": "Admin Modification",
        "P": "Rerepresentation",
        "R": "Representation",
        "S": "Supplemental Agreement",
        "X": "Cancellation/Termination",
    }

    deobligations = []
    total_deobligation = 0.0

    for row in raw_results:
        amount = float(row.get("Transaction Amount") or 0.0)
        if amount < 0:
            abs_val = abs(amount)
            total_deobligation += abs_val
            deobligations.append({
                "Award PIID": row.get("Award ID"),
                "Mod #": row.get("Mod"),
                "Action Type": f"{row.get('Action Type', 'N/A')} - {action_type_map.get(row.get('Action Type', ''), 'Unknown')}",
                "Sub Agency": row.get("Funding Sub Agency"),
                "De-obligated ($)": amount,
                "Vendor": row.get("Recipient Name"),
                "Action Date": row.get("Action Date"),
                "Description": row.get("Transaction Description", "N/A")
            })

    # Compute additional stats
    avg_deobligation = round(-total_deobligation / len(deobligations), 2) if deobligations else 0.0
    largest_deob = min(deobligations, key=lambda d: d["De-obligated ($)"])["De-obligated ($)"] if deobligations else 0.0

    from collections import Counter
    sub_agency_counts = Counter(d["Sub Agency"] for d in deobligations)
    vendor_counts = Counter(d["Vendor"] for d in deobligations)
    most_freq_sub_agency = sub_agency_counts.most_common(1)[0] if sub_agency_counts else ("N/A", 0)
    most_freq_vendor = vendor_counts.most_common(1)[0] if vendor_counts else ("N/A", 0)
    most_freq_action_type = Counter(d["Action Type"] for d in deobligations).most_common(1)[0] if deobligations else ("N/A", 0)

    output_summary = {
        "Agency Queried": funding_agency,
        "Fiscal Year": fiscal_year,
        "Total Negative Actions Found": len(deobligations),
        "Total Sampled De-obligation ($)": round(-total_deobligation, 2),
        "Average De-obligation ($)": avg_deobligation,
        "Largest Single De-obligation ($)": largest_deob,
        "Most Frequent Sub Agency": f"{most_freq_sub_agency[0]} ({most_freq_sub_agency[1]} actions)",
        "Most Frequent Vendor": f"{most_freq_vendor[0]} ({most_freq_vendor[1]} actions)",
        "Most Frequent Action Type": f"{most_freq_action_type[0]} ({most_freq_action_type[1]} actions"
    }

    return output_summary, deobligations

#Test run
summary, deob_rows = test_usaspending_deobligations("Department of Agriculture", 2025, 100)

print("=== DE-OBLIGATION TEST RESULTS ===")
print(json.dumps(summary, indent=2))

if deob_rows:
    df = pd.DataFrame(deob_rows)
    display(df)
else:
    print("No de-obligations found.")
        

# COMMAND ----------

import requests

def get_usda_frequency_baseline(fiscal_year: int = 2025):
    url = "https://api.usaspending.gov/api/v2/search/spending_by_category/funding_agency/"

    payload = {
        "filters": {
            "award_type_codes":["A","B","C","D"],
            "agencies": [{"type": "funding", "tier": "toptier", "name": "Department of Agriculture"}],
            "time_period": [{"start_date": f"{fiscal_year - 1}-10-01", "end_date": f"{fiscal_year}-09-30"}]
        },
        "limit": 1
    }

    resp = requests.post(url, json=payload, timeout=12).json()
    total_net = resp.get("results", [{}])[0].get("amount", 0.0)

    print(f"USDA FY{fiscal_year} Net Contract Obligations: ${total_net:,}")
    return total_net
net_usda_spend = get_usda_frequency_baseline(2025)