# Databricks notebook source
# DBTITLE 1,Scenarios Table
# MAGIC %sql
# MAGIC
# MAGIC USE CATALOG users;
# MAGIC USE SCHEMA kiyana_jones;
# MAGIC
# MAGIC CREATE TABLE IF Not EXISTS main_simulation_scenarios
# MAGIC (
# MAGIC     scenario_id STRING,
# MAGIC     scenario_type STRING, --usaspending for path a
# MAGIC     piid STRING,
# MAGIC     award_id STRING, -- USAspending generated_internal_id (e.g. CONT_AWD_...)
# MAGIC     recipient_name STRING,
# MAGIC     awarding_agency STRING,
# MAGIC     awarding_subagency STRING,
# MAGIC     contract_award_type STRING,
# MAGIC     naics STRING, -- NAICS code
# MAGIC     naics_description STRING,
# MAGIC     psc STRING, -- PSC code
# MAGIC     psc_description STRING,
# MAGIC     award_amount DECIMAL(18,2), -- ceiling; CLIN allocations must sum to this
# MAGIC     total_outlays DECIMAL(18,2), -- actual burn
# MAGIC     pop_start_date DATE,
# MAGIC     pop_current_end_date DATE,
# MAGIC     pop_potential_end_date DATE,
# MAGIC     created_at TIMESTAMP,
# MAGIC     expires_at TIMESTAMP,
# MAGIC     parameters STRING
# MAGIC )
# MAGIC
# MAGIC USING DELTA;
# MAGIC

# COMMAND ----------

# DBTITLE 1,USAspending Award Cache
# MAGIC %sql
# MAGIC USE CATALOG users;
# MAGIC USE SCHEMA kiyana_jones;
# MAGIC
# MAGIC CREATE TABLE IF Not EXISTS main_simulation_usaspending_award_cache
# MAGIC (
# MAGIC     piid STRING, -- uppercased PIID; one row per PIID (upsert via MERGE)
# MAGIC     award_data STRING, -- raw json payload from API
# MAGIC     fetched_at TIMESTAMP,
# MAGIC     expires_at TIMESTAMP -- TTL: fetched_at + 5 days, set by fetch_usaspending_piid tool (USASPENDING_CACHE_TTL_DAYS)
# MAGIC     
# MAGIC )
# MAGIC
# MAGIC USING DELTA;

# COMMAND ----------

# DBTITLE 1,Synthetic Clin Table
# MAGIC %sql
# MAGIC USE CATALOG users;
# MAGIC USE SCHEMA kiyana_jones;
# MAGIC
# MAGIC CREATE TABLE IF Not EXISTS main_simulation_synthetic_clins
# MAGIC (
# MAGIC     scenario_id STRING,
# MAGIC     piid STRING,
# MAGIC     option_year_series STRING,
# MAGIC     clin_number STRING, -- synthetic CLIN number
# MAGIC     department STRING,
# MAGIC     naics STRING,
# MAGIC     psc STRING,
# MAGIC     clin_description STRING,
# MAGIC     unit_of_measure STRING,
# MAGIC     unit_price DECIMAL(18,2),
# MAGIC     obligated_amount DECIMAL(18,2),
# MAGIC     pop_start_date DATE,
# MAGIC     pop_end_date DATE,
# MAGIC     generation_method STRING,
# MAGIC     created_at TIMESTAMP,
# MAGIC     expires_at TIMESTAMP --match on scenarion expiration date
# MAGIC )
# MAGIC USING DELTA;

# COMMAND ----------

# DBTITLE 1,Simulation Results Table
# MAGIC %sql
# MAGIC USE CATALOG users;
# MAGIC USE SCHEMA kiyana_jones;
# MAGIC
# MAGIC CREATE TABLE IF Not EXISTS main_simulation_results
# MAGIC (
# MAGIC  scenario_id STRING,
# MAGIC  piid STRING,
# MAGIC  clin_number STRING,
# MAGIC  metric_name STRING, -- 'daily burn'
# MAGIC  metric_value DECIMAL(18,2),
# MAGIC  metric_unit STRING,
# MAGIC  created_at TIMESTAMP,
# MAGIC  expires_at TIMESTAMP
# MAGIC )
# MAGIC
# MAGIC USING DELTA;

# COMMAND ----------

# DBTITLE 1,Load Simulation Scenarios from Volume
# Loads scenario JSON files written by fetch_usaspending_piid(create_simulation=True)
# from /Volumes/users/kiyana_jones/bronze/simulations/scenarios/<scenario_id>.json
# into users.kiyana_jones.main_simulation_scenarios (MERGE on scenario_id, so re-runs are safe).
import json
import os
from datetime import datetime, timezone

SCENARIO_DIR = "/Volumes/users/kiyana_jones/bronze/simulations/scenarios"
TARGET = "users.kiyana_jones.main_simulation_scenarios"
COLUMNS = [
    "scenario_id", "scenario_type", "piid", "award_id", "recipient_name",
    "awarding_agency", "awarding_subagency", "contract_award_type",
    "naics", "naics_description", "psc", "psc_description",
    "award_amount", "total_outlays", "pop_start_date", "pop_current_end_date", "pop_potential_end_date",
    "created_at", "expires_at", "parameters",
]
now = datetime.now(timezone.utc)

rows = []
if os.path.isdir(SCENARIO_DIR):
    for name in os.listdir(SCENARIO_DIR):
        if not name.endswith(".json"):
            continue
        with open(os.path.join(SCENARIO_DIR, name)) as f:
            s = json.load(f)
        if datetime.fromisoformat(s["expires_at"]) <= now:
            continue  # expired: removed by the cleanup cells below
        rows.append(tuple(None if s.get(c) is None else str(s.get(c)) for c in COLUMNS))

if rows:
    spark.createDataFrame(rows, ", ".join(f"{c} STRING" for c in COLUMNS)).createOrReplaceTempView("new_scenarios")
    spark.sql(f"""
        MERGE INTO {TARGET} AS t
        USING (
            SELECT scenario_id, scenario_type, piid, award_id, recipient_name,
                   awarding_agency, awarding_subagency, contract_award_type,
                   naics, naics_description, psc, psc_description,
                   CAST(award_amount AS DECIMAL(18,2))   AS award_amount,
                   CAST(total_outlays AS DECIMAL(18,2))  AS total_outlays,
                   CAST(pop_start_date AS DATE)          AS pop_start_date,
                   CAST(pop_current_end_date AS DATE)    AS pop_current_end_date,
                   CAST(pop_potential_end_date AS DATE)  AS pop_potential_end_date,
                   CAST(created_at AS TIMESTAMP)         AS created_at,
                   CAST(expires_at AS TIMESTAMP)         AS expires_at,
                   parameters
            FROM new_scenarios
        ) AS s
        ON t.scenario_id = s.scenario_id
        WHEN NOT MATCHED THEN INSERT *
    """)

print(f"Scenario files loaded: {len(rows)}")
display(spark.table(TARGET).orderBy("created_at", ascending=False).limit(10))

# COMMAND ----------

# DBTITLE 1,Delete Expired Simulations (5-day TTL)
# MAGIC %sql
# MAGIC -- Removes simulation data older than 5 days so the tables don't keep growing.
# MAGIC -- A row is expired when expires_at has passed; if expires_at is NULL, created_at + 5 days is used instead.
# MAGIC -- Child rows (CLINs, results) are deleted first, including any that belong to an expired scenario_id.
# MAGIC USE CATALOG users;
# MAGIC USE SCHEMA kiyana_jones;
# MAGIC
# MAGIC -- 1) Simulation results
# MAGIC DELETE FROM main_simulation_results
# MAGIC WHERE coalesce(expires_at, timestampadd(DAY, 5, created_at)) < current_timestamp()
# MAGIC    OR scenario_id IN (
# MAGIC         SELECT scenario_id FROM main_simulation_scenarios
# MAGIC         WHERE coalesce(expires_at, timestampadd(DAY, 5, created_at)) < current_timestamp()
# MAGIC    );
# MAGIC
# MAGIC -- 2) Synthetic CLINs
# MAGIC DELETE FROM main_simulation_synthetic_clins
# MAGIC WHERE coalesce(expires_at, timestampadd(DAY, 5, created_at)) < current_timestamp()
# MAGIC    OR scenario_id IN (
# MAGIC         SELECT scenario_id FROM main_simulation_scenarios
# MAGIC         WHERE coalesce(expires_at, timestampadd(DAY, 5, created_at)) < current_timestamp()
# MAGIC    );
# MAGIC
# MAGIC -- 3) Scenarios
# MAGIC DELETE FROM main_simulation_scenarios
# MAGIC WHERE coalesce(expires_at, timestampadd(DAY, 5, created_at)) < current_timestamp();
# MAGIC
# MAGIC -- 4) USAspending award cache
# MAGIC DELETE FROM main_simulation_usaspending_award_cache
# MAGIC WHERE coalesce(expires_at, timestampadd(DAY, 5, fetched_at)) < current_timestamp();
# MAGIC
# MAGIC -- Remaining row counts after cleanup
# MAGIC SELECT 'main_simulation_scenarios' AS table_name, count(*) AS rows_remaining FROM main_simulation_scenarios
# MAGIC UNION ALL SELECT 'main_simulation_synthetic_clins', count(*) FROM main_simulation_synthetic_clins
# MAGIC UNION ALL SELECT 'main_simulation_results', count(*) FROM main_simulation_results
# MAGIC UNION ALL SELECT 'main_simulation_usaspending_award_cache', count(*) FROM main_simulation_usaspending_award_cache;

# COMMAND ----------

# DBTITLE 1,Delete Expired Cache and Scenario Files (Volume)
# Removes USAspending cache files written by the fetch_usaspending_piid tool once expires_at has passed.
# Each file is /Volumes/users/kiyana_jones/bronze/usaspending_cache/<PIID>.json with fetched_at/expires_at (UTC ISO).
import json
import os
from datetime import datetime, timedelta, timezone

# Also removes expired scenario files written by fetch_usaspending_piid(create_simulation=True).
VOLUME_DIRS = {
    "cache": "/Volumes/users/kiyana_jones/bronze/usaspending_cache",
    "scenario": "/Volumes/users/kiyana_jones/bronze/simulations/scenarios",
}
TTL_DAYS = 5
now = datetime.now(timezone.utc)

for label, folder in VOLUME_DIRS.items():
    deleted, kept = 0, 0
    if os.path.isdir(folder):
        for name in os.listdir(folder):
            if not name.endswith(".json"):
                continue
            path = os.path.join(folder, name)
            try:
                with open(path) as f:
                    entry = json.load(f)
                expires_at = datetime.fromisoformat(entry["expires_at"])
            except Exception:
                # Unreadable file: fall back to file modification time + TTL
                expires_at = datetime.fromtimestamp(os.path.getmtime(path), timezone.utc) + timedelta(days=TTL_DAYS)
            if expires_at <= now:
                os.remove(path)
                deleted += 1
            else:
                kept += 1
    print(f"{label} files deleted: {deleted}, kept: {kept}")