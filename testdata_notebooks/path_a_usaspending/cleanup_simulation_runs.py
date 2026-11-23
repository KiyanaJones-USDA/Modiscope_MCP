# Databricks notebook source
# DBTITLE 1,Cleanup simulation runs
# MAGIC %md
# MAGIC # Cleanup simulation runs
# MAGIC
# MAGIC Deletes Modiscope scenario runs older than `retention_days` (default 7): scenario and CLIN JSON files in `/Volumes/users/kiyana_jones/bronze/simulations`, plus matching rows in the `main_simulation_*` tables. Runs daily as a scheduled job; set `dry_run = true` to preview without deleting.

# COMMAND ----------

# DBTITLE 1,Settings
import json, os
from datetime import datetime, timedelta, timezone

dbutils.widgets.text("retention_days", "7", "Retention (days)")
dbutils.widgets.dropdown("dry_run", "false", ["false", "true"], "Dry run (preview only)")

RETENTION_DAYS = int(dbutils.widgets.get("retention_days") or 7)
DRY_RUN = dbutils.widgets.get("dry_run") == "true"
SIM_DIR = "/Volumes/users/kiyana_jones/bronze/simulations"
SCHEMA = "users.kiyana_jones"
CUTOFF = datetime.now(timezone.utc) - timedelta(days=RETENTION_DAYS)
print(f"Deleting runs created before {CUTOFF.isoformat()} ({RETENTION_DAYS} days) | dry_run={DRY_RUN}")

# COMMAND ----------

# DBTITLE 1,Delete old scenario and CLIN files (volume)
def _created(path, key="created_at"):
    """created_at from the JSON file; falls back to the file's modification time."""
    try:
        with open(path) as f:
            ts = json.load(f).get(key)
        if ts:
            dt = datetime.fromisoformat(ts)
            return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
    except Exception:
        pass
    return datetime.fromtimestamp(os.path.getmtime(path), timezone.utc)

def _json_files(folder):
    return [n for n in os.listdir(folder) if n.endswith(".json")] if os.path.isdir(folder) else []

# 1) Scenarios: delete when created_at is older than the cutoff
scen_dir = f"{SIM_DIR}/scenarios"
kept_ids, old_ids = set(), set()
for name in _json_files(scen_dir):
    sid = name[:-5]
    (old_ids if _created(os.path.join(scen_dir, name)) < CUTOFF else kept_ids).add(sid)

# 2) CLINs: delete when the scenario is old/gone (orphan) or the file itself is old
clin_dir = f"{SIM_DIR}/clins"
old_clins = [n for n in _json_files(clin_dir)
             if n[:-5] not in kept_ids
             or datetime.fromtimestamp(os.path.getmtime(os.path.join(clin_dir, n)), timezone.utc) < CUTOFF]

if not DRY_RUN:
    for sid in old_ids:
        os.remove(f"{scen_dir}/{sid}.json")
    for n in old_clins:
        os.remove(f"{clin_dir}/{n}")

print(f"scenario files: {len(old_ids)} {'to delete' if DRY_RUN else 'deleted'}, {len(kept_ids)} kept")
print(f"CLIN files:     {len(old_clins)} {'to delete' if DRY_RUN else 'deleted'}, "
      f"{len(_json_files(clin_dir)) - (len(old_clins) if DRY_RUN else 0)} kept")

# COMMAND ----------

# DBTITLE 1,Delete old rows from simulation tables
# Child tables first (by age or by belonging to an old scenario), then scenarios.
# Partial progress is safe: anything missed is picked up on the next daily run.
old_scen = f"SELECT scenario_id FROM {SCHEMA}.main_simulation_scenarios WHERE created_at < :cutoff"
statements = [
    ("main_simulation_results", f"created_at < :cutoff OR scenario_id IN ({old_scen})"),
    ("main_simulation_synthetic_clins", f"created_at < :cutoff OR scenario_id IN ({old_scen})"),
    ("main_simulation_scenarios", "created_at < :cutoff"),
]
args = {"cutoff": CUTOFF.replace(tzinfo=None)}  # table timestamps are UTC
for table, where in statements:
    n = spark.sql(f"SELECT count(*) AS n FROM {SCHEMA}.{table} WHERE {where}", args=args).first()["n"]
    if not DRY_RUN and n:
        spark.sql(f"DELETE FROM {SCHEMA}.{table} WHERE {where}", args=args)
    remaining = spark.table(f"{SCHEMA}.{table}").count()
    print(f"{table}: {n} {'to delete' if DRY_RUN else 'deleted'}, {remaining} remaining")