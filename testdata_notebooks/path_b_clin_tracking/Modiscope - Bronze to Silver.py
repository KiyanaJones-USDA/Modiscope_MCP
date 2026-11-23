# Databricks notebook source
# MAGIC %sql
# MAGIC -- Use users catalog and schema with my name
# MAGIC USE CATALOG users;
# MAGIC USE SCHEMA kiyana_jones;
# MAGIC
# MAGIC

# COMMAND ----------

#Define widgets & execution mode

#Create widgets passed via job
dbutils.widgets.dropdown("execution_mode","INIT_EMPTY", ["INIT_EMPTY", "INGEST_EXCEL"], "Choose execution mode")
dbutils.widgets.text("file_path", "", "Uploaded File Path")
dbutils.widgets.text("contract_id", "", "Contract ID")

#Retrieve the passed values
execution_mode = dbutils.widgets.get("execution_mode").strip()
file_path = dbutils.widgets.get("file_path").strip()
contract_id = dbutils.widgets.get("contract_id").strip()

print(f"Triggered Bronze-to-silver run in mode: '{execution_mode}'")


# COMMAND ----------

# DBTITLE 1,Setup & Helper Function
import pandas as pd
import re

# contract_id is mandatory — it identifies each contract's rows across all silver tables
assert contract_id, "contract_id widget must always be set"
print(f"Contract ID: {contract_id}")

# In INGEST_EXCEL mode, also validate the file path
if execution_mode == "INGEST_EXCEL":
    assert file_path, "file_path widget must be set for INGEST_EXCEL mode"
    print(f"File path: {file_path}")

def clean_sheet(sheet_name, header=0):
    """Read and apply standard cleaning to a CLIN tracking log sheet.
    
    - Reads the sheet from the baseline Excel file
    - Adds contract_id column (row-level contract identifier)
    - Cleans column names (lowercase, underscores)
    - Strips whitespace from string columns
    """
    df = pd.read_excel(file_path, sheet_name=sheet_name, header=header)
    df["contract_id"] = contract_id
    df.columns = [
        re.sub(r'_+', '_', re.sub(r'[^a-z0-9_]', '_', str(col).lower())).strip('_')
        for col in df.columns
    ]
    for col in df.select_dtypes(include='object').columns:
        df[col] = df[col].str.strip()
    return df

print("Helper function clean_sheet() defined.")

# COMMAND ----------

# DBTITLE 1,Silver Table Schemas & Helpers
from pyspark.sql.types import StructType, StructField, StringType, LongType, DoubleType, TimestampType

# ── Explicit schemas matching each silver table ──────────────────────────
SILVER_SCHEMAS = {
    "silver_ipp_pop": StructType([
        StructField("mod", StringType(), True),
        StructField("billing_code", StringType(), True),
        StructField("tracking_id", StringType(), True),
        StructField("dept", StringType(), True),
        StructField("cor_tpoc", StringType(), True),
        StructField("ipp_approver", StringType(), True),
        StructField("description", StringType(), True),
        StructField("pop", StringType(), True),
        StructField("notes", StringType(), True),
        StructField("contract_id", StringType(), True),
        StructField("pop_start", TimestampType(), True),
        StructField("pop_end", TimestampType(), True),
    ]),
    "silver_contract_action_log": StructType([
        StructField("action_mod", StringType(), True),
        StructField("date", TimestampType(), True),
        StructField("clin", LongType(), True),
        StructField("description", StringType(), True),
        StructField("department_mission_area", StringType(), True),
        StructField("obligation", DoubleType(), True),
        StructField("total_obligation", DoubleType(), True),
        StructField("clin_balance", DoubleType(), True),
        StructField("notes", StringType(), True),
        StructField("pop", StringType(), True),
        StructField("contract_id", StringType(), True),
    ]),
    "silver_caf_tracker": StructType([
        StructField("mod_action", StringType(), True),
        StructField("date", TimestampType(), True),
        StructField("clin", LongType(), True),
        StructField("description", StringType(), True),
        StructField("department_mission_area", StringType(), True),
        StructField("obligation", DoubleType(), True),
        StructField("total_obligation", DoubleType(), True),
        StructField("notes", StringType(), True),
        StructField("contract_id", StringType(), True),
    ]),
    "silver_invoice_log": StructType([
        StructField("invoice_date", TimestampType(), True),
        StructField("invoice", StringType(), True),
        StructField("clin", LongType(), True),
        StructField("misc", StringType(), True),
        StructField("invoice_amount", DoubleType(), True),
        StructField("ipp_approved", StringType(), True),
        StructField("notes", StringType(), True),
        StructField("contract_id", StringType(), True),
    ]),
}

def create_empty_silver_tables(overwrite=False):
    """Create empty silver tables with correct schemas for a new contract.

    Use when no baseline CLIN tracking log Excel exists.
    Tables are created as empty Delta tables that the Databricks app
    can later populate with invoices and modifications.

    Args:
        overwrite: If False (default), skips tables that already exist.
                   If True, overwrites existing tables with empty ones.
    """
    mode = "overwrite" if overwrite else "ignore"
    for table_name, schema in SILVER_SCHEMAS.items():
        empty_df = spark.createDataFrame([], schema)
        empty_df.write.format("delta").mode(mode).saveAsTable(table_name)
        action = "Overwritten" if overwrite else "Created"
        print(f"{action}: {table_name} ({len(schema.fields)} columns)")

def write_silver_table(pandas_df, table_name):
    """Write a pandas DataFrame to a silver Delta table, replacing rows for the current contract_id.

    - If the table exists: deletes existing rows for this contract_id, then appends
    - If the table doesn't exist: creates the table
    Includes one-time migration from contract_info → contract_id column.
    """
    spark_df = spark.createDataFrame(pandas_df)
    if spark.catalog.tableExists(table_name):
        # One-time migration: rename contract_info → contract_id if needed
        existing_cols = [c.name for c in spark.table(table_name).schema]
        if "contract_info" in existing_cols and "contract_id" not in existing_cols:
            spark.sql(f"ALTER TABLE {table_name} SET TBLPROPERTIES ('delta.columnMapping.mode' = 'name')")
            spark.sql(f"ALTER TABLE {table_name} RENAME COLUMN contract_info TO contract_id")
            print(f"  Migrated {table_name}: renamed contract_info → contract_id")
        spark.sql(f"DELETE FROM {table_name} WHERE contract_id = '{contract_id}'")
        spark_df.write.format("delta").mode("append").option("mergeSchema", "true").saveAsTable(table_name)
    else:
        spark_df.write.format("delta").saveAsTable(table_name)
    print(f"✓ {table_name}: {len(pandas_df)} rows written for contract '{contract_id}'")

print(f"Defined schemas for {len(SILVER_SCHEMAS)} silver tables.")
print("Helpers: create_empty_silver_tables(), write_silver_table()")

# COMMAND ----------

# DBTITLE 1,Branch on Execution Mode
# ── Branch on execution mode ────────────────────────────────────────────
if execution_mode == "INIT_EMPTY":
    create_empty_silver_tables(overwrite=False)
    print(f"\nEmpty silver tables initialized for contract: {contract_id}")
    dbutils.notebook.exit(f"INIT_EMPTY complete for {contract_id}")

# ── INGEST_EXCEL continues to the read/write cells below ────────────────
print(f"Proceeding with Excel ingestion from: {file_path}")

# COMMAND ----------

# DBTITLE 1,Read & Clean IPP POP Info Sheet
# Read & clean _IPP_POP_info sheet (header on row 1)
ipp_pop_df = clean_sheet('_IPP_POP_info', header=1)

# Parse Period of Performance (pop) into separate start/end date columns
ipp_pop_df[['pop_start', 'pop_end']] = ipp_pop_df['pop'].str.split(' to ', expand=True)
ipp_pop_df['pop_start'] = pd.to_datetime(ipp_pop_df['pop_start'], errors='coerce')
ipp_pop_df['pop_end'] = pd.to_datetime(ipp_pop_df['pop_end'], errors='coerce')

print(f"Shape: {ipp_pop_df.shape}")
print(f"Columns: {list(ipp_pop_df.columns)}")
print(f"Nulls per column:\n{ipp_pop_df.isnull().sum()}")
display(ipp_pop_df)

# COMMAND ----------

# DBTITLE 1,Write silver_ipp_pop
write_silver_table(ipp_pop_df, "silver_ipp_pop")

# COMMAND ----------

# DBTITLE 1,Read & Clean Contract Action Log Sheet
# Read & clean Contract Action Log sheet
contract_action_log_df = clean_sheet('Contract Action Log')

# Parse date to datetime
contract_action_log_df['date'] = pd.to_datetime(contract_action_log_df['date'], errors='coerce')

# Add pop column if not present in the uploaded file
if 'pop' not in contract_action_log_df.columns:
    contract_action_log_df['pop'] = None
    print("Added missing 'pop' column (filled with nulls)")

# Cast financial columns to float
contract_action_log_df['obligation'] = contract_action_log_df['obligation'].astype(float)
contract_action_log_df['total_obligation'] = contract_action_log_df['total_obligation'].astype(float)
contract_action_log_df['clin_balance'] = contract_action_log_df['clin_balance'].astype(float)

# If description contains "de-ob" or "de-obligation", make obligation negative
deob_mask = contract_action_log_df['description'].str.contains(r'de-ob|de-obligation', case=False, na=False)
contract_action_log_df.loc[deob_mask, 'obligation'] = -contract_action_log_df.loc[deob_mask, 'obligation'].abs()

# Round financial columns to 2 decimal places
for col in ['obligation', 'total_obligation', 'clin_balance']:
    contract_action_log_df[col] = contract_action_log_df[col].round(2)

print(f"Shape: {contract_action_log_df.shape}")
print(f"Columns: {list(contract_action_log_df.columns)}")
print(f"De-obligation rows flagged: {deob_mask.sum()}")
print(f"Nulls per column:\n{contract_action_log_df.isnull().sum()}")

# Display with $ formatting (display copy only — silver table stays numeric)
display_df = contract_action_log_df.copy()
for col in ['obligation', 'total_obligation', 'clin_balance']:
    display_df[col] = display_df[col].apply(lambda x: f"${x:,.2f}" if pd.notna(x) else None)
display(display_df)



# COMMAND ----------

# DBTITLE 1,Write silver_contract_action_log
write_silver_table(contract_action_log_df, "silver_contract_action_log")

# COMMAND ----------

# DBTITLE 1,Read & Clean CAF Tracker
# Read & clean CAF Tracker sheet
caf_tracker_df = clean_sheet('CAF Tracker')

# Parse date to datetime
caf_tracker_df['date'] = pd.to_datetime(caf_tracker_df['date'], errors='coerce')

# Cast financial columns to float
caf_tracker_df['obligation'] = caf_tracker_df['obligation'].astype(float)
caf_tracker_df['total_obligation'] = caf_tracker_df['total_obligation'].astype(float)

print(f"Shape: {caf_tracker_df.shape}")
print(f"Columns: {list(caf_tracker_df.columns)}")
print(f"Nulls per column:\n{caf_tracker_df.isnull().sum()}")
display(caf_tracker_df)

# COMMAND ----------

# DBTITLE 1,Write silver_caf_tracker
write_silver_table(caf_tracker_df, "silver_caf_tracker")

# COMMAND ----------

# DBTITLE 1,Read & Clean Invoice Log
# Read & clean Invoice Log sheet
invoice_log_df = clean_sheet('Invoice Log')

# Parse invoice_date to datetime
invoice_log_df['invoice_date'] = pd.to_datetime(invoice_log_df['invoice_date'])

# Cast financial column to float
invoice_log_df['invoice_amount'] = invoice_log_df['invoice_amount'].astype(float)

print(f"Shape: {invoice_log_df.shape}")
print(f"Columns: {list(invoice_log_df.columns)}")
print(f"Nulls per column:\n{invoice_log_df.isnull().sum()}")
display(invoice_log_df)

# COMMAND ----------

# DBTITLE 1,Write silver_invoice_log
write_silver_table(invoice_log_df, "silver_invoice_log")

# COMMAND ----------

# DBTITLE 1,Verify Multi-Contract Structure
# MAGIC %sql
# MAGIC SELECT 'silver_ipp_pop' AS table_name, contract_id, COUNT(*) AS rows
# MAGIC FROM silver_ipp_pop GROUP BY ALL
# MAGIC UNION ALL
# MAGIC SELECT 'silver_contract_action_log', contract_id, COUNT(*)
# MAGIC FROM silver_contract_action_log GROUP BY ALL
# MAGIC UNION ALL
# MAGIC SELECT 'silver_caf_tracker', contract_id, COUNT(*)
# MAGIC FROM silver_caf_tracker GROUP BY ALL
# MAGIC UNION ALL
# MAGIC SELECT 'silver_invoice_log', contract_id, COUNT(*)
# MAGIC FROM silver_invoice_log GROUP BY ALL
# MAGIC ORDER BY table_name
# MAGIC

# COMMAND ----------

# DBTITLE 1,One-Time Cleanup: Remove Old contract_info Rows
# One-time cleanup: remove orphaned rows from the contract_info → contract_id migration.
# These rows have the old descriptive name as their contract_id value.
# Run this ONCE, then delete this cell.

old_value = "Support Services - Contract A 15847897F0112"
for table in ["silver_ipp_pop", "silver_contract_action_log", "silver_caf_tracker", "silver_invoice_log"]:
    before = spark.table(table).count()
    spark.sql(f"DELETE FROM {table} WHERE contract_id = '{old_value}'")
    after = spark.table(table).count()
    print(f"✓ {table}: {before} → {after} rows (removed {before - after})")