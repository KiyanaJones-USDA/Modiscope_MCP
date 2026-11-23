# Databricks notebook source
# DBTITLE 1,CLIN Summary
# MAGIC %sql
# MAGIC -- Use users catalog and schema with my name
# MAGIC USE CATALOG users;
# MAGIC USE SCHEMA kiyana_jones;
# MAGIC
# MAGIC CREATE OR REPLACE VIEW contract_portfolio_summary AS
# MAGIC SELECT
# MAGIC COUNT(DISTINCT clin) AS total_active_clins,
# MAGIC SUM(current_obligated_ceiling) AS total_obligated_ceiling,
# MAGIC SUM(remaining_funds) AS total_remaining_balance,
# MAGIC ROUND((SUM(total_invoiced) / NULLIF(SUM(current_obligated_ceiling),0.0)) * 100.0, 2) AS portfolio_burn_pct,
# MAGIC SUM(daily_burn_rate) AS aggregate_daily_burn,
# MAGIC MIN(effective_pop) AS earliest_pop_deadline
# MAGIC

# COMMAND ----------

# DBTITLE 1,CLIN Status Summary
# MAGIC %sql
# MAGIC USE CATALOG users;
# MAGIC USE SCHEMA kiyana_jones;
# MAGIC
# MAGIC --- View lastest mod for each clin
# MAGIC WITH latest_contract_action AS (
# MAGIC   SELECT
# MAGIC     contract_id,
# MAGIC     clin,
# MAGIC     action_mod,
# MAGIC     date,
# MAGIC     total_obligation,
# MAGIC     clin_balance,
# MAGIC     pop,
# MAGIC     ROW_NUMBER() OVER (
# MAGIC       PARTITION BY contract_id, clin
# MAGIC       ORDER BY date DESC, action_mod DESC
# MAGIC     ) AS rn
# MAGIC   FROM silver_contract_action_log
# MAGIC ),
# MAGIC
# MAGIC invoice_burn AS (
# MAGIC   SELECT
# MAGIC     contract_id,
# MAGIC     clin,
# MAGIC     -- total cumulative billed amount for this clin
# MAGIC     COALESCE(SUM(invoice_amount), 0.0) AS total_invoiced,
# MAGIC     -- 30-day burn rate
# MAGIC     COALESCE(
# MAGIC       SUM(CASE
# MAGIC         WHEN invoice_date >= date_sub(current_date(), 30)
# MAGIC         THEN invoice_amount ELSE 0.0
# MAGIC       END) / 30.0,
# MAGIC       0.0
# MAGIC     ) AS daily_burn_rate_30d
# MAGIC   FROM silver_invoice_log
# MAGIC   WHERE ipp_approved = 'Yes'
# MAGIC   GROUP BY contract_id, clin
# MAGIC )
# MAGIC
# MAGIC SELECT
# MAGIC   a.contract_id,
# MAGIC   a.clin,
# MAGIC   a.action_mod                                AS latest_action,
# MAGIC   a.date                                      AS latest_action_date,
# MAGIC   a.pop,                            
# MAGIC   a.total_obligation                          AS ceiling_amount,
# MAGIC   COALESCE(b.total_invoiced, 0.0)             AS invoiced_actuals,
# MAGIC   a.clin_balance                              AS real_time_balance,
# MAGIC   COALESCE(b.daily_burn_rate_30d, 0.0)        AS daily_burn_velocity,
# MAGIC   CASE
# MAGIC     WHEN a.total_obligation > 0
# MAGIC     THEN ROUND(COALESCE(b.total_invoiced, 0.0) / a.total_obligation * 100, 2)
# MAGIC     ELSE 0.0
# MAGIC   END                                         AS pct_ceiling_consumed
# MAGIC FROM latest_contract_action a
# MAGIC LEFT JOIN invoice_burn b
# MAGIC   ON a.contract_id = b.contract_id
# MAGIC   AND a.clin = b.clin
# MAGIC WHERE a.rn = 1
# MAGIC ORDER BY a.contract_id, a.clin;