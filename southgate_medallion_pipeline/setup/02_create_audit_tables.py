# Databricks notebook source
# MAGIC %md
# MAGIC # 02 - Audit & Error Log Tables
# MAGIC - `ops.pipeline_audit_log` - one row per entity per layer per run (counts, timings, status, retries)
# MAGIC - `ops.pipeline_error_log` - full stack traces for failures
# MAGIC - `ops.dq_quarantine` - rows that failed QUARANTINE-level quality rules, kept for replay

# COMMAND ----------

dbutils.widgets.text("catalog", "retail_lakehouse")
CATALOG = dbutils.widgets.get("catalog")
spark.sql(f"USE CATALOG {CATALOG}")

# COMMAND ----------

spark.sql("""
CREATE TABLE IF NOT EXISTS ops.pipeline_audit_log (
  audit_id          BIGINT GENERATED ALWAYS AS IDENTITY,
  run_id            STRING,          -- Databricks job run id (or uuid for interactive)
  pipeline_name     STRING,
  layer             STRING,          -- BRONZE / SILVER / GOLD
  entity_name       STRING,
  start_ts          TIMESTAMP,
  end_ts            TIMESTAMP,
  duration_sec      DOUBLE,
  status            STRING,          -- RUNNING / SUCCESS / FAILED / SKIPPED
  records_read      BIGINT,
  records_written   BIGINT,
  records_quarantined BIGINT,
  records_warned    BIGINT,
  retry_count       INT,
  error_summary     STRING,
  triggered_by      STRING
)
CLUSTER BY (start_ts, entity_name)
COMMENT 'Run-level audit: one row per entity per layer per run'
""")

spark.sql("""
CREATE TABLE IF NOT EXISTS ops.pipeline_error_log (
  error_id      BIGINT GENERATED ALWAYS AS IDENTITY,
  run_id        STRING,
  layer         STRING,
  entity_name   STRING,
  error_ts      TIMESTAMP,
  attempt       INT,
  error_type    STRING,
  error_message STRING,
  stack_trace   STRING
)
CLUSTER BY (error_ts)
COMMENT 'Full stack traces for pipeline failures'
""")

spark.sql("""
CREATE TABLE IF NOT EXISTS ops.dq_quarantine (
  run_id        STRING,
  entity_name   STRING,
  failed_rules  ARRAY<STRING>,
  quarantined_ts TIMESTAMP,
  record_json   STRING            -- full source row as JSON so it can be repaired/replayed
)
CLUSTER BY (entity_name, quarantined_ts)
COMMENT 'Rows failing QUARANTINE-level quality rules'
""")

print("ops.pipeline_audit_log, ops.pipeline_error_log, ops.dq_quarantine ready")
