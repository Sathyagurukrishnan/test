# Databricks notebook source
# MAGIC %md
# MAGIC # 40 - Pipeline Health Check
# MAGIC Final task (runs with `run_if: ALL_DONE`). Reads this run's rows from the audit
# MAGIC log, prints a summary, and fails the job if anything failed upstream or if
# MAGIC quarantine volume looks abnormal, so a partially-broken run never looks green.

# COMMAND ----------

# MAGIC %run ../utils/pipeline_utils

# COMMAND ----------

dbutils.widgets.text("catalog", "retail_lakehouse")
CATALOG = dbutils.widgets.get("catalog")
spark.sql(f"USE CATALOG {CATALOG}")

run_ctx = get_run_context()
run_id = run_ctx["run_id"]

# COMMAND ----------

from pyspark.sql import functions as F

audit = spark.table("ops.pipeline_audit_log").filter(F.col("run_id") == run_id)

summary = (audit.groupBy("layer")
    .agg(F.count("*").alias("entities"),
         F.sum(F.when(F.col("status") == "SUCCESS", 1).otherwise(0)).alias("succeeded"),
         F.sum(F.when(F.col("status") == "FAILED", 1).otherwise(0)).alias("failed"),
         F.sum("records_written").alias("records_written"),
         F.sum("records_quarantined").alias("quarantined"),
         F.sum("records_warned").alias("warned"),
         F.round(F.sum("duration_sec"), 1).alias("total_sec"))
    .orderBy("layer"))

display(summary)

# COMMAND ----------

failed = [r["entity_name"] for r in
          audit.filter("status = 'FAILED'").select("entity_name").collect()]

total_written = audit.agg(F.sum("records_written")).first()[0] or 0
total_quarantined = audit.agg(F.sum("records_quarantined")).first()[0] or 0
quarantine_pct = (100.0 * total_quarantined / total_written) if total_written else 0

QUARANTINE_THRESHOLD_PCT = 5.0

print(f"run_id={run_id}  written={total_written}  "
      f"quarantined={total_quarantined} ({quarantine_pct:.2f}%)")

if failed:
    raise RuntimeError(f"Run {run_id} had failures: {sorted(set(failed))}. "
                       f"See ops.pipeline_error_log for stack traces.")

if quarantine_pct > QUARANTINE_THRESHOLD_PCT:
    raise RuntimeError(
        f"Quarantine rate {quarantine_pct:.1f}% exceeds {QUARANTINE_THRESHOLD_PCT}% - "
        f"likely a bad source file. Check ops.dq_quarantine for run_id = '{run_id}'")

print("Pipeline healthy")
