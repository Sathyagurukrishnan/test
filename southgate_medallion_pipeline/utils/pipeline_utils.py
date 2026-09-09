# Databricks notebook source
# MAGIC %md
# MAGIC # utils / pipeline_utils
# MAGIC Shared helpers used by every layer notebook (imported with `%run ../utils/pipeline_utils`):
# MAGIC - `get_run_context()` - resolves job run id / trigger for audit rows
# MAGIC - `audit_start / audit_success / audit_failure` - writes to `ops.pipeline_audit_log`
# MAGIC - `log_error()` - full stack trace to `ops.pipeline_error_log`
# MAGIC - `retry_with_backoff` - decorator: exponential backoff + jitter, logs every attempt
# MAGIC - `apply_quality_rules()` - metadata-driven DQ: quarantine or warn per rule

# COMMAND ----------

import time, uuid, json, random, traceback, functools
from datetime import datetime, timezone
from pyspark.sql import functions as F

PIPELINE_NAME = "retail_medallion"

# ---------------------------------------------------------------- run context
def get_run_context():
    """Job run id when running inside a Databricks Job, else a UUID for interactive runs."""
    try:
        ctx = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
        tags = json.loads(ctx.toJson()).get("tags", {})
        run_id = tags.get("multitaskParentRunId") or tags.get("runId") or str(uuid.uuid4())
        triggered_by = tags.get("user", "unknown")
    except Exception:
        run_id, triggered_by = str(uuid.uuid4()), "interactive"
    return {"run_id": str(run_id), "triggered_by": triggered_by}

# ---------------------------------------------------------------- audit logging
def _write_audit(row: dict):
    (spark.createDataFrame([row])
        .withColumn("start_ts", F.to_timestamp("start_ts"))
        .withColumn("end_ts", F.to_timestamp("end_ts"))
        .select("run_id","pipeline_name","layer","entity_name","start_ts","end_ts",
                "duration_sec","status","records_read","records_written",
                "records_quarantined","records_warned","retry_count",
                "error_summary","triggered_by")
        .write.mode("append").saveAsTable("ops.pipeline_audit_log"))

def audit_event(run_ctx, layer, entity, status, start_ts, *, records_read=0,
                records_written=0, records_quarantined=0, records_warned=0,
                retry_count=0, error_summary=None):
    end = datetime.now(timezone.utc)
    _write_audit({
        "run_id": run_ctx["run_id"], "pipeline_name": PIPELINE_NAME,
        "layer": layer, "entity_name": entity,
        "start_ts": start_ts.isoformat(), "end_ts": end.isoformat(),
        "duration_sec": round((end - start_ts).total_seconds(), 2),
        "status": status, "records_read": int(records_read),
        "records_written": int(records_written),
        "records_quarantined": int(records_quarantined),
        "records_warned": int(records_warned),
        "retry_count": int(retry_count),
        "error_summary": "" if error_summary is None else str(error_summary)[:2000],
        "triggered_by": run_ctx["triggered_by"],
    })

def log_error(run_ctx, layer, entity, attempt, exc):
    row = {
        "run_id": run_ctx["run_id"], "layer": layer, "entity_name": entity,
        "error_ts": datetime.now(timezone.utc).isoformat(), "attempt": int(attempt),
        "error_type": type(exc).__name__, "error_message": str(exc)[:4000],
        "stack_trace": traceback.format_exc()[:20000],
    }
    (spark.createDataFrame([row])
        .withColumn("error_ts", F.to_timestamp("error_ts"))
        .select("run_id","layer","entity_name","error_ts","attempt",
                "error_type","error_message","stack_trace")
        .write.mode("append").saveAsTable("ops.pipeline_error_log"))

# ---------------------------------------------------------------- retry decorator
def retry_with_backoff(max_retries=3, base_delay=10, max_delay=120,
                       run_ctx=None, layer=None, entity=None):
    """
    Exponential backoff with jitter. Every failed attempt is written to the
    error log; the caller gets the exception only after the final attempt.
    Retries handle transient issues (cloud storage throttling, concurrent
    MERGE conflicts). Deterministic errors still fail fast after max_retries.
    """
    def decorator(fn):
        @functools.wraps(fn)
        def wrapper(*args, **kwargs):
            attempt = 0
            while True:
                try:
                    result = fn(*args, **kwargs)
                    return result, attempt          # surface retry_count to audit
                except Exception as exc:
                    attempt += 1
                    if run_ctx:
                        log_error(run_ctx, layer, entity, attempt, exc)
                    if attempt > max_retries:
                        raise
                    delay = min(max_delay, base_delay * (2 ** (attempt - 1)))
                    delay += random.uniform(0, delay * 0.25)   # jitter
                    print(f"[retry] {entity} {layer} attempt {attempt}/{max_retries} "
                          f"failed: {exc}. Sleeping {delay:.0f}s")
                    time.sleep(delay)
        return wrapper
    return decorator

# ---------------------------------------------------------------- data quality
def apply_quality_rules(df, quality_rules, run_ctx, entity):
    """
    Metadata-driven DQ. Rules come from ops.pipeline_metadata.quality_rules.
    - QUARANTINE rules: failing rows are removed from the clean set and appended
      (as JSON) to ops.dq_quarantine for inspection/replay.
    - WARN rules: rows stay, count is reported in the audit row.
    Returns (clean_df, quarantined_count, warned_count).
    """
    if not quality_rules:
        return df, 0, 0

    quarantine_rules = [(r["rule_name"], r["expression"]) for r in quality_rules
                        if r["action"] == "QUARANTINE"]
    warn_rules = [(r["rule_name"], r["expression"]) for r in quality_rules
                  if r["action"] == "WARN"]

    warned = 0
    for name, expr in warn_rules:
        n = df.filter(f"NOT ({expr})").count()
        if n:
            print(f"[dq-warn] {entity}: {n} rows fail '{name}'")
        warned += n

    if not quarantine_rules:
        return df, 0, warned

    fail_flags = [F.when(~F.expr(expr), F.lit(name)) for name, expr in quarantine_rules]
    flagged = df.withColumn("_failed_rules",
                            F.array_compact(F.array(*fail_flags)))
    bad = flagged.filter(F.size("_failed_rules") > 0)
    clean = flagged.filter(F.size("_failed_rules") == 0).drop("_failed_rules")

    bad_count = bad.count()
    if bad_count:
        (bad.select(
                F.lit(run_ctx["run_id"]).alias("run_id"),
                F.lit(entity).alias("entity_name"),
                F.col("_failed_rules").alias("failed_rules"),
                F.current_timestamp().alias("quarantined_ts"),
                F.to_json(F.struct([c for c in df.columns])).alias("record_json"))
            .write.mode("append").saveAsTable("ops.dq_quarantine"))
        print(f"[dq-quarantine] {entity}: {bad_count} rows quarantined")

    return clean, bad_count, warned

print("pipeline_utils loaded")
