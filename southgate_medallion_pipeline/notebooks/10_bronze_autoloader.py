# Databricks notebook source
# MAGIC %md
# MAGIC # 10 - Bronze: Auto Loader ingestion (metadata-driven)
# MAGIC Loops over every active entity in `ops.pipeline_metadata` and runs an
# MAGIC Auto Loader (`cloudFiles`) stream from the landing volume into a bronze Delta table.
# MAGIC
# MAGIC Design points:
# MAGIC - `trigger(availableNow=True)`: processes all new files then stops, so it runs as a
# MAGIC   scheduled batch job but keeps Auto Loader's exactly-once file tracking. Drop a new
# MAGIC   CSV into the volume folder and the next run picks up only that file.
# MAGIC - Schema inference + evolution per entity, state stored in the ops volume.
# MAGIC   `rescue` mode captures unexpected columns into `_rescued_data` instead of failing.
# MAGIC - Bronze is append-only, raw as-landed (all strings for CSV) + lineage columns.
# MAGIC - Liquid clustering on `_ingest_ts` for time-based pruning.
# MAGIC - Full audit + retry via pipeline_utils.

# COMMAND ----------

# MAGIC %run ../utils/pipeline_utils

# COMMAND ----------

dbutils.widgets.text("catalog", "retail_lakehouse")
dbutils.widgets.text("entity_filter", "*")   # '*' = all active entities, or comma list
CATALOG = dbutils.widgets.get("catalog")
ENTITY_FILTER = dbutils.widgets.get("entity_filter")

spark.sql(f"USE CATALOG {CATALOG}")
run_ctx = get_run_context()
print(f"run_id={run_ctx['run_id']}")

# COMMAND ----------

from datetime import datetime, timezone
from pyspark.sql import functions as F

STATE = f"/Volumes/{CATALOG}/ops/pipeline_state"

meta = spark.table("ops.pipeline_metadata").filter("is_active = true")
if ENTITY_FILTER != "*":
    wanted = [e.strip() for e in ENTITY_FILTER.split(",")]
    meta = meta.filter(F.col("entity_name").isin(wanted))
entities = [r.asDict() for r in meta.orderBy("priority", "entity_name").collect()]
print(f"{len(entities)} entities to ingest")

# COMMAND ----------

def ingest_entity(m):
    """One Auto Loader micro-batch run for a single entity."""
    entity, tgt = m["entity_name"], m["bronze_table"]

    reader = (spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", m["file_format"])
        .option("cloudFiles.schemaLocation", f"{STATE}/schemas/{entity}")
        .option("cloudFiles.schemaEvolutionMode", "rescue")
        .option("cloudFiles.inferColumnTypes", "false")      # bronze keeps strings; silver casts
        .option("cloudFiles.maxFilesPerTrigger", 1000))
    for k, v in (m["format_options"] or {}).items():
        reader = reader.option(k, v)

    df = (reader.load(m["source_path"])
        .withColumn("_ingest_ts", F.current_timestamp())
        .withColumn("_source_file", F.col("_metadata.file_path"))
        .withColumn("_file_modified", F.col("_metadata.file_modification_time"))
        .withColumn("_run_id", F.lit(run_ctx["run_id"])))

    q = (df.writeStream
        .option("checkpointLocation", f"{STATE}/checkpoints/{entity}")
        .option("mergeSchema", "true")
        .trigger(availableNow=True)
        .toTable(tgt))
    q.awaitTermination()

    # rows written in this run (cheap: bronze is append-only and stamped with _run_id)
    written = (spark.table(tgt)
               .filter(F.col("_run_id") == run_ctx["run_id"]).count())

    # enable liquid clustering + CDF once per table (no-op if already set)
    spark.sql(f"ALTER TABLE {tgt} CLUSTER BY (_ingest_ts)")
    spark.sql(f"ALTER TABLE {tgt} SET TBLPROPERTIES "
              f"('delta.enableChangeDataFeed' = 'true')")
    return written

# COMMAND ----------

failures = []
for m in entities:
    entity = m["entity_name"]
    start = datetime.now(timezone.utc)
    try:
        wrapped = retry_with_backoff(max_retries=3, base_delay=15,
                                     run_ctx=run_ctx, layer="BRONZE", entity=entity)(ingest_entity)
        written, retries = wrapped(m)
        audit_event(run_ctx, "BRONZE", entity, "SUCCESS", start,
                    records_read=written, records_written=written, retry_count=retries)
        print(f"[bronze] {entity}: {written} new rows -> {m['bronze_table']}")
    except Exception as exc:
        audit_event(run_ctx, "BRONZE", entity, "FAILED", start,
                    retry_count=3, error_summary=str(exc)[:500])
        failures.append(entity)
        print(f"[bronze] FAILED {entity}: {exc}")

# Fail the task only after all entities were attempted, so one bad source
# doesn't block the other nine.
if failures:
    raise RuntimeError(f"Bronze ingestion failed for: {failures}")

print("Bronze layer complete")
