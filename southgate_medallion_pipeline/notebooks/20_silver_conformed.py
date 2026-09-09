# Databricks notebook source
# MAGIC %md
# MAGIC # 20 - Silver: Conformed layer (metadata-driven)
# MAGIC Streams incrementally from each bronze Delta table (own checkpoint, exactly-once)
# MAGIC with `trigger(availableNow=True)` and, per micro-batch:
# MAGIC 1. **Conform** - cast string columns to typed columns using `column_casts` from metadata
# MAGIC    (`try_cast`, so a bad value becomes NULL and gets caught by a rule instead of failing the run)
# MAGIC 2. **Deduplicate** - latest row per primary key (order_by_col, falling back to `_ingest_ts`)
# MAGIC 3. **Quality rules** - QUARANTINE rows out to `ops.dq_quarantine`, count WARN rows
# MAGIC 4. **MERGE** - SCD Type 1 upsert, or SCD Type 2 history (customers, products)
# MAGIC 5. **Liquid clustering** from metadata (`CLUSTER BY AUTO` when no keys specified)

# COMMAND ----------

# MAGIC %run ../utils/pipeline_utils

# COMMAND ----------

dbutils.widgets.text("catalog", "retail_lakehouse")
dbutils.widgets.text("entity_filter", "*")
CATALOG = dbutils.widgets.get("catalog")
ENTITY_FILTER = dbutils.widgets.get("entity_filter")

spark.sql(f"USE CATALOG {CATALOG}")
run_ctx = get_run_context()

# COMMAND ----------

from datetime import datetime, timezone
from pyspark.sql import functions as F, Window
from delta.tables import DeltaTable

STATE = f"/Volumes/{CATALOG}/ops/pipeline_state"

meta = spark.table("ops.pipeline_metadata").filter("is_active = true")
if ENTITY_FILTER != "*":
    meta = meta.filter(F.col("entity_name").isin([e.strip() for e in ENTITY_FILTER.split(",")]))
entities = [r.asDict(recursive=True) for r in meta.orderBy("priority", "entity_name").collect()]

# COMMAND ----------

def conform(df, m):
    """Cast metadata-declared columns; everything else stays as-is. Drops bronze lineage cols
    except _ingest_ts / _source_file which silver keeps for traceability."""
    for col, typ in (m["column_casts"] or {}).items():
        if col in df.columns:
            df = df.withColumn(col, F.expr(f"try_cast(`{col}` AS {typ})"))
    drop = [c for c in ("_rescued_data", "_file_modified", "_run_id") if c in df.columns]
    return df.drop(*drop)

def dedupe(df, m):
    pks = m["primary_keys"]
    order_col = m["order_by_col"] or "_ingest_ts"
    w = Window.partitionBy(*pks).orderBy(F.col(order_col).desc_nulls_last(),
                                         F.col("_ingest_ts").desc())
    return (df.withColumn("_rn", F.row_number().over(w))
              .filter("_rn = 1").drop("_rn"))

def ensure_silver_table(df, m):
    tgt = m["silver_table"]
    if not spark.catalog.tableExists(tgt):
        # Layout choice from metadata. A Delta table has exactly one physical layout:
        # PARTITIONED BY (classic, one folder per value - only for low-cardinality
        # columns like a date on large tables) OR liquid clustering (the modern
        # default: no folder explosion, keys can be changed later with ALTER TABLE).
        if m["partition_keys"]:
            layout = "PARTITIONED BY (" + ", ".join(m["partition_keys"]) + ")"
        else:
            cluster = ", ".join(m["cluster_keys"]) if m["cluster_keys"] else "AUTO"
            layout = f"CLUSTER BY ({cluster})"
        cdf = "true" if m.get("enable_cdf", True) else "false"
        cols = ", ".join(f"`{f.name}` {f.dataType.simpleString()}" for f in df.schema.fields)
        extra = ""
        if m["scd_type"] == 2:
            extra = ", __start_ts TIMESTAMP, __end_ts TIMESTAMP, __is_current BOOLEAN"
        spark.sql(f"""
            CREATE TABLE {tgt} ({cols}{extra})
            {layout}
            TBLPROPERTIES (
              delta.enableChangeDataFeed = {cdf},
              delta.enableDeletionVectors = true,
              delta.tuneFileSizesForRewrites = true
            )""")

def merge_scd1(batch_df, m):
    tgt = DeltaTable.forName(spark, m["silver_table"])
    cond = " AND ".join(f"t.`{k}` = s.`{k}`" for k in m["primary_keys"])
    (tgt.alias("t").merge(batch_df.alias("s"), cond)
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute())

def merge_scd2(batch_df, m):
    """Close the current row when any tracked attribute changed, insert the new version."""
    tgt_name = m["silver_table"]
    pks = m["primary_keys"]
    attrs = [c for c in batch_df.columns if c not in pks + ["_ingest_ts", "_source_file"]]
    change_cond = " OR ".join(f"NOT (t.`{c}` <=> s.`{c}`)" for c in attrs)
    pk_cond = " AND ".join(f"t.`{k}` = s.`{k}`" for k in pks)

    staged = (batch_df
              .withColumn("__start_ts", F.current_timestamp())
              .withColumn("__end_ts", F.lit(None).cast("timestamp"))
              .withColumn("__is_current", F.lit(True)))

    tgt = DeltaTable.forName(spark, tgt_name)
    # Step 1: expire current rows whose attributes changed
    (tgt.alias("t")
        .merge(staged.alias("s"), f"{pk_cond} AND t.__is_current = true")
        .whenMatchedUpdate(condition=change_cond,
                           set={"__end_ts": "current_timestamp()",
                                "__is_current": "false"})
        .execute())
    # Step 2: insert rows that have no current version (new keys or just-expired)
    existing_current = (spark.table(tgt_name).filter("__is_current = true")
                        .select(*pks).withColumn("__x", F.lit(1)))
    to_insert = (staged.join(existing_current, on=pks, how="left")
                       .filter("__x IS NULL").drop("__x")
                       .select(*spark.table(tgt_name).columns))   # align column order
    to_insert.write.format("delta").mode("append").saveAsTable(tgt_name)

def process_entity(m):
    """Stream bronze -> silver for one entity. Returns (read, written, quarantined, warned)."""
    entity = m["entity_name"]
    counters = {"read": 0, "written": 0, "q": 0, "w": 0}

    def handle_batch(batch_df, batch_id):
        if batch_df.isEmpty():
            return
        counters["read"] += batch_df.count()
        df = conform(batch_df, m)
        df = dedupe(df, m)
        df, qn, wn = apply_quality_rules(df, m["quality_rules"], run_ctx, entity)
        counters["q"] += qn
        counters["w"] += wn
        if df.isEmpty():
            return
        ensure_silver_table(df, m)
        if m["scd_type"] == 2:
            merge_scd2(df, m)
        else:
            merge_scd1(df, m)
        counters["written"] += df.count()

    q = (spark.readStream.table(m["bronze_table"])
         .writeStream
         .option("checkpointLocation", f"{STATE}/checkpoints/silver_{entity}")
         .trigger(availableNow=True)
         .foreachBatch(handle_batch)
         .start())
    q.awaitTermination()
    return counters

# COMMAND ----------

failures = []
for m in entities:
    entity = m["entity_name"]
    start = datetime.now(timezone.utc)
    try:
        wrapped = retry_with_backoff(max_retries=3, base_delay=15,
                                     run_ctx=run_ctx, layer="SILVER", entity=entity)(process_entity)
        c, retries = wrapped(m)
        audit_event(run_ctx, "SILVER", entity, "SUCCESS", start,
                    records_read=c["read"], records_written=c["written"],
                    records_quarantined=c["q"], records_warned=c["w"],
                    retry_count=retries)
        print(f"[silver] {entity}: read={c['read']} written={c['written']} "
              f"quarantined={c['q']} warned={c['w']}")
    except Exception as exc:
        audit_event(run_ctx, "SILVER", entity, "FAILED", start,
                    retry_count=3, error_summary=str(exc)[:500])
        failures.append(entity)
        print(f"[silver] FAILED {entity}: {exc}")

if failures:
    raise RuntimeError(f"Silver processing failed for: {failures}")

print("Silver layer complete")
