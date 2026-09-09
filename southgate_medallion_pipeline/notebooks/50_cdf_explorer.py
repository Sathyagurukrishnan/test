# Databricks notebook source
# MAGIC %md
# MAGIC # 50 - Change Data Feed (CDF) explorer
# MAGIC A learning notebook, not part of the scheduled job. Run it after at least one
# MAGIC pipeline run (ideally two, so there are updates to look at).
# MAGIC
# MAGIC **What CDF is:** with `delta.enableChangeDataFeed = true` (we set it via the
# MAGIC `enable_cdf` flag in `ops.pipeline_metadata`), Delta records row-level changes
# MAGIC alongside each commit. Instead of re-reading a whole table to find what changed,
# MAGIC a downstream consumer asks "give me every insert/update/delete since version N".
# MAGIC
# MAGIC Every change row carries three extra columns:
# MAGIC | column | meaning |
# MAGIC |---|---|
# MAGIC | `_change_type` | `insert`, `delete`, `update_preimage` (row before), `update_postimage` (row after) |
# MAGIC | `_commit_version` | the Delta table version that produced the change |
# MAGIC | `_commit_timestamp` | when that commit happened |
# MAGIC
# MAGIC This is what makes the gold materialized views refresh **incrementally**, and it's
# MAGIC the standard way to feed downstream systems (reverse ETL, caches, other domains)
# MAGIC without full reloads.

# COMMAND ----------

dbutils.widgets.text("catalog", "retail_lakehouse")
CATALOG = dbutils.widgets.get("catalog")
spark.sql(f"USE CATALOG {CATALOG}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. SQL: `table_changes()`
# MAGIC Simplest way in. Second argument is the starting version (0 = since creation).
# MAGIC You can also use a timestamp string, or add a third argument as the end version.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- everything that ever changed in silver.customers, newest first
# MAGIC SELECT customer_id, loyalty_tier, __is_current,
# MAGIC        _change_type, _commit_version, _commit_timestamp
# MAGIC FROM table_changes('silver.customers', 0)
# MAGIC ORDER BY _commit_version DESC, customer_id
# MAGIC LIMIT 50;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- how many changes of each type per commit - nice mental model of what MERGE did
# MAGIC SELECT _commit_version, _change_type, COUNT(*) AS rows
# MAGIC FROM table_changes('silver.customers', 0)
# MAGIC GROUP BY ALL
# MAGIC ORDER BY _commit_version, _change_type;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. PySpark batch read
# MAGIC Same feed via the DataFrame API. `startingVersion` / `endingVersion` (or the
# MAGIC `startingTimestamp` / `endingTimestamp` pair) bound the window.

# COMMAND ----------

changes = (spark.read.format("delta")
    .option("readChangeFeed", "true")
    .option("startingVersion", 0)
    .table("silver.products"))

display(changes.select("product_id", "list_price", "_change_type",
                       "_commit_version", "_commit_timestamp")
               .orderBy("_commit_version", "product_id"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. The bookmark pattern (how a real incremental consumer works)
# MAGIC A consumer remembers the last version it processed, then each run reads only
# MAGIC `last_version + 1 .. current`. This is exactly the pattern behind CDC replication.
# MAGIC Below: a tiny consumer that keeps a running count of loyalty-tier changes.

# COMMAND ----------

from pyspark.sql import functions as F
from delta.tables import DeltaTable

spark.sql("""
CREATE TABLE IF NOT EXISTS ops.cdf_bookmarks (
  consumer_name STRING, source_table STRING, last_version BIGINT, updated_at TIMESTAMP
)""")

CONSUMER, SRC = "tier_change_counter", "silver.customers"

# where did we get to last time?
bm = (spark.table("ops.cdf_bookmarks")
      .filter(f"consumer_name = '{CONSUMER}' AND source_table = '{SRC}'")
      .select("last_version").collect())
start_version = (bm[0]["last_version"] + 1) if bm else 0

current_version = (DeltaTable.forName(spark, SRC)
                   .history(1).select("version").first()["version"])

if start_version > current_version:
    print(f"Nothing new: bookmark {start_version - 1} is already at the head.")
else:
    delta_changes = (spark.read.format("delta")
        .option("readChangeFeed", "true")
        .option("startingVersion", start_version)
        .option("endingVersion", current_version)
        .table(SRC)
        # postimage only = the new state of updated rows; inserts count too
        .filter("_change_type IN ('insert', 'update_postimage')"))

    print(f"Processing versions {start_version}..{current_version}: "
          f"{delta_changes.count()} change rows")
    display(delta_changes.groupBy("loyalty_tier", "_change_type").count())

    # advance the bookmark only after successful processing
    (spark.createDataFrame([{"consumer_name": CONSUMER, "source_table": SRC,
                             "last_version": int(current_version)}])
        .withColumn("updated_at", F.current_timestamp())
        .createOrReplaceTempView("bm_stage"))
    spark.sql(f"""
        MERGE INTO ops.cdf_bookmarks t
        USING bm_stage s
          ON t.consumer_name = s.consumer_name AND t.source_table = s.source_table
        WHEN MATCHED THEN UPDATE SET last_version = s.last_version, updated_at = s.updated_at
        WHEN NOT MATCHED THEN INSERT *""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Streaming read
# MAGIC The same feed as a stream - each micro-batch receives only new changes, and the
# MAGIC checkpoint replaces the manual bookmark from section 3. This is how you'd push
# MAGIC silver changes continuously into another system. (Runs for ~30s here, then stops.)

# COMMAND ----------

stream = (spark.readStream.format("delta")
    .option("readChangeFeed", "true")
    .option("startingVersion", 0)
    .table("silver.orders")
    .filter("_change_type != 'update_preimage'")
    .groupBy("_change_type").count())

q = (stream.writeStream
     .format("memory").queryName("cdf_stream_demo")
     .outputMode("complete")
     .trigger(processingTime="10 seconds")
     .start())

import time; time.sleep(30)
display(spark.sql("SELECT * FROM cdf_stream_demo"))
q.stop()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Things worth knowing
# MAGIC - **CDF vs streaming the table directly:** streaming a Delta table (what our silver
# MAGIC   notebook does from bronze) only sees appends. CDF also gives you updates and
# MAGIC   deletes with before/after images - essential once MERGE is involved, which is
# MAGIC   why silver-and-below consumers want CDF.
# MAGIC - **SCD2 + CDF:** on `silver.customers`, one changed customer shows up as an
# MAGIC   `update_pre/postimage` pair (old row flipped to `__is_current = false`) plus an
# MAGIC   `insert` (the new current row). Run the section-2 query after re-uploading a
# MAGIC   modified customers CSV and watch it happen.
# MAGIC - **Retention:** change data lives in `_change_data` under the table location and is
# MAGIC   cleaned up by VACUUM on the same retention as regular files - consumers should
# MAGIC   not fall further behind than the retention window.
# MAGIC - **Cost:** near-zero for MERGE-heavy tables (change files are written as a
# MAGIC   by-product); enabling it everywhere on silver is a reasonable default.
