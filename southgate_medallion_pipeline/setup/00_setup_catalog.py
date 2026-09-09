# Databricks notebook source
# MAGIC %md
# MAGIC # 00 - Setup: Catalog, Schemas, Volume
# MAGIC Creates the Unity Catalog objects for the medallion pipeline.
# MAGIC Run once. Idempotent (IF NOT EXISTS everywhere).
# MAGIC
# MAGIC After running, upload the 10 CSVs from `source_data/` into the volume:
# MAGIC `/Volumes/retail_lakehouse/landing/source_files/<entity>/` (one folder per entity).

# COMMAND ----------

dbutils.widgets.text("catalog", "retail_lakehouse")
CATALOG = dbutils.widgets.get("catalog")

# COMMAND ----------

spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG}")
spark.sql(f"USE CATALOG {CATALOG}")

for schema in ["landing", "bronze", "silver", "gold", "ops"]:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema}")

# Volume for raw file landing (Auto Loader source) + one for schema/checkpoint state
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.landing.source_files")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.ops.pipeline_state")

# COMMAND ----------

# Create one folder per entity inside the volume so Auto Loader can watch each path
ENTITIES = ["customers", "products", "stores", "suppliers", "orders",
            "order_items", "shipments", "inventory_snapshots", "payments", "returns"]

for e in ENTITIES:
    dbutils.fs.mkdirs(f"/Volumes/{CATALOG}/landing/source_files/{e}")
    dbutils.fs.mkdirs(f"/Volumes/{CATALOG}/ops/pipeline_state/checkpoints/{e}")
    dbutils.fs.mkdirs(f"/Volumes/{CATALOG}/ops/pipeline_state/schemas/{e}")

print("Catalog, schemas and volumes ready.")
print(f"Upload each CSV into /Volumes/{CATALOG}/landing/source_files/<entity>/")

# COMMAND ----------

# MAGIC %md
# MAGIC **Recommended (account/workspace admin, run once):** enable Predictive Optimization
# MAGIC so Databricks automatically runs OPTIMIZE / VACUUM / clustering maintenance:
# MAGIC ```sql
# MAGIC ALTER CATALOG retail_lakehouse ENABLE PREDICTIVE OPTIMIZATION;
# MAGIC ```
