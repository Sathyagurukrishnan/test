# Databricks notebook source
# MAGIC %md
# MAGIC # 01 - Metadata Table (drives the whole pipeline)
# MAGIC One row per entity. The bronze/silver/gold notebooks read this table and loop
# MAGIC over active entities, so onboarding a new source = inserting one row here.
# MAGIC No code change needed.

# COMMAND ----------

dbutils.widgets.text("catalog", "retail_lakehouse")
CATALOG = dbutils.widgets.get("catalog")
spark.sql(f"USE CATALOG {CATALOG}")

# COMMAND ----------

spark.sql("""
CREATE TABLE IF NOT EXISTS ops.pipeline_metadata (
  entity_name          STRING  NOT NULL,
  source_system        STRING,
  source_path          STRING  NOT NULL,   -- volume folder Auto Loader watches
  file_format          STRING  NOT NULL,   -- csv / json / parquet
  format_options       MAP<STRING,STRING>, -- passed straight to Auto Loader reader
  bronze_table         STRING  NOT NULL,
  silver_table         STRING  NOT NULL,
  primary_keys         ARRAY<STRING>,      -- dedupe + MERGE keys for silver
  order_by_col         STRING,             -- tiebreaker for dedupe (latest wins)
  cluster_keys         ARRAY<STRING>,      -- liquid clustering keys; empty = CLUSTER BY AUTO
  partition_keys       ARRAY<STRING>,      -- Hive-style partitioning; mutually exclusive with
                                           -- cluster_keys (a Delta table has one layout or the other)
  enable_cdf           BOOLEAN,            -- delta.enableChangeDataFeed on the silver table
  column_casts         MAP<STRING,STRING>, -- silver conforming: column -> target type
  scd_type             INT,                -- 1 = merge/upsert, 2 = history (customers/products)
  quality_rules        ARRAY<STRUCT<rule_name STRING, expression STRING, action STRING>>,
                                           -- action: QUARANTINE (row removed) or WARN (flagged only)
  load_type            STRING,             -- INCREMENTAL / SNAPSHOT
  is_active            BOOLEAN,
  priority             INT,                -- load order (dims before facts)
  created_at           TIMESTAMP,
  updated_at           TIMESTAMP,
  CONSTRAINT pk_pipeline_metadata PRIMARY KEY (entity_name)
)
COMMENT 'Metadata-driven config: one row per entity, consumed by bronze/silver/gold notebooks'
""")

# COMMAND ----------

from pyspark.sql import functions as F

VOL = f"/Volumes/{CATALOG}/landing/source_files"
CSV_OPTS = {"header": "true", "inferSchema": "false"}  # types are cast in silver, bronze stays strings

def rule(name, expr, action="QUARANTINE"):
    return {"rule_name": name, "expression": expr, "action": action}

# silver conforming: which bronze (string) columns get cast, and to what
CASTS = {
    "customers":  {"postcode": "INT", "registered_at": "TIMESTAMP"},
    "products":   {"unit_cost": "DECIMAL(10,2)", "list_price": "DECIMAL(10,2)",
                   "created_at": "TIMESTAMP"},
    "stores":     {"floor_area_sqm": "DECIMAL(10,1)", "opened_date": "DATE"},
    "suppliers":  {"lead_time_days": "INT", "otif_score_pct": "DECIMAL(4,1)"},
    "orders":     {"order_ts": "TIMESTAMP", "shipping_fee": "DECIMAL(10,2)"},
    "order_items": {"line_number": "INT", "quantity": "INT",
                    "unit_price": "DECIMAL(10,2)", "discount_amount": "DECIMAL(10,2)",
                    "line_total": "DECIMAL(12,2)"},
    "shipments":  {"shipped_ts": "TIMESTAMP", "delivered_ts": "TIMESTAMP",
                   "promised_transit_days": "INT", "weight_kg": "DECIMAL(6,1)"},
    "inventory_snapshots": {"snapshot_date": "DATE", "qty_on_hand": "INT",
                            "qty_reserved": "INT", "qty_on_order": "INT",
                            "reorder_point": "INT"},
    "payments":   {"amount": "DECIMAL(12,2)", "payment_ts": "TIMESTAMP"},
    "returns":    {"qty_returned": "INT", "refund_amount": "DECIMAL(10,2)",
                   "returned_ts": "TIMESTAMP"},
}

ENTITIES = [
    # entity, pks, order_by, cluster_keys, scd, quality_rules, load_type, priority
    ("customers",  ["customer_id"], "registered_at", ["customer_id"], 2,
     [rule("valid_pk", "customer_id IS NOT NULL"),
      rule("valid_email", "email RLIKE '^[^@\\\\s]+@[^@\\\\s]+\\\\.[^@\\\\s]+$'", "WARN")],
     "INCREMENTAL", 1),
    ("products",   ["product_id"], "created_at", ["product_id", "category"], 2,
     [rule("valid_pk", "product_id IS NOT NULL"),
      rule("non_negative_price", "CAST(list_price AS DOUBLE) >= 0")],
     "INCREMENTAL", 1),
    ("stores",     ["store_id"], None, [], 1,
     [rule("valid_pk", "store_id IS NOT NULL")], "SNAPSHOT", 1),
    ("suppliers",  ["supplier_id"], None, [], 1,
     [rule("valid_pk", "supplier_id IS NOT NULL")], "SNAPSHOT", 1),
    ("orders",     ["order_id"], "order_ts", ["order_ts", "store_id"], 1,
     [rule("valid_pk", "order_id IS NOT NULL"),
      rule("known_customer", "customer_id LIKE 'CUST%'", "WARN")],
     "INCREMENTAL", 2),
    ("order_items", ["order_line_id"], None, ["order_id", "product_id"], 1,
     [rule("valid_pk", "order_line_id IS NOT NULL"),
      rule("positive_qty", "CAST(quantity AS INT) > 0")],
     "INCREMENTAL", 2),
    ("shipments",  ["shipment_id"], "shipped_ts", ["shipped_ts", "carrier"], 1,
     [rule("valid_pk", "shipment_id IS NOT NULL")], "INCREMENTAL", 2),
    # inventory_snapshots is the deliberate PARTITIONED BY example: date-partitioned
    # snapshot data is the classic partitioning use case. cluster_keys stays empty
    # because a table cannot have both partitioning and liquid clustering.
    ("inventory_snapshots", ["snapshot_id"], "snapshot_date", [], 1,
     [rule("valid_pk", "snapshot_id IS NOT NULL"),
      rule("non_negative_stock", "CAST(qty_on_hand AS INT) >= 0")],
     "SNAPSHOT", 2),
    ("payments",   ["payment_id"], "payment_ts", ["payment_ts"], 1,
     [rule("valid_pk", "payment_id IS NOT NULL"),
      rule("positive_amount", "CAST(amount AS DOUBLE) > 0")],
     "INCREMENTAL", 2),
    ("returns",    ["return_id"], "returned_ts", [], 1,
     [rule("valid_pk", "return_id IS NOT NULL")], "INCREMENTAL", 2),
]

rows = []
for name, pks, order_by, ckeys, scd, rules, load_type, prio in ENTITIES:
    rows.append({
        "entity_name": name,
        "source_system": "retail_pos" if name in ("orders", "order_items", "payments", "returns") else "erp",
        "source_path": f"{VOL}/{name}",
        "file_format": "csv",
        "format_options": CSV_OPTS,
        "bronze_table": f"bronze.{name}_raw",
        "silver_table": f"silver.{name}",
        "primary_keys": pks,
        "order_by_col": order_by,
        "cluster_keys": ckeys,
        "partition_keys": ["snapshot_date"] if name == "inventory_snapshots" else [],
        "enable_cdf": True,
        "column_casts": CASTS[name],
        "scd_type": scd,
        "quality_rules": rules,
        "load_type": load_type,
        "is_active": True,
        "priority": prio,
    })

df = (spark.createDataFrame(rows)
      .withColumn("created_at", F.current_timestamp())
      .withColumn("updated_at", F.current_timestamp()))

# Idempotent upsert of the config itself
df.createOrReplaceTempView("meta_stage")
spark.sql("""
MERGE INTO ops.pipeline_metadata t
USING meta_stage s ON t.entity_name = s.entity_name
WHEN MATCHED THEN UPDATE SET
  source_system = s.source_system, source_path = s.source_path,
  file_format = s.file_format, format_options = s.format_options,
  bronze_table = s.bronze_table, silver_table = s.silver_table,
  primary_keys = s.primary_keys, order_by_col = s.order_by_col,
  cluster_keys = s.cluster_keys, partition_keys = s.partition_keys,
  enable_cdf = s.enable_cdf, column_casts = s.column_casts, scd_type = s.scd_type,
  quality_rules = s.quality_rules, load_type = s.load_type,
  is_active = s.is_active, priority = s.priority, updated_at = s.updated_at
WHEN NOT MATCHED THEN INSERT *
""")

display(spark.table("ops.pipeline_metadata").orderBy("priority", "entity_name"))
