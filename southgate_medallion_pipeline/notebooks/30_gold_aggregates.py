# Databricks notebook source
# MAGIC %md
# MAGIC # 30 - Gold: Business aggregates
# MAGIC Consumption-ready star-style tables rebuilt from silver each run.
# MAGIC - `CREATE OR REPLACE ... CLUSTER BY AUTO` - automatic liquid clustering lets
# MAGIC   Databricks pick and evolve clustering keys from actual query patterns.
# MAGIC - Full audit + retry per table.
# MAGIC - The always-fresh, incrementally-maintained views live in
# MAGIC   `31_gold_materialized_views.sql` (run as a SQL task on a serverless warehouse).

# COMMAND ----------

# MAGIC %run ../utils/pipeline_utils

# COMMAND ----------

dbutils.widgets.text("catalog", "retail_lakehouse")
CATALOG = dbutils.widgets.get("catalog")
spark.sql(f"USE CATALOG {CATALOG}")
run_ctx = get_run_context()

from datetime import datetime, timezone

# COMMAND ----------

GOLD_TABLES = {

"gold.fct_daily_sales": """
CREATE OR REPLACE TABLE gold.fct_daily_sales
CLUSTER BY AUTO
TBLPROPERTIES (delta.enableDeletionVectors = true)
COMMENT 'Daily sales by store and channel, net of cancellations'
AS
SELECT
  CAST(o.order_ts AS DATE)              AS sales_date,
  o.store_id,
  st.store_name,
  st.state,
  o.channel,
  COUNT(DISTINCT o.order_id)            AS orders,
  SUM(oi.quantity)                      AS units_sold,
  SUM(oi.line_total)                    AS gross_revenue,
  SUM(oi.discount_amount)               AS total_discounts,
  SUM(oi.line_total) - SUM(oi.quantity * p.unit_cost) AS gross_margin
FROM silver.orders o
JOIN silver.order_items oi ON o.order_id = oi.order_id
JOIN (SELECT * FROM silver.products WHERE __is_current) p
  ON oi.product_id = p.product_id
LEFT JOIN silver.stores st ON o.store_id = st.store_id
WHERE o.order_status = 'COMPLETED'
GROUP BY ALL
""",

"gold.dim_customer_360": """
CREATE OR REPLACE TABLE gold.dim_customer_360
CLUSTER BY AUTO
TBLPROPERTIES (delta.enableDeletionVectors = true)
COMMENT 'One row per customer: lifetime value, recency, returns behaviour'
AS
WITH orders_agg AS (
  SELECT o.customer_id,
         COUNT(DISTINCT o.order_id)     AS lifetime_orders,
         SUM(oi.line_total)             AS lifetime_revenue,
         MAX(CAST(o.order_ts AS DATE))  AS last_order_date
  FROM silver.orders o
  JOIN silver.order_items oi ON o.order_id = oi.order_id
  WHERE o.order_status = 'COMPLETED'
  GROUP BY o.customer_id
),
returns_agg AS (
  SELECT o.customer_id, COUNT(*) AS total_returns, SUM(r.refund_amount) AS total_refunds
  FROM silver.returns r
  JOIN silver.orders o ON r.order_id = o.order_id
  WHERE r.return_status = 'APPROVED'
  GROUP BY o.customer_id
)
SELECT
  c.customer_id, c.first_name, c.last_name, c.email, c.city, c.state,
  c.loyalty_tier, c.preferred_channel,
  COALESCE(oa.lifetime_orders, 0)   AS lifetime_orders,
  COALESCE(oa.lifetime_revenue, 0)  AS lifetime_revenue,
  oa.last_order_date,
  DATEDIFF(CURRENT_DATE(), oa.last_order_date) AS days_since_last_order,
  COALESCE(ra.total_returns, 0)     AS total_returns,
  COALESCE(ra.total_refunds, 0)     AS total_refunds,
  CASE
    WHEN oa.lifetime_revenue >= 5000 THEN 'VIP'
    WHEN oa.lifetime_revenue >= 1000 THEN 'HIGH_VALUE'
    WHEN oa.lifetime_revenue > 0     THEN 'STANDARD'
    ELSE 'PROSPECT'
  END AS value_segment
FROM (SELECT * FROM silver.customers WHERE __is_current) c
LEFT JOIN orders_agg  oa ON c.customer_id = oa.customer_id
LEFT JOIN returns_agg ra ON c.customer_id = ra.customer_id
""",

"gold.fct_carrier_performance": """
CREATE OR REPLACE TABLE gold.fct_carrier_performance
CLUSTER BY AUTO
TBLPROPERTIES (delta.enableDeletionVectors = true)
COMMENT 'Monthly carrier scorecard: on-time %, transit variance, loss rate'
AS
SELECT
  DATE_TRUNC('MONTH', s.shipped_ts)     AS ship_month,
  s.carrier,
  COUNT(*)                              AS shipments,
  SUM(CASE WHEN s.shipment_status = 'DELIVERED'
            AND DATEDIFF(s.delivered_ts, s.shipped_ts) <= s.promised_transit_days
           THEN 1 ELSE 0 END)           AS on_time_deliveries,
  ROUND(100.0 * SUM(CASE WHEN s.shipment_status = 'DELIVERED'
            AND DATEDIFF(s.delivered_ts, s.shipped_ts) <= s.promised_transit_days
           THEN 1 ELSE 0 END) / COUNT(*), 1) AS on_time_pct,
  ROUND(AVG(CASE WHEN s.shipment_status = 'DELIVERED'
           THEN DATEDIFF(s.delivered_ts, s.shipped_ts) END), 2) AS avg_transit_days,
  SUM(CASE WHEN s.shipment_status = 'LOST' THEN 1 ELSE 0 END)  AS lost_shipments,
  ROUND(AVG(s.weight_kg), 1)            AS avg_weight_kg
FROM silver.shipments s
GROUP BY ALL
""",

"gold.fct_inventory_position": """
CREATE OR REPLACE TABLE gold.fct_inventory_position
CLUSTER BY AUTO
TBLPROPERTIES (delta.enableDeletionVectors = true)
COMMENT 'Latest stock position per store/product with replenishment flags'
AS
WITH latest AS (
  SELECT *, ROW_NUMBER() OVER (PARTITION BY store_id, product_id
                               ORDER BY snapshot_date DESC) AS rn
  FROM silver.inventory_snapshots
)
SELECT
  l.snapshot_date, l.store_id, st.store_name, l.product_id,
  p.product_name, p.category,
  l.qty_on_hand, l.qty_reserved, l.qty_on_order, l.reorder_point,
  l.qty_on_hand - l.qty_reserved AS qty_available,
  CASE
    WHEN l.qty_on_hand = 0 THEN 'OUT_OF_STOCK'
    WHEN l.qty_on_hand - l.qty_reserved <= l.reorder_point THEN 'REORDER_NOW'
    ELSE 'HEALTHY'
  END AS stock_status
FROM latest l
LEFT JOIN silver.stores st ON l.store_id = st.store_id
LEFT JOIN (SELECT * FROM silver.products WHERE __is_current) p
  ON l.product_id = p.product_id
WHERE l.rn = 1
"""
}

# COMMAND ----------

failures = []
for table, ddl in GOLD_TABLES.items():
    start = datetime.now(timezone.utc)
    try:
        build = retry_with_backoff(max_retries=2, base_delay=20,
                                   run_ctx=run_ctx, layer="GOLD",
                                   entity=table)(lambda d=ddl: spark.sql(d))
        _, retries = build()
        written = spark.table(table).count()
        audit_event(run_ctx, "GOLD", table, "SUCCESS", start,
                    records_written=written, retry_count=retries)
        print(f"[gold] {table}: {written} rows")
    except Exception as exc:
        audit_event(run_ctx, "GOLD", table, "FAILED", start,
                    retry_count=2, error_summary=str(exc)[:500])
        failures.append(table)
        print(f"[gold] FAILED {table}: {exc}")

if failures:
    raise RuntimeError(f"Gold build failed for: {failures}")

print("Gold layer complete")
