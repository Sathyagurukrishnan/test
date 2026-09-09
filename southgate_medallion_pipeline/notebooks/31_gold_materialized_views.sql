-- Databricks notebook source
-- MAGIC %md
-- MAGIC # 31 - Gold: Materialized Views
-- MAGIC Serving-layer MVs on top of silver. Databricks refreshes these **incrementally**
-- MAGIC where possible (only changed data is recomputed, using the Change Data Feed we
-- MAGIC enabled on silver), which is the key difference vs the full-rebuild tables in 30.
-- MAGIC
-- MAGIC Requirements: run on a **serverless SQL warehouse or as a SQL task** in the job
-- MAGIC (MVs are created/refreshed through DBSQL). Each MV also carries its own SCHEDULE
-- MAGIC as a safety net, so it stays fresh even if the job doesn't run.

-- COMMAND ----------

USE CATALOG ${catalog};

-- COMMAND ----------

CREATE MATERIALIZED VIEW IF NOT EXISTS gold.mv_sales_last_30d
SCHEDULE EVERY 4 HOURS
COMMENT 'Rolling 30-day sales by category and channel - powers the exec dashboard'
AS
SELECT
  CAST(o.order_ts AS DATE)   AS sales_date,
  p.category,
  o.channel,
  COUNT(DISTINCT o.order_id) AS orders,
  SUM(oi.quantity)           AS units,
  SUM(oi.line_total)         AS revenue
FROM silver.orders o
JOIN silver.order_items oi ON o.order_id = oi.order_id
JOIN (SELECT * FROM silver.products WHERE __is_current) p
  ON oi.product_id = p.product_id
WHERE o.order_status = 'COMPLETED'
  AND o.order_ts >= CURRENT_DATE() - INTERVAL 30 DAYS
GROUP BY ALL;

-- COMMAND ----------

CREATE MATERIALIZED VIEW IF NOT EXISTS gold.mv_top_products
SCHEDULE EVERY 12 HOURS
COMMENT 'Product leaderboard: revenue, units, return rate'
AS
WITH sales AS (
  SELECT oi.product_id,
         SUM(oi.quantity)   AS units_sold,
         SUM(oi.line_total) AS revenue
  FROM silver.order_items oi
  JOIN silver.orders o ON oi.order_id = o.order_id
  WHERE o.order_status = 'COMPLETED'
  GROUP BY oi.product_id
),
rets AS (
  SELECT product_id, SUM(qty_returned) AS units_returned
  FROM silver.returns
  WHERE return_status = 'APPROVED'
  GROUP BY product_id
)
SELECT
  p.product_id, p.product_name, p.category, p.brand,
  COALESCE(s.units_sold, 0)     AS units_sold,
  COALESCE(s.revenue, 0)        AS revenue,
  COALESCE(r.units_returned, 0) AS units_returned,
  ROUND(100.0 * COALESCE(r.units_returned, 0)
        / NULLIF(s.units_sold, 0), 2) AS return_rate_pct
FROM (SELECT * FROM silver.products WHERE __is_current) p
LEFT JOIN sales s ON p.product_id = s.product_id
LEFT JOIN rets  r ON p.product_id = r.product_id;

-- COMMAND ----------

CREATE MATERIALIZED VIEW IF NOT EXISTS gold.mv_payment_health
SCHEDULE EVERY 1 DAY
COMMENT 'Daily payment success/decline/refund rates by method'
AS
SELECT
  CAST(payment_ts AS DATE) AS payment_date,
  payment_method,
  COUNT(*)                                                        AS attempts,
  SUM(CASE WHEN payment_status = 'SETTLED'  THEN 1 ELSE 0 END)    AS settled,
  SUM(CASE WHEN payment_status = 'DECLINED' THEN 1 ELSE 0 END)    AS declined,
  SUM(CASE WHEN payment_status = 'REFUNDED' THEN 1 ELSE 0 END)    AS refunded,
  ROUND(SUM(CASE WHEN payment_status = 'SETTLED' THEN amount ELSE 0 END), 2) AS settled_amount
FROM silver.payments
GROUP BY ALL;

-- COMMAND ----------

-- Refresh explicitly when this notebook runs as the final job task,
-- so the MVs are current the moment the pipeline finishes.
REFRESH MATERIALIZED VIEW gold.mv_sales_last_30d;
REFRESH MATERIALIZED VIEW gold.mv_top_products;
REFRESH MATERIALIZED VIEW gold.mv_payment_health;
