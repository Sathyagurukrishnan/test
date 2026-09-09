# Retail Medallion Pipeline (Databricks, metadata-driven)

A complete medallion architecture demo: 10 CSV sources → Auto Loader bronze → conformed
silver (SCD1/SCD2, quality rules, quarantine) → gold aggregates + materialized views,
all driven by a metadata table, with full audit logging, error logging, retries with
exponential backoff, and a single Databricks Job wiring it together.

```
 Volume (10 CSVs)          BRONZE                 SILVER                    GOLD
┌──────────────────┐   ┌───────────────┐   ┌──────────────────┐   ┌─────────────────────────┐
│ /Volumes/.../     │   │ Auto Loader   │   │ try_cast conform │   │ fct_daily_sales         │
│  customers/       │──▶│ availableNow  │──▶│ dedupe on PK     │──▶│ dim_customer_360        │
│  orders/  ...     │   │ schema rescue │   │ DQ rules ─▶ quarantine │ fct_carrier_performance │
└──────────────────┘   │ append + CDF  │   │ MERGE SCD1/SCD2  │   │ fct_inventory_position  │
                       └───────────────┘   │ liquid clustering│   │ + 3 materialized views  │
                                           └──────────────────┘   └─────────────────────────┘
        ops.pipeline_metadata drives every step · ops.pipeline_audit_log / error_log / dq_quarantine record every step
```

## What's in the box

| Path | Purpose |
|---|---|
| `source_data/` | 10 CSV files, ~34,600 records, with deliberate dirty rows (dup PK, bad emails, negative qty/price, orphan FKs) so quality rules have something to catch |
| `setup/00_setup_catalog.py` | Creates catalog `retail_lakehouse`, schemas (landing/bronze/silver/gold/ops), volumes, per-entity folders |
| `setup/01_create_metadata_table.py` | `ops.pipeline_metadata` DDL + MERGE of 10 config rows (paths, PKs, cast maps, cluster keys, SCD type, quality rules, priority) |
| `setup/02_create_audit_tables.py` | `ops.pipeline_audit_log`, `ops.pipeline_error_log`, `ops.dq_quarantine` |
| `utils/pipeline_utils.py` | Audit writers, error logger, retry-with-backoff decorator, metadata-driven quality rule engine |
| `notebooks/10_bronze_autoloader.py` | Auto Loader (cloudFiles) ingestion loop over all active entities |
| `notebooks/20_silver_conformed.py` | Streaming bronze→silver: conform, dedupe, DQ, SCD1/SCD2 MERGE |
| `notebooks/30_gold_aggregates.py` | 4 gold tables, `CLUSTER BY AUTO` |
| `notebooks/31_gold_materialized_views.sql` | 3 materialized views with refresh schedules (SQL task) |
| `notebooks/40_health_check.py` | Final task: fails the job on any upstream failure or abnormal quarantine rate |
| `notebooks/50_cdf_explorer.py` | Learning notebook (not in the job): reads the Change Data Feed via SQL `table_changes()`, batch, and streaming, and shows the bookmark pattern for incremental consumers |
| `jobs/medallion_job.json` | Databricks Job: metadata → bronze → silver → (gold tables ∥ MVs) → health check, with per-task retries and failure emails |

## Setup (one time)

1. **Import the code.** Upload the `setup/`, `notebooks/` and `utils/` folders into your
   workspace at `/Workspace/medallion_pipeline/` (Workspace → Import, or Repos/Git folder).
   Keep the folder structure — notebooks use `%run ../utils/pipeline_utils`.
2. **Run the setup notebooks in order** on any cluster with Unity Catalog:
   `00_setup_catalog` → `01_create_metadata_table` → `02_create_audit_tables`.
3. **Upload the source files.** In Catalog Explorer, open
   `retail_lakehouse.landing.source_files` and upload each CSV **into its matching
   folder** (`customers.csv` → `customers/`, etc.). Auto Loader watches folders, so
   later you just drop new files into the same folders and only those get picked up.
4. **Create the job.** Workflows → Jobs → Create → switch to the JSON editor (kebab menu
   → "Edit as JSON") and paste `jobs/medallion_job.json`. Replace:
   - `your.email@company.com` with your address
   - `REPLACE_WITH_SERVERLESS_WAREHOUSE_ID` with a serverless SQL warehouse ID
     (SQL Warehouses → your warehouse → copy the ID from the URL/overview)
   - notebook paths if you imported somewhere other than `/Workspace/medallion_pipeline/`
   The job has no cluster spec on the notebook tasks, so it runs on **serverless job
   compute** by default; attach a job cluster instead if serverless isn't enabled.
5. **Run the job.** First run ingests everything; subsequent runs are incremental.
   Unpause the 5am AEST schedule when you're happy.

## Verify a run

```sql
USE CATALOG retail_lakehouse;

-- run summary
SELECT layer, entity_name, status, records_read, records_written,
       records_quarantined, retry_count, duration_sec
FROM ops.pipeline_audit_log ORDER BY start_ts DESC;

-- what got quarantined and why
SELECT entity_name, failed_rules, record_json FROM ops.dq_quarantine;

-- stack traces for anything that failed
SELECT * FROM ops.pipeline_error_log ORDER BY error_ts DESC;

-- the good stuff
SELECT * FROM gold.dim_customer_360 ORDER BY lifetime_revenue DESC LIMIT 20;
SELECT * FROM gold.mv_top_products ORDER BY revenue DESC LIMIT 20;
```

Expected quarantines from the seeded dirty data: ~6 products (negative price),
~30 order_items (negative qty), plus WARN counts for bad emails and orphan customer IDs.
The duplicate customer row is removed by dedupe, and customers/products build SCD2
history (`__is_current`, `__start_ts`, `__end_ts`).

## To onboard a new source

Insert one row into `ops.pipeline_metadata` (path, format, PKs, casts, rules), create
its folder in the volume, drop files in. No code changes.

## Recent Databricks features used

- **Auto Loader** with `availableNow` trigger (batch cost, streaming exactly-once file
  tracking), schema inference/evolution with `rescue` mode and `_rescued_data`
- **Liquid clustering** everywhere — explicit keys on bronze/silver from metadata,
  `CLUSTER BY AUTO` on gold so Databricks picks and evolves keys from query patterns
- **Liquid clustering vs partitioning, metadata-driven** — each entity's silver layout
  comes from metadata: `partition_keys` set → classic `PARTITIONED BY`
  (`inventory_snapshots` is the worked example, partitioned by `snapshot_date`);
  otherwise `CLUSTER BY` on `cluster_keys`, or `CLUSTER BY AUTO` when empty. A Delta
  table can only have one layout, and liquid clustering is the modern default — the
  partitioned table is there to learn the difference side by side
- **Predictive optimization** (see note in `00_setup_catalog`) — automatic
  OPTIMIZE/VACUUM/cluster maintenance, replaces hand-scheduled maintenance jobs
- **Unity Catalog volumes** for file landing and checkpoint/schema state (no DBFS)
- **Change Data Feed** — switched per entity via the `enable_cdf` metadata flag,
  enables incremental MV refresh, and `50_cdf_explorer` teaches how to consume it
  (change types, pre/post images, bookmarks, streaming)
- **Deletion vectors** + `tuneFileSizesForRewrites` on merge-heavy silver tables
- **Materialized views** with incremental refresh + `SCHEDULE` clauses
- **Identity columns** for surrogate keys on the audit tables
- **Serverless job compute** and a fan-out/fan-in task DAG with `run_if: ALL_DONE`
  health gate

## Best practices baked in

- **Metadata-driven**: one generic notebook per layer, config in a table, new sources
  are an INSERT not a PR
- **Bronze is immutable**: append-only, strings-as-landed, lineage columns
  (`_ingest_ts`, `_source_file`, `_run_id`); all casting happens in silver with
  `try_cast` so bad values become rule violations, not run failures
- **Quarantine, don't drop**: failing rows are kept as JSON with the rules they failed,
  so they can be inspected and replayed
- **Two retry layers**: in-code exponential backoff with jitter (transient storage/
  MERGE conflicts) + job-level `max_retries` (infra-level failures); every attempt is
  logged with a stack trace
- **Isolate failures**: each layer attempts every entity before raising, so one bad
  source doesn't block the other nine; the health-check task turns partial failures
  into a red run
- **Exactly-once end to end**: Auto Loader file tracking into bronze, Delta streaming
  checkpoints into silver, idempotent MERGEs keyed on PKs
- **Ops observability as tables**: audit, errors and quarantine are queryable Delta
  tables — point a dashboard or alert at them

## Ideas to extend

- Wrap the whole thing as a **Databricks Asset Bundle** (`databricks.yml`) for
  dev/prod promotion via CI/CD
- Swap the silver notebook for a **Lakeflow Declarative Pipeline** with
  `APPLY CHANGES` + expectations for a declarative flavour of the same design
- Add a **Lakehouse Monitoring** profile on silver tables for drift/DQ dashboards
- Add SQL Alerts on `ops.pipeline_audit_log` (failed runs, quarantine spikes)
