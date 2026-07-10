# 05 Spark Batch

## Purpose

Spark is the distributed batch transformation engine for the lakehouse path. It reads Bronze data from MinIO, standardizes and deduplicates it into Silver Iceberg tables, rebuilds Gold business tables, validates the results, and exports evidence.

Spark owns the reconciled batch path:

```text
MinIO Bronze
  -> Spark raw temp views
  -> Iceberg Silver stg_* tables
  -> Iceberg Gold dimensions, facts, OBT, aggregates, features
  -> Trino SQL serving
  -> DuckDB Executive Mart export
```

## Why Spark Is Needed

dbt-DuckDB is excellent for local reproducibility, but the platform also demonstrates a distributed batch implementation. Spark solves these problems:

| Pain point | Spark responsibility |
| --- | --- |
| Raw data spans many datasets and events | Reads multiple Bronze Parquet and JSON paths from MinIO. |
| Deduplication and standardization need repeatable logic | Applies stable-key deduplication and typed transformations. |
| Gold tables need to be shared by services | Writes Iceberg tables visible through Hive Metastore and Trino. |
| Batch truth must reconcile streaming metrics | Builds `agg_hourly_reconciled_kpi` as the canonical comparison target. |
| Evidence must prove the run | Captures row counts, validations, Trino smoke results, and parity reports. |

Spark Gold through Trino is canonical for official historical reporting.

## Services

Start lakehouse first, then batch:

```powershell
docker compose --profile lakehouse up -d
docker compose --profile batch up -d
```

| Service | Local URL | Role |
| --- | --- | --- |
| Spark master | `http://localhost:8085` | Cluster coordinator and UI. |
| Spark worker | internal | Executes the PySpark job. |
| Spark History Server | `http://localhost:18080` | Shows completed applications from Spark event logs. |
| Hive Metastore | `thrift://hive-metastore:9083` | Catalog metadata for Iceberg tables. |
| MinIO | `http://minio:9000` internally | Bronze/Silver/Gold/checkpoint storage. |
| Trino | `http://localhost:8080` | SQL smoke checks and executive mart export source. |

The Spark image is defined in [infra/spark/Dockerfile](../infra/spark/Dockerfile). The job entrypoint is [scripts/spark/run_batch.py](../scripts/spark/run_batch.py).

## Batch Interface

Run one logical hourly window:

```powershell
uv run python scripts/spark/run_batch.py --start-ts 2026-06-01T00:00:00Z --end-ts 2026-06-01T01:00:00Z --mode hourly
```

Run a backfill range:

```powershell
uv run python scripts/spark/run_batch.py --start-ts 2026-06-01T00:00:00Z --end-ts 2026-06-03T00:00:00Z --mode backfill
```

The interface uses half-open UTC windows: `[start_ts, end_ts)`. `hourly` requires exactly one hour. `backfill` accepts any positive UTC range.

## Implementation

The engine is **PySpark with Spark DataFrames and Spark SQL**. The job uses
`pyspark.sql.SparkSession`, distributed `spark.read.parquet` and `spark.read.json`
readers, `Window` and `row_number` based dedupe, and Spark SQL DDL (`CREATE OR REPLACE
TABLE`, `MERGE INTO`) against the Iceberg catalog. Silver builders produce Spark
DataFrames; Gold builders are pure Spark SQL templates in
`src/vina_bim_shop/lakehouse/spark/sql.py:ordered_gold_queries`.

Spark reads Bronze paths:

- `s3a://bronze/batch/<dataset>/snapshot_date=<date>/*`
- `s3a://bronze/events/<topic>/ingest_date=<date>/*`
- `data/raw/bad_snapshots/bad_snapshots.jsonl` (read by `_read_optional_bad_snapshots`
  using the `VBS_RAW_ROOT` env var or its default)

It then:

1. Creates raw temp views for batch snapshots, Kafka event logs, and quarantine
   contracts (`raw_bad_events`, `raw_bad_snapshots`).
2. Deduplicates Silver tables by stable business keys and latest timestamp using
   `Window.partitionBy(...).orderBy(...desc()).row_number()`.
3. Extracts event fields from JSON payloads into typed Silver columns with
   `get_json_object`, preserving `schema_version` and tolerating missing fields.
4. Writes Silver Iceberg tables with `MERGE INTO` for idempotent updates and
   `CREATE TABLE IF NOT EXISTS` for first-run bootstrap.
5. Rebuilds Gold Iceberg tables with `CREATE OR REPLACE TABLE` from Spark SQL.
6. Runs PySpark validations and Great Expectations validations.
7. Runs Trino smoke queries over `iceberg.gold`.
8. Runs dbt-DuckDB parity checks.
9. Exports the DuckDB Executive Mart from Trino Gold.
10. Captures Spark master and History Server evidence.

Important table families:

| Family | Examples | Write behavior |
| --- | --- | --- |
| Silver snapshots | `stg_orders`, `stg_order_items`, `stg_payments`, `stg_shipments` | Merge by stable source keys. |
| Silver events | `stg_commerce_events`, `stg_catalog_events`, `stg_fulfillment_events`, `stg_ops_events` | Merge by `event_id` and partition by event date. |
| Silver quarantine | `stg_bad_snapshots` | Merge by `bad_record_id`; partitions by `days(ingest_ts)`. |
| Gold dimensions | `dim_customer`, `dim_product`, `dim_category`, `dim_promotion` | Rebuilt from current Silver state. |
| Gold facts | `fact_order`, `fact_order_item`, `fact_payment_attempt`, `fact_shipment` | Rebuilt with reconciled business formulas. |
| Gold serving | `obt_order_performance`, `agg_hourly_reconciled_kpi`, feature tables | Rebuilt for BI, reconciliation, and local feature surfaces. |

## Data Challenge Handling

The map from generator challenges to Spark code paths is documented in
[11 Solving Data Challenges](11_solving_data_challenges.md). Highlights:

- **Dedup**: `Window.partitionBy(<stable key>).orderBy(created_ts.desc()).row_number()`
  in `src/vina_bim_shop/lakehouse/spark/job.py:_dedupe_latest`.
- **Nullable dimensions**: `coalesce(shipping_method, 'unknown')` in
  `sql.py:dim_shipping_method` and `sql.py:fact_shipment`; `coalesce(p.brand, 'unknown')`
  in `sql.py:dim_product` so the product dimension is never null.
- **Schema evolution**: every `stg_*_events` query preserves `schema_version` as a typed
  column; JSON paths are extracted with `get_json_object` so missing keys produce null.
- **Quarantine**: `_read_optional_dead_letter_events` and `_read_optional_bad_snapshots`
  register `raw_bad_events` and `raw_bad_snapshots`; `stg_bad_snapshots` is a real Silver
  table keyed by `bad_record_id`.
- **Skew**: the canonical Spark batch path does not use salting. A separate controlled
  coursework-scale experiment applies deterministic salting only to the two configured
  hot cities and proves exact aggregate equivalence without changing production semantics.
- **Operational signals**: `stg_ops_events` is a Silver fact that the platform uses for
  audit and DataHub lineage.

## Controlled Baseline And Optimization Evidence

The standalone optimization experiment is intentionally outside the canonical
`run_batch_pipeline` path: it does not read Bronze, write Silver/Gold, or modify Iceberg
tables. It derives 150,000 deterministic rows from the `coursework` generator profile,
disables AQE, and submits one Spark application per variant. The optimized skew variant
uses `pmod(xxhash64(customer_id), 16)` only for `Ho Chi Minh City` and `Ha Noi`; all other
cities use salt bucket `0`. Exact city totals and all four exact distinct counts are the
correctness gate. Elapsed time and partition balance are observed evidence, not a
guaranteed speedup.

| Rubric row | Evidence |
| --- | --- |
| 15 - baseline and UI proof | [optimization report](../evidence/05_spark_batch/optimization/optimization_report.md), [skew baseline](../evidence/05_spark_batch/screenshots/spark_skew_baseline_history.png), [cardinality baseline](../evidence/05_spark_batch/screenshots/spark_high_cardinality_baseline_history.png) |
| 16 - skew | [skew equivalence](../evidence/05_spark_batch/optimization/skew_equivalence.json), [skew optimized](../evidence/05_spark_batch/screenshots/spark_skew_optimized_history.png) |
| 17 - high cardinality | [all-ID equivalence](../evidence/05_spark_batch/optimization/high_cardinality_equivalence.json), [cardinality optimized](../evidence/05_spark_batch/screenshots/spark_high_cardinality_optimized_history.png) |
| 18 - schema evolution | `schema_version` preservation and `get_json_object` nullable extraction in `job.py`, plus [PySpark validation](../evidence/05_spark_batch/pyspark_validation_report.json) |
| 19 - duplicate/quarantine | `_dedupe_latest`, `raw_bad_events`, and `raw_bad_snapshots`, plus [GX validation](../evidence/05_spark_batch/gx/validation_results.json) |
| 20 - pipeline integration | Airflow task `hourly_batch_lakehouse.run_hourly_batch_window`, which invokes the unchanged canonical `run_batch_pipeline` |

## Data Types And Partitioning

Spark maps raw values into typed Silver/Gold columns so Trino, DuckDB, and DataHub see stable schemas.

| Choice | Example | Rationale |
| --- | --- | --- |
| Timestamp event columns | `event_timestamp`, `created_ts`, `order_timestamp`, `payment_timestamp` | Supports windows, freshness, deduplication, and point-in-time features. |
| String natural IDs | `order_id`, `customer_id`, `product_id` | Business identifiers are not numeric measures. |
| Long surrogate keys | `order_key`, `customer_key`, `product_key` | Stable dimensional joins. |
| Double financial measures | `official_paid_revenue`, `gross_merchandise_value`, `estimated_margin` | Compatible across Spark, Trino, DuckDB, and Pinot reconciliation outputs. |
| Decimal rates | `category_cost_rate` | Fixed-precision category cost percentage. |
| Boolean flags | `is_paid_order`, `is_payment_success`, `is_delivery_delayed` | Clear filterable business states. |

Selected Spark/Iceberg partitioning:

| Table group | Partitioning |
| --- | --- |
| Event Silver tables | `days(event_timestamp)` |
| Inventory Silver | `days(snapshot_ts)` |
| Order facts | `order_date_key` |
| Payment facts | `payment_date_key` |
| Shipment facts | `shipment_created_date_key` |
| Hourly KPI aggregate | `days(metric_hour)` |
| Streaming feature table | `days(event_timestamp)` |

## Service Interactions

| Service | Relationship |
| --- | --- |
| MinIO | Provides Bronze inputs and stores Silver/Gold Iceberg files. |
| Hive Metastore | Registers Spark-written Iceberg table metadata. |
| Trino | Queries Spark Gold and provides source rows for DuckDB Executive Mart export. |
| dbt-DuckDB | Rebuilds the same Gold logic locally for parity evidence. |
| Great Expectations | Validates Bronze and Gold quality expectations during batch evidence runs. |
| Airflow | Triggers hourly batch runs and collects run artifacts. |
| Pinot | Compared against `agg_hourly_reconciled_kpi`; Pinot remains provisional. |
| DataHub | Receives Spark Silver/Gold lineage metadata and Trino table metadata. |

## Evidence

Spark batch evidence is written under [evidence/05_spark_batch](../evidence/05_spark_batch/).

Expected artifacts include:

- [spark_master_status.json](../evidence/05_spark_batch/spark_master_status.json)
- [spark_history_applications.json](../evidence/05_spark_batch/spark_history_applications.json)
- [spark_job_manifest.json](../evidence/05_spark_batch/spark_job_manifest.json)
- [spark_table_row_counts.json](../evidence/05_spark_batch/spark_table_row_counts.json)
- [pyspark_validation_report.json](../evidence/05_spark_batch/pyspark_validation_report.json)
- [gx/validation_results.json](../evidence/05_spark_batch/gx/validation_results.json)
- [trino_gold_smoke_results.json](../evidence/05_spark_batch/trino_gold_smoke_results.json)
- [dbt_parity_report.md](../evidence/05_spark_batch/dbt_parity_report.md)
- [optimization/run_manifest.json](../evidence/05_spark_batch/optimization/run_manifest.json)
- [optimization/optimization_report.md](../evidence/05_spark_batch/optimization/optimization_report.md)
- [optimization/skew_equivalence.json](../evidence/05_spark_batch/optimization/skew_equivalence.json)
- [optimization/high_cardinality_equivalence.json](../evidence/05_spark_batch/optimization/high_cardinality_equivalence.json)
- `executive_mart_export_manifest.json`
- `executive_mart_export_report.md`

## Limitations

- The local Spark cluster is intentionally small and profile-based.
- Gold tables are rebuilt for the coursework implementation rather than optimized with every production incremental pattern.
- Airflow orchestrates batch runs, but Spark itself owns the transformation logic.
- dbt-DuckDB parity supports confidence and local inspection; Spark/Iceberg/Trino remains canonical for the distributed platform.
- The canonical Spark batch path does not use salting. The isolated optimization evidence
  is a coursework experiment and does not alter canonical table semantics.
- `stg_bad_snapshots` reads from the local raw root (via `VBS_RAW_ROOT`) because the
  standard batch upload excludes `bad_snapshots`. The dbt-DuckDB path is the canonical
  local-parity quarantine; the Spark path is the lakehouse quarantine.

## Convenience Make Targets

The root [Makefile](../Makefile) wraps the most common commands for this deliverable under a `make + verb` convention. It delegates to the official scripts in [scripts/](../scripts/) and to `scripts/ctl.py` for Docker Compose lifecycle; compose targets expand documented profile bundles and prebuild shared images where needed.

| Target | What it does |
| --- | --- |
| `make up-lakehouse` | Start the lakehouse profile (Spark reads Bronze from MinIO and writes Silver/Gold Iceberg). |
| `make down-lakehouse` | Stop the lakehouse profile and remove its volumes. |
| `make up-batch` | Start the batch profile (Spark master, worker, history server). |
| `make down-batch` | Stop the batch profile and remove its volumes. |
| `make up-ingestion` | Start the ingestion profile (needed for Kafka Connect Bronze landing). |
| `make down-ingestion` | Stop the ingestion profile and remove its volumes. |
| `make build-dbt` | Run `dbt build` against the local DuckDB parity oracle referenced in the parity report. |
