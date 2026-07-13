# 04 Lakehouse

## Purpose

The lakehouse is the shared storage and SQL foundation for the platform. In this project, "lakehouse" means:

- MinIO for local object storage
- Hive Metastore for table catalog metadata
- Postgres as the metastore backing database
- Apache Iceberg for Silver and Gold table format
- Trino for SQL access over curated lakehouse tables

The lakehouse is responsible for preserving raw Bronze data, supporting Spark-written Silver/Gold Iceberg tables, and exposing canonical historical SQL through Trino.

## Why The Project Needs A Lakehouse

Kafka is excellent for event transport, but it is not a complete analytical storage layer. The platform also needs a place to keep replayable files, curated tables, checkpoints, and evidence.

| Pain point | Lakehouse solution |
| --- | --- |
| Raw files and event logs need durable storage | MinIO stores batch snapshots, Kafka replay logs, checkpoints, and evidence objects. |
| Batch and SQL tools need a shared table catalog | Hive Metastore records Iceberg table metadata for Spark and Trino. |
| Historical analytics need table semantics | Iceberg gives Silver/Gold tables schemas, snapshots, and partitioning. |
| Reviewers need SQL access | Trino exposes `iceberg.silver` and `iceberg.gold` without opening Spark internals. |
| Local evidence must be reproducible | MinIO paths and DuckDB exports make the platform inspectable on one machine. |

## Services

Start the lakehouse profile:

```powershell
docker compose --profile lakehouse up -d
```

| Service | Local URL | Responsibility |
| --- | --- | --- |
| MinIO S3 API | `http://localhost:9000` | Object storage for Bronze, Silver, Gold, checkpoints, and evidence. |
| MinIO Console | `http://localhost:9001` | Browser inspection of buckets and objects. |
| Shared Postgres | `localhost:5433` | Backing database for Hive Metastore, Airflow, and DataHub. |
| Hive Metastore | `thrift://localhost:9083` | Catalog metadata for Iceberg tables. |
| Trino | `http://localhost:8080` | SQL query surface for Iceberg tables. |

Local credentials are development defaults in [.env.example](../.env.example).

## Bucket Design

The `minio-init` service creates these buckets idempotently:

| Bucket | Written by | Read by | Purpose |
| --- | --- | --- | --- |
| `bronze` | Generator upload scripts, Kafka Connect | Spark, evidence scripts | Raw Parquet snapshots and raw Kafka replay logs. |
| `silver` | Spark | Spark, Trino | Standardized Iceberg tables. |
| `gold` | Spark | Trino, DataHub, DuckDB exporter | Business-ready Iceberg tables. |
| `checkpoints` | Spark, Flink | Spark History, Flink recovery, DataHub metadata | Job state and Spark event logs. |
| `evidence` | Flink audit sinks, evidence scripts | Reviewers, evidence capture scripts | Runtime proof artifacts and curated streaming audit outputs. |

## Bronze To Silver To Gold Lifecycle

```text
Generator Parquet snapshots
  -> data/raw/<dataset>/
  -> MinIO bronze/batch/<dataset>/snapshot_date=<date>/*
  -> Spark raw temp views
  -> Iceberg Silver stg_* tables
  -> Iceberg Gold dimensions/facts/serving tables
  -> Trino SQL and DuckDB exports

Kafka source topics
  -> Kafka Connect S3 sink
  -> MinIO bronze/events/<topic>/ingest_date=<date>/*
  -> Spark raw_kafka_* temp views
  -> Iceberg Silver event tables
  -> Gold aggregates/features and reconciliation evidence
```

### Bronze

Bronze stores source-fidelity data. It preserves raw columns, event envelopes, nested payloads, schema versions, and malformed-record wrappers.

| Bronze input | Format | Why this type |
| --- | --- | --- |
| Batch snapshots | Parquet | Columnar, compact, efficient for Spark/dbt scans, and typed enough for batch source-state exports. |
| Kafka replay logs | JSON/JSONL objects from Kafka Connect | Keeps event envelopes human-readable and replayable; preserves nested payloads and schema drift. |
| Bad snapshots and DLQ records | JSONL wrappers | Lets malformed examples be audited without breaking file readers. |

Bronze files are not registered as normal analyst-facing tables. They are raw inputs for Spark and quality inspection.

### Silver

Silver is where source data becomes standardized and typed.

| Silver behavior | Example |
| --- | --- |
| Deduplicate by stable key | `stg_order_items` dedupes intentional duplicate `order_item_id` payloads. |
| Cast timestamps | `event_timestamp`, `created_ts`, `snapshot_ts`, `payment_timestamp`, and shipment times become timestamp values. |
| Flatten envelopes | `stg_commerce_events` extracts session, customer, product, order, payment, category, device, source, and amount fields. |
| Preserve schema drift | `schema_version` remains available and optional newer fields stay nullable. |
| Partition event-heavy tables | Event tables are partitioned by `days(event_timestamp)` in Spark/Iceberg. |

Silver tables are stored as Iceberg tables in the distributed path and as dbt views in the local DuckDB path.

### Gold

Gold is business-ready and modeled for reporting.

| Gold family | Examples | Purpose |
| --- | --- | --- |
| Dimensions | `dim_customer`, `dim_seller`, `dim_product`, `dim_category`, `dim_date` | Reusable descriptive entities. |
| Facts | `fact_order`, `fact_order_item`, `fact_payment_attempt`, `fact_shipment`, `fact_inventory_snapshot` | Reconciled business events and measures. |
| Bridge | `bridge_product_category` | Many-to-many product taxonomy. |
| Serving tables | `obt_order_performance`, `agg_hourly_reconciled_kpi` | Executive BI and canonical KPI comparison. |
| Feature tables | `feat_customer_90d`, `feat_stream_60m`, `feat_customer_unified` | Local ML/AI preparation surfaces. |

Gold uses surrogate keys for joins, natural IDs for auditability, double/decimal numeric metrics for financial calculations, timestamps for time logic, and booleans for business flags.

## Data Type Choices Across Layers

| Layer | Type strategy | Rationale |
| --- | --- | --- |
| Bronze snapshots | Preserve source Parquet types and add ingestion metadata. | Minimizes early transformation and keeps source-state evidence intact. |
| Bronze events | Preserve JSON envelope fields and nested payloads. | Supports schema drift, replay, and event-contract inspection. |
| Silver | Cast to explicit timestamps, numeric fields, booleans, and strings. | Provides stable input for joins, windows, tests, and Gold formulas. |
| Gold | Enforce table contracts, keys, relationships, and business metric types. | Makes DBeaver ERDs, dbt tests, Trino SQL, and DuckDB exports consistent. |
| Pinot serving | Flink-derived realtime schema optimized for OLAP dimensions and measures. | Keeps live queries fast while leaving official truth in Gold. |

## Trino Catalog

Trino exposes the `iceberg` catalog configured in [infra/lakehouse/trino/catalog/iceberg.properties](../infra/lakehouse/trino/catalog/iceberg.properties).

The catalog uses:

- Hive Metastore: `thrift://hive-metastore:9083`
- MinIO internal endpoint: `http://minio:9000`
- Iceberg table format for curated Silver/Gold tables

Trino is the canonical SQL surface over Spark-written Gold. It does not normally write Silver/Gold tables in this project.

## Service Interactions

| Service | Relationship |
| --- | --- |
| Generator | Produces local raw snapshots that are uploaded to Bronze. |
| Kafka Connect | Lands source-topic event logs into `bronze/events/<topic>/...`. |
| Spark | Reads Bronze, writes Silver/Gold Iceberg tables, and stores Spark event logs/checkpoints. |
| Flink | Writes checkpoints to `checkpoints/flink` and JSONL audit outputs under the `evidence` bucket. |
| Trino | Queries Iceberg Silver/Gold tables through Hive Metastore. |
| DuckDB Executive Mart | Exports Trino Gold snapshots into `data/gold/vina_bim_shop_executive.duckdb`. |
| Airflow/GX | Orchestrates lakehouse batch runs and serves validation evidence. |
| DataHub | Ingests MinIO/S3 prefix metadata and Trino/Iceberg datasets. |

## Run And Evidence

Upload current local raw batch snapshots:

```powershell
uv run python scripts/lakehouse/land_bronze_batch.py --raw-root data/raw --snapshot-date 2026-06-01 --minio-alias LOCAL
```

Run Trino smoke SQL:

```powershell
uv run python scripts/lakehouse/smoke_sql.py
```

Capture evidence:

```powershell
uv run python scripts/lakehouse/capture_evidence.py
uv run python scripts/lakehouse/capture_bronze_evidence.py --evidence-root evidence/04_lakehouse --listing-path evidence/04_lakehouse/bronze_listing.txt
```

Key evidence files:

- [minio_health.json](../evidence/04_lakehouse/minio_health.json)
- [minio_buckets.json](../evidence/04_lakehouse/minio_buckets.json)
- [hive_metastore_health.txt](../evidence/04_lakehouse/hive_metastore_health.txt)
- [trino_catalogs.txt](../evidence/04_lakehouse/trino_catalogs.txt)
- [trino_schemas.txt](../evidence/04_lakehouse/trino_schemas.txt)
- [bronze_listing.txt](../evidence/04_lakehouse/bronze_listing.txt)
- [bronze_landing_examples.json](../evidence/04_lakehouse/bronze_landing_examples.json)
- [run_manifest.json](../evidence/04_lakehouse/run_manifest.json)

## Storage Optimization Evidence (Row 26)

This is a smoke-scale, local physical-layout experiment. It completed a genuine Iceberg rewrite without changing schemas, partition specifications, persistent table properties, or the compaction policy.

The local smoke source was regenerated (800 customers, 1,800 orders, and 6,891 raw order-item rows), while persisted `2026-04-26` and `2026-05-01` Bronze inputs supplied the canonical backfill. The disabled-by-default `--layout-profile compaction-evidence` profile rebuilt only the two Gold fact targets from their existing queries. It deterministically split each fact's rows by natural-key hash into two writer buckets, then restored the temporary `spark.sql.iceberg.distribution-mode=none` session setting. Spark and GX validation both passed.

The optimization CLI evaluated only `silver.stg_orders`, `silver.stg_order_items`, `gold.fact_order`, and `gold.fact_order_item`, with a 128 MiB target and `min-input-files=2`. Its durable pre-rewrite snapshot showed six date partitions with two Gold data files. It skipped the one-file Silver targets and genuinely compacted both Gold facts.

| Table | Files before → after | Rows | Logical invariant | Procedure outcome |
| --- | ---: | ---: | --- | --- |
| `silver.stg_orders` | 1 → 1 | 1,800 | row count and aggregate hash unchanged | skipped: fewer than 2 input files |
| `silver.stg_order_items` | 1 → 1 | 6,756 | row count and aggregate hash unchanged | skipped: fewer than 2 input files |
| `gold.fact_order` | 13 → 7 | 1,800 | row count and aggregate hash unchanged | 12 data files rewritten |
| `gold.fact_order_item` | 13 → 7 | 6,756 | row count and aggregate hash unchanged | 12 data files rewritten |

The guarded CLI captured two warmups and seven measured Trino executions per table before and after compaction. Result hashes are equal across both phases. The observed client medians changed from 158.547 to 128.030 ms for `fact_order` and from 158.560 to 137.837 ms for `fact_order_item`; the unchanged Silver files also had timing variation, so these local samples are not a general performance guarantee.

Code and evidence:

- [canonical opt-in writer profile](../src/vina_bim_shop/lakehouse/spark/job.py), [optimization CLI](../scripts/lakehouse/optimize_iceberg.py), and [Iceberg maintenance helpers](../src/vina_bim_shop/lakehouse/spark/maintenance.py)
- [seed backfill manifest](../evidence/04_lakehouse/optimization/seed_batch/spark_job_manifest.json) and [layout manifest](../evidence/04_lakehouse/optimization/seed_batch/compaction_layout_manifest.json)
- [before file/invariant snapshot](../evidence/04_lakehouse/optimization/before_file_stats.csv) and [after file/invariant snapshot](../evidence/04_lakehouse/optimization/after_file_stats.csv)
- [compaction result](../evidence/04_lakehouse/optimization/compaction_results.json), [raw Trino samples](../evidence/04_lakehouse/optimization/query_benchmark.json), and [report](../evidence/04_lakehouse/optimization/report.md)
- [archived generic-writer-limit attempt](../evidence/04_lakehouse/optimization/attempts/2026-07-11-generic-writer-limit-blocked/report.md)

## Limitations

- The local lakehouse is not secured for production use.
- Raw Bronze files are intentionally not the normal analyst-facing SQL surface.
- Spark owns Silver/Gold writes; Trino is used for query serving.
- Local reproducibility favors staged profiles over a single full-stack startup.
- The Row-26 source is smoke-scale and local. The two-bucket profile is disabled by default and exists only to create a controlled compaction candidate; it is not a production writer-tuning recommendation.

## Convenience Make Targets

The root [Makefile](../Makefile) wraps the most common commands for this deliverable under a `make + verb` convention. It delegates to the official scripts in [scripts/](../scripts/) and to `scripts/ctl.py` for Docker Compose lifecycle; compose targets expand documented profile bundles and prebuild shared images where needed.

| Target | What it does |
| --- | --- |
| `make up-lakehouse` | Start the lakehouse profile (MinIO, Hive Metastore, Trino, shared Postgres). |
| `make down-lakehouse` | Stop the lakehouse profile and remove its volumes. |
