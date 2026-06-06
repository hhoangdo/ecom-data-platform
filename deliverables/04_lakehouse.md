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

## Limitations

- The local lakehouse is not secured for production use.
- Raw Bronze files are intentionally not the normal analyst-facing SQL surface.
- Spark owns Silver/Gold writes; Trino is used for query serving.
- Local reproducibility favors staged profiles over a single full-stack startup.
