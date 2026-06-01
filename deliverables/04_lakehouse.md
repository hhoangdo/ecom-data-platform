# ADR 02 Lakehouse Runbook

This runbook implements `architecture/decisions/2026-06-01-02-minio-hive-trino-lakehouse-plan.md` without changing later-session ownership.

## Services

Start the lakehouse profile:

```powershell
docker compose --profile lakehouse up -d
```

Local service URLs:

| Service | URL |
| --- | --- |
| MinIO S3 API | `http://localhost:9000` |
| MinIO Console | `http://localhost:9001` |
| Shared Postgres | `localhost:5433` |
| Hive Metastore Thrift | `thrift://localhost:9083` |
| Trino UI and API | `http://localhost:8080` |

Local credentials are fixed development defaults in `.env.example`. They are not production secrets.

## Buckets

The one-shot `minio-init` service creates these buckets idempotently:

| Bucket | Purpose |
| --- | --- |
| `bronze` | Raw Parquet snapshots and raw Kafka JSON/JSONL replay logs. |
| `silver` | Future Spark-owned cleaned Iceberg tables. |
| `gold` | Future Spark-owned business-ready Iceberg tables. |
| `checkpoints` | Future Spark/Flink checkpoint and job state. |
| `evidence` | Coursework screenshots, health JSON, and exported reports. |

Bronze raw files remain unregistered by default. Spark may read Bronze by object path later, but Trino should not expose Bronze as normal analyst-facing tables.

## Bronze Raw Landing

Option A raw landing is now split by ownership:

- Kafka Connect lands source-topic event logs into MinIO Bronze.
- A small Python CLI uploads local batch snapshot exports into MinIO Bronze.
- Spark remains the future reader of Bronze object paths and the future writer of Silver/Gold.

Bronze raw object layout:

- `bronze/batch/<dataset>/snapshot_date=<date>/*`
- `bronze/events/<topic>/ingest_date=<date>/*`

Upload the current local raw batch snapshots:

```powershell
uv run python scripts/lakehouse/land_bronze_batch.py --raw-root data/raw --snapshot-date 2026-06-01 --minio-alias LOCAL
```

This command expects a MinIO client alias such as `LOCAL` to already be configured against the local MinIO API.

## Shared Postgres

The `lakehouse-postgres` service initializes isolated databases and users for:

| Database | Used by |
| --- | --- |
| `hive_metastore` | Hive Metastore in ADR 02. |
| `airflow` | Airflow in ADR 06. |
| `datahub` | DataHub in ADR 07. |

ADR 02 uses only `hive_metastore`; the others exist so later services reuse this shared Postgres instead of adding separate quickstart databases.

## Trino Catalog

Trino exposes only the `iceberg` catalog configured in `infra/lakehouse/trino/catalog/iceberg.properties`.

The catalog uses:

- Hive Metastore: `thrift://hive-metastore:9083`
- MinIO internal endpoint: `http://minio:9000`
- Iceberg table format for future Silver/Gold tables

Trino may create a temporary smoke table to prove connectivity. Spark remains the normal owner for Silver/Gold writes in ADR 03.

## Smoke Tests

Run the Trino smoke SQL:

```powershell
uv run python scripts/lakehouse/smoke_sql.py
```

The smoke SQL verifies the catalog, schemas, and a read-only metadata-table query through `iceberg.information_schema.schemata`. It does not create Silver/Gold tables because Spark remains the normal Silver/Gold write owner in ADR 03.

Capture service evidence:

```powershell
uv run python scripts/lakehouse/capture_evidence.py
```

Evidence is written under `evidence/04_lakehouse/`.

Capture Bronze landing examples after uploads/connectors run:

```powershell
uv run python scripts/lakehouse/capture_bronze_evidence.py --evidence-root evidence/04_lakehouse --listing-path evidence/04_lakehouse/bronze_listing.txt
```

The Bronze landing artifact is `evidence/04_lakehouse/bronze_landing_examples.json` and should include example batch and event object keys.

## Required Screenshots

Capture these files under `evidence/04_lakehouse/screenshots/`:

- `minio_buckets.png`: MinIO Console showing `bronze`, `silver`, `gold`, `checkpoints`, and `evidence`.
- `trino_query_history.png`: Trino UI showing the smoke query history.
- `trino_sample_query.png`: Trino UI or terminal evidence showing the smoke query result.

## Reset

Stop lakehouse services:

```powershell
docker compose --profile lakehouse down
```

Stop services and remove lakehouse volumes:

```powershell
uv run python scripts/lakehouse/cleanup_lakehouse.py --volumes
```

Remove ADR 02 evidence as well:

```powershell
uv run python scripts/lakehouse/cleanup_lakehouse.py --volumes --clean-evidence
```

Lakehouse Docker volumes are operational state, not committed coursework evidence.

## Do Not Do

- Do not register raw Bronze files as normal analyst-facing tables.
- Do not let Trino become the normal Silver/Gold writer.
- Do not add Spark transformations in this session.
- Do not add DataHub ingestion in this session.
- Do not introduce separate Postgres services for Airflow or DataHub later.
