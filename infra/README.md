# `infra`

This folder contains platform and service assets for the local data platform. It is mostly configuration, container build context, service-specific SQL/model files, and orchestration assets rather than reusable Python package code.

## What Is Here

| Path | Purpose |
| --- | --- |
| `analytics/dbt/` | dbt-DuckDB project, profiles, macros, tests, and Bronze/Silver/Gold models. |
| `kafka/` | Kafka topic config, JSON Schemas, Kafka Connect Dockerfile, and S3 sink template. |
| `lakehouse/` | MinIO, Hive Metastore, Postgres, and Trino service configuration. |
| `spark/` | Spark image and container entrypoint assets. |
| `flink/` | Flink image and job submission/runtime shell helpers. |
| `pinot/` | Pinot table configs, schemas, and SQL examples. |
| `orchestration/` | Airflow Dockerfile, DAG wrappers, plugin code, and generated GX Data Docs seed files. |
| `governance/` | DataHub ingestion recipes and metadata fixtures. |

## How To Read It

Follow the platform stages in the root README: ingestion, lakehouse, batch, streaming, serving, orchestration, and governance. Each subfolder supports one of those runtime stages.

## How It Differs From Similar Folders

| Folder | Role |
| --- | --- |
| `infra/` | Runtime assets consumed by Docker Compose, dbt, Kafka Connect, Airflow, Pinot, Trino, and DataHub. |
| `src/vina_bim_shop/` | Importable Python implementation used by scripts, tests, and DAG adapters. |
| `scripts/` | Thin commands reviewers/operators run directly. See `../scripts/README.md` for which scripts are official. |

If a file configures a service or declares a model/schema used by a service, it usually belongs here. If it exposes a Python function imported by tests or scripts, it usually belongs under `src/vina_bim_shop/`.
