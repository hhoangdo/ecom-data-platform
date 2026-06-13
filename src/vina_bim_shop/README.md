# `src/vina_bim_shop`

This folder contains the importable Python package for the coursework platform. It is the reusable implementation layer behind the CLI scripts, Airflow DAG adapters, tests, and local runtime helpers.

## What Is Here

| Path | Purpose |
| --- | --- |
| `generators/` | Synthetic e-commerce snapshot and Kafka-shaped event generation. |
| `kafka/` | Kafka topic, schema registry, producer/consumer, Bronze sink, cleanup, and evidence helpers. |
| `lakehouse/` | MinIO/Trino/lakehouse helpers plus Spark batch implementation modules. |
| `flink/` | Flink job code, runtime helpers, smoke checks, and verification logic. |
| `pinot/` | Pinot bootstrap, query example, and evidence refresh helpers. |
| `orchestration/` | Python functions used by Airflow DAG wrappers and local evidence workflows. |
| `datahub_lineage/` | DataHub metadata, lineage, and assertion emitters. |
| `quality/` | Shared quality report and policy helpers. |

## How To Read It

Start with the package that matches the platform stage you are reviewing. For example, read `generators/` with the data generation deliverable, `kafka/` with ingestion, `lakehouse/spark/` with batch, and `flink/` with streaming.

## How It Differs From Similar Folders

| Folder | Role |
| --- | --- |
| `src/vina_bim_shop/` | Importable Python application code and shared runtime logic. |
| `scripts/` | User-facing command-line entry points that call into this package. See `../../scripts/README.md` for the official script inventory. |
| `infra/` | Service assets such as Dockerfiles, dbt models, Kafka schemas, Airflow DAG files, and Pinot/DataHub configuration. |

Domain writeups live in `../../architecture/domain/`, not in this package. The committed taxonomy input lives in `../../data/reference/taxonomy/taxonomy_snapshot.yaml`.
