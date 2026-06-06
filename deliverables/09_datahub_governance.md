# DataHub Governance

## Purpose

DataHub is the governance and metadata catalog layer for the Vina Bim Shop platform. It records datasets, tags, lineage, quality assertions, and representative entity verification across the implemented local stack.

In this coursework, DataHub is not used as a decorative UI. Its responsibility is to prove that the platform can emit metadata about the same assets that the pipelines produce: Kafka topics, MinIO/S3 prefixes, Trino/Iceberg tables, dbt models, Spark lineage, Flink lineage, and Great Expectations assertions.

## Why DataHub Is Needed

As the platform grows from generator outputs into ingestion, lakehouse, streaming, serving, and orchestration services, it becomes difficult to answer governance questions from file paths alone.

| Governance question | DataHub contribution |
| --- | --- |
| Which datasets exist across the platform? | Dataset entities are emitted for Kafka, S3/MinIO, Trino, and dbt assets. |
| Which layer does a dataset belong to? | Tags such as `bronze`, `silver`, `gold`, `official`, `provisional`, and `quality_gate` classify assets. |
| How did a Gold table or realtime topic get produced? | Spark and Flink lineage entities connect source, processing, and output assets. |
| Were quality checks represented as metadata? | GX assertions are emitted as governance evidence. |
| Can evidence be verified without trusting the UI search index? | Representative entity and tag lookups are checked directly through GMS/GraphQL. |

The governance layer helps the instructor inspect the platform as a system rather than as isolated Docker services.

## Implemented Scope

The governance profile runs the local DataHub services and OpenSearch:

| Service | Responsibility |
| --- | --- |
| `datahub-gms` | DataHub metadata service and API endpoint. |
| `datahub-frontend` | Local web UI for catalog inspection. |
| `datahub-actions` | Action service used by the DataHub stack. |
| `datahub-opensearch` | Local search index dependency. |
| `datahub-system-update` | Initializes or updates DataHub system metadata. |

Metadata recipes and supporting files live under `infra/governance/recipes/`:

| Recipe or file | Role |
| --- | --- |
| `kafka_topics.yml` | Emits Kafka topic datasets. |
| `minio_storage.yml` | Emits object-storage prefix datasets. |
| `trino_tables.yml` | Emits Trino/Iceberg table datasets. |
| `dbt_legacy.yml` | Emits dbt model metadata. |
| `minio_container_metadata.json` | Supports MinIO/S3 container metadata capture. |

Custom lineage and assertion emitters live under `src/vina_bim_shop/datahub_lineage/`.

## Service Interactions

| Platform area | Governance relationship |
| --- | --- |
| Data generation | Source datasets and issue evidence provide the starting point for metadata interpretation. |
| Kafka ingestion | Kafka topics are emitted as source and derived topic datasets. |
| Lakehouse | MinIO/S3 prefixes and Trino/Iceberg tables are represented as catalog assets. |
| Spark batch | Spark lineage connects Bronze/Silver/Gold transformation relationships. |
| Flink streaming | Flink lineage connects raw Kafka topics to derived realtime topics. |
| Pinot serving | Pinot is represented through the realtime serving lineage around derived topics and query evidence. |
| Airflow/GX | DataHub ingestion can be triggered by Airflow, and GX assertions are emitted as quality metadata. |
| dbt-DuckDB | dbt model metadata provides an independent local modeling view for the parity path. |

## Evidence Counts

The latest successful governance evidence records:

| Asset type | Count |
| --- | ---: |
| Kafka topic datasets | 8 |
| MinIO/S3 prefix datasets | 4 |
| Trino datasets | 45 |
| dbt datasets | 52 |
| Spark lineage entities | 20 |
| Flink lineage entities | 3 |
| GX assertions | 8 |

Representative datasets were verified directly through DataHub GMS/GraphQL:

| Platform | Representative entity |
| --- | --- |
| Iceberg/Trino | `vina_bim_shop.fact_order` |
| Kafka | `commerce_events` |
| Pinot | `realtime_commerce_metrics_1m` |
| S3/MinIO | `checkpoints.spark-events` |

Representative tags verified in the evidence include `bronze`, `silver`, `gold`, `official`, `provisional`, and `quality_gate`.

## Runtime And Evidence

Start governance after the relevant platform assets exist:

```powershell
docker compose --profile governance up -d
```

If using Airflow as the control plane, trigger:

```powershell
docker compose exec airflow-webserver airflow dags trigger datahub_ingestion
```

Standalone evidence capture is implemented through:

```powershell
uv run python scripts/datahub/capture_evidence.py
```

Committed evidence is stored in:

| Path | Purpose |
| --- | --- |
| `evidence/09_datahub_governance/datahub_health.json` | GMS health response. |
| `evidence/09_datahub_governance/dataset_count.json` | Dataset, lineage, assertion, and representative lookup counts. |
| `evidence/09_datahub_governance/tag_count.json` | Governance tag verification. |
| `evidence/09_datahub_governance/run_manifest.json` | Evidence capture manifest. |
| `evidence/final_integration/datahub_lineage.md` | Human-readable governance evidence summary. |
| `evidence/final_integration/ui_screenshots/12_datahub_ui.png` | UI screenshot paired with direct metadata proof. |

## Known Limitation

Current known limitation: local DataHub frontend did not render the fuller graph view during capture, even though the repo evidence supports that metadata emission occurred.

For that reason, the governance proof does not rely only on the frontend screenshot. The screenshot is paired with direct GMS health, GraphQL/entity verification, dataset counts, tag counts, lineage counts, and GX assertion counts.

This limitation is local UI/rendering evidence, not a claim that metadata ingestion failed.
