# DataHub Governance

## Purpose

DataHub is the governance and metadata catalog layer for the Vina Bim Shop platform. It records datasets, tags, lineage, quality assertions, and representative entity verification across the implemented local stack.

In this coursework, DataHub is not used as a decorative UI. Its responsibility is to prove that the platform can emit metadata about the same assets that the pipelines produce: Kafka topics, MinIO/S3 prefixes, Trino/Iceberg tables, dbt models, Spark and Flink lineage (v1 + v2), and Great Expectations assertions.

## Why DataHub Is Needed

As the platform grows from generator outputs into ingestion, lakehouse, streaming, serving, and orchestration services, it becomes difficult to answer governance questions from file paths alone.

| Governance question | DataHub contribution |
| --- | --- |
| Which datasets exist across the platform? | Dataset entities are emitted for Kafka, S3/MinIO, Trino, and dbt assets. |
| Which layer does a dataset belong to? | Tags such as `bronze`, `silver`, `gold`, `official`, `provisional`, and `quality_gate` classify assets. |
| How did a Gold table or realtime topic get produced? | Spark and Flink lineage (v1 + v2) connect source datasets to processing DataJobs to output datasets. |
| Were quality checks represented as metadata? | GX assertions are emitted as governance evidence. |
| Can evidence be verified without trusting the UI search index? | Representative entity, DataJob, and tag lookups are checked directly through GMS/GraphQL. |

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

Custom lineage and assertion emitters live under `src/vina_bim_shop/datahub_lineage/`:

| Module | Role |
| --- | --- |
| `emitter.py` | Shared `DataHubLineageEmitter` wrapper around `DataHubRestEmitter`, plus URN helpers. |
| `spark_lineage.py` | v1 `UpstreamLineageClass` emission for Spark Silver/Gold lineage. |
| `flink_lineage.py` | v1 `UpstreamLineageClass` emission for Flink real-time lineage. |
| `datajob_lineage.py` | v2 `DataJobInfoClass` + `DataJobInputOutputClass` emission for Spark and Flink DataJobs. |
| `gx_assertions.py` | Great Expectations assertion emission. |

## Lineage types in this project

The repo emits **two** lineage types in parallel. The v1 and v2 lineage are complementary, not redundant:

| Aspect | Where it lives | Code path | Result |
| --- | --- | --- | --- |
| v1 `UpstreamLineageClass` | On each output dataset | `spark_lineage.emit_spark_batch_lineage()`, `flink_lineage.emit_flink_streaming_lineage()` | A flat 2-node graph (`stg_orders` → `fact_order`). UI shows as dataset-to-dataset arrows. |
| v2 `DataJobInputOutputClass` | On a new `DataJob` entity, grouped by a `DataFlow` | `datajob_lineage.emit_datajob_lineage()` | A multi-node graph: `[input dataset] -> [DataJob node] -> [output dataset]`. This is the form the DataHub v1.5 frontend renders with the new entity nodes in the middle of the lineage canvas. |

Both kinds are emitted on every run; the GMS API exposes them as complementary aspects of the same pipeline. Direct GMS GraphQL lookups against `dataJob(urn: ...).inputOutput` are used to verify v2 emission is queryable.

## Service Interactions

| Platform area | Governance relationship |
| --- | --- |
| Data generation | Source datasets and issue evidence provide the starting point for metadata interpretation. |
| Kafka ingestion | Kafka topics are emitted as source and derived topic datasets. |
| Lakehouse | MinIO/S3 prefixes and Trino/Iceberg tables are represented as catalog assets. |
| Spark batch | Spark lineage connects Bronze/Silver/Gold datasets, with v1 `UpstreamLineageClass` and v2 `DataJobInputOutputClass`. |
| Flink streaming | Flink lineage connects raw Kafka topics to derived realtime topics, with v1 and v2 lineage. |
| Pinot serving | Pinot is represented through the realtime serving lineage around derived topics and query evidence. |
| Airflow/GX | DataHub ingestion can be triggered by Airflow, and GX assertions are emitted as quality metadata. |
| dbt-DuckDB | dbt model metadata provides an independent local modeling view for the parity path. |

## Evidence Counts

The latest governance evidence records:

| Asset type | Count |
| --- | ---: |
| Kafka topic datasets | 8 |
| MinIO/S3 prefix datasets | 4 |
| Trino datasets | 45 |
| dbt datasets | 52 |
| Spark v1 lineage edges | 20 |
| Flink v1 lineage edges | 3 |
| v2 Spark `DataJob` entities | 20 |
| v2 Flink `DataJob` entities | 3 |
| GX assertions | 8 |

Representative datasets were verified directly through DataHub GMS/GraphQL:

| Platform | Representative entity |
| --- | --- |
| Iceberg/Trino | `vina_bim_shop.fact_order` |
| Kafka | `commerce_events` |
| Pinot | `realtime_commerce_metrics_1m` |
| S3/MinIO | `checkpoints.spark-events` |

Representative v2 `DataJob` URNs verified directly through GMS GraphQL `dataJob(urn: ...).inputOutput`:

| Pipeline | DataJob URN (short form) |
| --- | --- |
| Spark Iceberg transform | `urn:li:dataJob:(urn:li:dataFlow:(spark,vina-bim-shop-batch,local),iceberg_transform_fact_order)` |
| Flink derive | `urn:li:dataJob:(urn:li:dataFlow:(flink,vina-bim-shop-streaming,local),flink_derive_realtime_commerce_metrics_1m)` |

All 23 expected `DataJob` URNs are verified queryable with non-empty `inputDatasets` and `outputDatasets` (see `evidence/09_datahub_governance/dataset_count.json` -> `datajob_lineage_index_readiness`).

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
|---|---|
| `evidence/09_datahub_governance/datahub_health.json` | GMS health response. |
| `evidence/09_datahub_governance/dataset_count.json` | Dataset, lineage, assertion, and DataJob readiness counts. |
| `evidence/09_datahub_governance/tag_count.json` | Governance tag verification. |
| `evidence/09_datahub_governance/run_manifest.json` | Evidence capture manifest. |
| `evidence/final_integration/datahub_lineage.md` | Human-readable governance evidence summary. |
| `evidence/final_integration/ui_screenshots/README.md` | Final integration UI evidence; see "DataHub UI Evidence Gap" section for the unfixable-real-UI verdict. |
| `tmp/datahub_mae_consumer_diagnosis_2026-06-07.md` | Conclusive diagnosis of why the real DataHub UI cannot be captured in this deployment. |
| `tmp/datahub_focused_supplements_archive_2026-06-07/` | Archived focused PNGs from the previous session (rejected as the final answer). |

## Real DataHub UI is unfixable in this deployment

The DataHub v1.5.0.6 React frontend renders the Lineage canvas via Cytoscape against the OpenSearch graph service, and the Discover and Manage Tags pages against the same OpenSearch index. In this local deployment, the **MAE consumer in GMS does not write to OpenSearch** — a structural issue confirmed across this session and two prior sessions:

- GMS produces MCL events into Kafka successfully.
- The `generic-mae-consumer-job-client` is subscribed and assigned all 6 partitions across `MetadataChangeLog_Versioned_v1` and `MetadataChangeLog_Timeseries_v1`.
- The consumer's hook chain includes `UpdateIndicesHook` (the path that writes to OpenSearch).
- **But the `UpdateIndicesHook` does not write a single document to OpenSearch** after the system-update bootstrap.

Verified evidence (this session, with the OpenSearch named volume in place, against a fresh stack):

- OpenSearch per-index `index_total=0` for **every** DataHub metadata index (`datasetindex_v2`, `datajobindex_v2`, `dataflowindex_v2`, `tagindex_v2`, `graph_service_v1`, `chartindex_v2`, `dashboardindex_v2`, `assertionindex_v2`, `containerindex_v2`, `domainindex_v2`, `glossarytermindex_v2`, `glossarynodeindex_v2`, `corpuserindex_v2`, `corpgroupindex_v2`, `mlfeatureindex_v2`, `mlmodelindex_v2`, `notebookindex_v2`, `dataproductindex_v2`, `dataprocessindex_v2`, ...). Only `datahubpolicyindex_v2: index_total=13` from the system-update bootstrap.
- `BulkListener: Successfully fed bulk request` appears **2 times** in the entire GMS log (the 13-event bootstrap); **zero** post-bootstrap writes.
- `searchAcrossLineage(input: { urn: fact_order, direction: UPSTREAM })` → `searchResults: []` (graph service has no data).
- `search(input: { type: DATASET, query: "vina_bim_shop" })` → `total: 0, searchResults: []` (search index has no data).
- eBean DB (`lakehouse_postgres_data`) has all the data: **24** `dataJobInfo`, **24** `dataJobInputOutput`, **46** `datasetKey`, **16** `assertionInfo`, **107** `globalTags`, **155** `upstreamLineage`.
- GMS GraphQL queries that read from eBean work correctly: `dataJob(urn:...).inputOutput` returns non-empty `inputDatasets` + `outputDatasets`; `dataset(urn:...)` returns the entity.
- No errors in the GMS log that would explain the missing bulk writes — the failure is silent.
- ~6 GB / 15.4 GB memory used; no OOM, no throttling, no resource pressure.

The full diagnosis is in `tmp/datahub_mae_consumer_diagnosis_2026-06-07.md`.

**The repo therefore does not include `12*` DataHub UI captures in `evidence/final_integration/ui_screenshots/`.** Previous sessions produced backend-rendered focused PNGs labeled "NOT the DataHub UI" as an honest supplement; those PNGs have been archived to `tmp/datahub_focused_supplements_archive_2026-06-07/` because they do not show the real DataHub UI. The authoritative governance proof is the backend evidence:

- `evidence/09_datahub_governance/dataset_count.json` — 109 datasets, 23 v2 DataJobs, 8 GX assertions, 6 tags.
- `evidence/09_datahub_governance/tag_count.json` — tag verification.
- Direct GMS GraphQL queries documented in `evidence/final_integration/datahub_lineage.md` and the diagnosis file.

The capture script at `output/playwright/capture_datahub.mjs` is kept on disk (in the gitignored `output/playwright/`) for re-running if/when the MAE consumer is fixed in this deployment or a different DataHub image is used.

This verdict applies to the React UI only. The DataHub metadata backend (GMS + eBean DB) is fully functional and queryable.
