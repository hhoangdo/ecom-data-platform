# Final Integration UI Evidence

This directory contains the canonical UI screenshot set for the final integration run of the `vina-bim-shop` platform. Together, these images document the operational state of the core services that support event ingestion, schema management, object storage, batch and streaming processing, analytical serving, orchestration, and data quality reporting.

The DataHub governance layer is documented separately: see [DataHub UI Evidence Gap](#datahub-ui-evidence-gap-real-ui-is-unfixable) below.

## Overview

| File | Platform / Component | What the Screenshot Confirms |
|---|---|---|
| `01_kafka_ui.png` | Kafka UI | The Kafka cluster is online and exposes the expected broker, topic, and partition footprint for the integrated environment. |
| `02_schema_registry.png` | Schema Registry | Event value schemas are registered for the platform's Kafka topics. |
| `03_kafka_connect.png` | Kafka Connect | The S3 sink connector responsible for object storage delivery is present. |
| `04_minio_console.png` | MinIO Object Store | The bronze, silver, gold, checkpoints, and evidence buckets are available with populated contents. |
| `05_trino_ui.png` | Trino | The query engine is running with one active worker and is ready to serve analytical access. |
| `06_spark_master_ui.png` | Spark Master | The Spark cluster has a live worker and a completed batch application for the project workload. |
| `07_spark_history_server.png` | Spark History Server | Historical batch executions and downloadable event logs are retained for inspection. |
| `08_flink_ui.png` | Flink Dashboard | Streaming jobs are active and task execution capacity is available. |
| `09_pinot_ui.png` | Pinot Cluster Manager | The real-time serving cluster is online with its controller, broker, server, and registered tables. |
| `10_airflow_ui.png` | Airflow | The DAG overview lists all six required orchestration DAGs and shows `datahub_ingestion` run history. |
| `11_gx_data_docs.png` | GX Data Docs | The bronze validation detail page shows expectation-level results, severity, DAG blocking, and quarantine behavior. |

## Screenshot Catalog

### `01_kafka_ui.png`

This screenshot shows the Kafka UI dashboard for the `vina-bim-shop-local` cluster. At capture time, the interface reports one online cluster, one broker, 13 topics, and 90 partitions, which establishes that the messaging backbone is active and populated with the expected topic inventory.

### `02_schema_registry.png`

This image captures the Schema Registry response listing registered value subjects. The visible subject set includes `catalog_events-value`, `commerce_events-value`, `dead_letter_events-value`, `fulfillment_events-value`, and `ops_events-value`, confirming that the event streams have corresponding schema registrations.

### `03_kafka_connect.png`

This screenshot records the Kafka Connect endpoint response for the configured connectors. The visible connector name, `source-events-s3-sink`, confirms that the pipeline component responsible for persisting source events into object storage is registered in the environment.

### `04_minio_console.png`

This image shows the MinIO Object Browser after authentication. The console lists the `bronze`, `silver`, `gold`, `checkpoints`, and `evidence` buckets together with object counts and storage sizes, demonstrating that the lakehouse storage layout exists and contains materialized artifacts.

### `05_trino_ui.png`

This screenshot captures the Trino cluster overview for `VINA_BIM_SHOP_LOCAL`. The page shows one active worker, no running, queued, or blocked queries at the moment of capture, and zero throughput, which indicates the cluster is healthy and idle rather than unavailable.

### `06_spark_master_ui.png`

This image shows the Spark Master status page at `spark://spark-master:7077`. It reports one live worker and one completed application named `vina-bim-shop-batch`, confirming that the batch processing cluster is active and has already executed the project batch workload successfully.

### `07_spark_history_server.png`

This screenshot documents the Spark History Server backed by the `s3a://checkpoints/spark-events` event log directory. The table lists four completed `vina-bim-shop-batch` runs with start and completion timestamps, durations, and downloadable event logs, providing evidence of repeatable completed batch executions.

### `08_flink_ui.png`

This image captures the Flink dashboard overview. It shows one task manager, four total task slots with two currently available, and two running jobs named `vina-bim-shop-commerce-metrics` and `vina-bim-shop-ops-alerts`, demonstrating that the streaming layer is actively processing workloads.

### `09_pinot_ui.png`

This screenshot shows the Pinot cluster manager home view for `vina-bim-shop-pinot`. The dashboard reports one controller, one broker, one server, three tables, and a `DefaultTenant` entry bound to those resources, confirming that the serving layer is online and that the expected table set is registered.

### `10_airflow_ui.png`

This image captures the Airflow DAG overview. The page lists all six required orchestration DAGs: `datahub_ingestion`, `hourly_batch_lakehouse`, `kafka_topic_bootstrap`, `local_evidence_build`, `pinot_bootstrap`, and `reconciliation_report`. The visible `datahub_ingestion` run history also shows that metadata ingestion is wired into the orchestrator rather than documented only as a standalone command.

### `11_gx_data_docs.png`

This screenshot records the GX Data Docs detail page for the `bronze_raw_minio` validation suite. The page shows a `warning` result with `1/2 expectations passed`, including the failed not-null path expectation, the passing row-count expectation, `Blocks DAG: no`, and `Requires quarantine: yes`. The Data Docs index also links to `gold_trino_contract`, which passes `2/2` expectations.

## DataHub UI Evidence Gap: Real UI is Unfixable

The DataHub governance layer in this repo is real and complete at the GMS API level:

- 109 datasets across 4 recipes (Kafka 8, MinIO/S3 4, Trino 45, dbt 52)
- 20 Spark v1 lineage edges
- 3 Flink v1 lineage edges
- 24 v2 `DataJob` entities (20 Spark + 3 Flink + 1 Airflow orchestration DAG)
- 8 GX assertions
- 6 governance vocabulary tags
- 155 upstreamLineage aspect rows
- 107 globalTags aspect rows

All of this is verifiable directly through GMS GraphQL against the eBean DB. The v2 lineage is verifiable via `dataJob(urn:...).inputOutput` (returns non-empty `inputDatasets` + `outputDatasets`); representative entities are verifiable via `dataset(urn:...)`; tags are verifiable via `tag(urn:...)`. The evidence tables in `evidence/09_datahub_governance/dataset_count.json` and `evidence/09_datahub_governance/tag_count.json` are the authoritative governance proof.

**However, the DataHub v1.5.0.6 React frontend cannot render any of this in this local deployment.** The frontend's Lineage canvas (Cytoscape), Discover search, and Manage Tags page all read from the OpenSearch graph service and search index. The GMS v1.5.0.6 MAE consumer — the Kafka pipeline that is supposed to mirror the eBean DB aspects into OpenSearch — is structurally broken in this deployment. Verified across this session and two prior sessions:

| Evidence | Finding |
|---|---|
| GMS `/health` | 200 (GMS is healthy) |
| MAE consumer registered | Yes, `consumer-generic-mae-consumer-job-client-5`, subscribed to `MetadataChangeLog_Versioned_v1` and `MetadataChangeLog_Timeseries_v1` |
| MAE consumer partitions assigned | All 6 partitions (3 + 3) |
| MAE consumer hooks enabled | `[UpdateIndicesHook, EntityChangeEventGeneratorHook, FormAssignmentHook, IncidentsSummaryHook, IngestionSchedulerHook, SiblingAssociationHook]` |
| `BulkListener: Successfully fed bulk request` log entries | **2 entries** total, both for the 13-event system-update bootstrap. **Zero** post-bootstrap writes. |
| OpenSearch per-index `index_total` for `datasetindex_v2`, `datajobindex_v2`, `dataflowindex_v2`, `tagindex_v2`, `graph_service_v1`, etc. | **0** for every DataHub metadata index |
| `searchAcrossLineage(input: { urn: fact_order, direction: UPSTREAM })` | `searchResults: []` (graph service has no data) |
| `search(input: { type: DATASET, query: "vina_bim_shop" })` | `total: 0, searchResults: []` (search index has no data) |
| `dataJob(urn:...).inputOutput` (eBean-backed) | Returns the v2 DataJob with non-empty input/output datasets |
| `dataset(urn:...)` (eBean-backed) | Returns the entity |
| Memory pressure / OOM | None (~6 GB / 15.4 GB used; containers all healthy) |
| Errors in GMS log explaining missing bulk writes | None — silent failure |

**The MAE consumer is alive, assigned, and has the right hook chain, but it does not write a single document to OpenSearch.** This is reproducible from a fresh `docker compose up`, with the OpenSearch named volume that this session added, with the postmortem's staged profile startup, and after GMS restarts. It is a structural issue with the GMS v1.5.0.6 + OpenSearch 2.19.3 combination in this deployment, not a timing or cold-start window.

**This directory does not contain `12*` DataHub UI captures.** Previous sessions produced backend-rendered focused PNGs labeled "NOT the DataHub UI" as an honest supplement. Those PNGs have been archived to `tmp/datahub_focused_supplements_archive_2026-06-07/`. Producing them again as the final answer was explicitly rejected because they do not show the real DataHub UI. The real DataHub UI cannot be captured in this deployment.

The authoritative governance proof is the backend evidence in `evidence/09_datahub_governance/dataset_count.json` and the GraphQL queries listed in `evidence/final_integration/datahub_lineage.md`. The full root-cause diagnosis is recorded in `tmp/datahub_mae_consumer_diagnosis_2026-06-07.md`.

## Evidence Sources (DataHub governance, when readable directly through GMS)

| Evidence Source | Value | What It Proves |
|---|---:|---|
| `datahub_health.json.status_code` | 200 | GMS was reachable during the refresh. |
| `dataset_count.json.total_datasets_emitted` | 109 | Kafka, S3/MinIO, Trino, and dbt assets were emitted. |
| `dataset_count.json.counts_by_recipe.kafka_topics` | 8 | Kafka topic catalog coverage is present. |
| `dataset_count.json.counts_by_recipe.minio_storage` | 4 | MinIO/S3 storage-prefix coverage is present. |
| `dataset_count.json.counts_by_recipe.trino_tables` | 45 | Trino/Iceberg table catalog coverage is present. |
| `dataset_count.json.counts_by_recipe.dbt_legacy` | 52 | dbt-DuckDB parity model coverage is present. |
| `dataset_count.json.custom_lineage.spark_entities` | 20 | Spark Silver/Gold v1 lineage metadata was emitted. |
| `dataset_count.json.custom_lineage.flink_entities` | 3 | Flink real-time v1 lineage metadata was emitted. |
| `dataset_count.json.custom_lineage.gx_assertions_emitted` | 8 | GX assertion metadata was emitted. |
| `dataset_count.json.custom_lineage.datajob_entities` | 23 | Spark + Flink v2 `DataJob` lineage metadata was emitted (20 spark + 3 flink). |
| `dataset_count.json.datajob_lineage_index_readiness.status` | `success` | All 23 DataJob URNs verified queryable with non-empty `inputOutput` via direct GMS GraphQL. |
| `dataset_count.json.verified_representative_datasets` | 4/4 | Representative Iceberg, Kafka, Pinot, and S3 entities resolve through GraphQL. |
| `tag_count.json.verified_tag_count` | 6 | Governance vocabulary tags resolve through GraphQL. |
| Direct GMS `dataJob(urn:...).inputOutput` query | 23/23 | Confirms v2 DataJob lineage is queryable at the aspect level (more direct than `searchAcrossLineage`, which is empty in this deployment). |
| Direct GMS `searchAcrossLineage` and `search` queries | 0 results | Confirms the OpenSearch index has no documents; this is the structural gap that prevents the React UI from rendering. |
