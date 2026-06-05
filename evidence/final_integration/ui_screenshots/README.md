# Final Integration UI Evidence

This directory contains the canonical UI screenshot set for the final integration run of the `vina-bim-shop` platform. Together, these images document the operational state of the core services that support event ingestion, schema management, object storage, batch and streaming processing, analytical serving, orchestration, data quality reporting, and metadata governance.

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
| `12_datahub_ui.png` | DataHub | The governed `vina_bim_shop.fact_order` dataset opens on the Lineage tab, supplemented by GraphQL evidence below. |

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

### `12_datahub_ui.png`

This image shows the DataHub dataset workspace centered on `vina_bim_shop.fact_order` with the Lineage tab open. The visible summary panel identifies the dataset, its Iceberg platform grouping, and the attached `silver` tag. In this local cold-start state, the UI opens the governed entity but does not reliably render search results, assertion cards, or upstream lineage edges, so the screenshot is paired with direct GMS/GraphQL evidence.

| Evidence Source | Value | What It Proves |
|---|---:|---|
| `datahub_health.json.status_code` | 200 | GMS was reachable during the refresh. |
| `dataset_count.json.total_datasets_emitted` | 109 | Kafka, S3/MinIO, Trino, and dbt assets were emitted. |
| `dataset_count.json.counts_by_recipe.kafka_topics` | 8 | Kafka topic catalog coverage is present. |
| `dataset_count.json.counts_by_recipe.minio_storage` | 4 | MinIO/S3 storage-prefix coverage is present. |
| `dataset_count.json.counts_by_recipe.trino_tables` | 45 | Trino/Iceberg table catalog coverage is present. |
| `dataset_count.json.counts_by_recipe.dbt_legacy` | 52 | dbt-DuckDB parity model coverage is present. |
| `dataset_count.json.custom_lineage.spark_entities` | 20 | Spark Silver/Gold lineage metadata was emitted. |
| `dataset_count.json.custom_lineage.flink_entities` | 3 | Flink real-time lineage metadata was emitted. |
| `dataset_count.json.custom_lineage.gx_assertions_emitted` | 8 | GX assertion metadata was emitted. |
| `dataset_count.json.verified_representative_datasets` | 4/4 | Representative Iceberg, Kafka, Pinot, and S3 entities resolve through GraphQL. |
| `tag_count.json.verified_tag_count` | 6 | Governance vocabulary tags resolve through GraphQL. |
