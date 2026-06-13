# `evidence`

This folder contains committed proof artifacts for the coursework platform. Review it after the matching deliverable when you want to verify what was produced, captured, queried, or packaged.

## What Is Here

| Path | Purpose |
| --- | --- |
| `01_data_generator/` | Generator run manifests, row counts, quality reports, schema summaries, and sample rows. |
| `02_schema_design/` | dbt/schema evidence, table counts, screenshots, and build reports. |
| `03_kafka_ingestion/` | Kafka topic, Schema Registry, Kafka Connect, producer, consumer, and screenshot evidence. |
| `04_lakehouse/` | MinIO, Hive Metastore, Trino, Bronze landing, and lakehouse smoke evidence. |
| `05_spark_batch/` | Spark batch, dbt parity, Great Expectations, Trino Gold, and Spark UI evidence. |
| `06_flink_streaming/` | Flink runtime, derived topics, checkpoints, and streaming verification evidence. |
| `07_pinot_serving/` | Pinot table, query, reconciliation, and serving-layer evidence. |
| `08_airflow_gx/` | Airflow/GX run manifests, validation outputs, and generated Data Docs. |
| `09_datahub_governance/` | DataHub health, dataset, tag, and run evidence. |
| `final_dataset/` | Submitted final raw dataset package and manifest. |
| `final_integration/` | Cross-platform health, lineage, reconciliation, UI screenshot, and final manifest evidence. |

## How To Read It

Use the numbered folders with the matching `../deliverables/` document. Markdown, JSON, CSV, and SQL output files are the main machine-verifiable evidence. Screenshot folders are historical review evidence and are not the official regeneration path.

## How It Differs From Similar Folders

| Folder | Role |
| --- | --- |
| `evidence/` | Captured outputs proving the platform ran or produced expected artifacts. |
| `deliverables/` | Narrative coursework writeups explaining design and implementation choices. |
| `data/` | Local generated runtime data, mostly gitignored, plus committed reference inputs. |
