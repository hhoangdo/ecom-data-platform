# ADR 08 Final Integration Run — 2026-06-02

## Stage Status

| Stage | Profile(s) | Key Result |
|-------|-----------|------------|
| 0 | none | Generated 10 parquet files (800 customers, 1,800 orders, 6,891 items) |
| 1 | ingestion+lakehouse | 5 schemas, 8 topics, S3 sink registered, 24,859 events → Kafka, bronze uploaded |
| 2 | lakehouse+batch | Spark backfill FINISHED (1.7 min), 22 Gold tables via HMS |
| 3 | ingestion+lakehouse+streaming | Flink: 2 RUNNING jobs (commerce-metrics, ops-alerts) |
| 4 | ingestion+serving | Pinot: 3 tables bootstrapped, 1 Ctrl/Broker/Server all Alive |
| 5 | lakehouse+orchestration | Airflow + GX Data Docs running |
| 6 | ingestion+lakehouse+governance | DataHub frontend healthy |
| 7 | none | dbt-DuckDB: 119/119 PASS |

## Screenshot Audit

| # | File | Content | Quality |
|---|------|---------|---------|
| S1 | 01_kafka_ui.png | Kafka Dashboard (13 topics, 90 partitions) | GOOD — topics confirmed with messages |
| S2 | 02_schema_registry.png | 5 JSON Schema subjects | GOOD |
| S3 | 03_kafka_connect.png | source-events-s3-sink registered | GOOD |
| S4 | 04_minio_console.png | MinIO login (API confirmed 10 batch + 5 event dirs) | ADEQUATE — login UI issue |
| S5 | 05_trino_ui.png | Trino login (API confirmed ACTIVE, 0 workers) | ADEQUATE — login UI issue |
| S6 | 06_spark_master_ui.png | 1 Completed app (FINISHED, 1.7 min) | GOOD |
| S7 | 07_spark_history_server.png | No completed applications found (event log path issue) | KNOWN ISSUE |
| S8 | 08_flink_ui.png | 2 RUNNING jobs, 4 task slots | GOOD |
| S9 | 09_pinot_ui.png | 3 tables, 1 Ctrl/Broker/Server Alive | GOOD |
| S10 | 10_airflow_ui.png | Airflow login page | ADEQUATE — login attempt failed |
| S11 | 11_gx_data_docs.png | GX Data Docs index | GOOD |
| S12 | 12_datahub_ui.png | DataHub login page | ADEQUATE — login attempt failed |

## Root Fixes Applied

1. **Screenshots AFTER data** — Re-sequenced: data ops → verify → screenshot for every stage
2. **--publish-kafka** — Generator run with `--publish-kafka --kafka-bootstrap-servers localhost:9092`. Verified: 24,859 messages across 5 topics
3. **Pinot bootstrap** — `scripts/pinot/bootstrap.py` executed in Stage 4, 3 tables applied

## Known Issues

- Trino: 0 worker nodes registered (queries stuck QUEUED) — coordinator self-registration issue with Trino 476
- MinIO Console: 403 on session API — web UI login broken, API confirmed data exists
- Airflow: Login failed with airflow/airflow — may need admin user creation
- DataHub: Login failed with datahub/datahub — may need default admin reset
- Spark History: Event log directory s3a://checkpoints/spark-events not written by client-mode job

## dbt Parity

119/119 PASS (52 models, 66 tests, 1 hook) — 12.55 seconds
