# ADR 08 Final Integration Repair - 2026-06-03

## Stage Status

| Stage | Profile(s) | Key Result |
|-------|------------|------------|
| 0 | none | Generated 10 parquet files (800 customers, 1,800 orders, 6,891 items) |
| 1 | ingestion+lakehouse | 5 schemas, 8 topic datasets in Kafka/DataHub, S3 sink registered, 24,859 events published, bronze uploaded |
| 2 | lakehouse+batch | Spark backfill completed; 22 Gold tables visible in Trino; Spark History now shows 4 completed applications |
| 3 | ingestion+lakehouse+streaming | Flink: 2 RUNNING jobs (commerce-metrics, ops-alerts) |
| 4 | ingestion+serving | Pinot: 3 tables bootstrapped, controller/broker/server all Alive |
| 5 | lakehouse+orchestration | Airflow healthy; GX Data Docs served; latest `datahub_ingestion` run succeeded |
| 6 | ingestion+lakehouse+governance | DataHub GMS/frontend healthy; 4 recipes plus custom lineage emitted successfully |
| 7 | none | dbt-DuckDB parity: 119/119 PASS |

## Screenshot Audit

| # | File | Content | Quality |
|---|------|---------|---------|
| S1 | 01_kafka_ui.png | Kafka Dashboard with populated topics | GOOD |
| S2 | 02_schema_registry.png | 5 JSON Schema subjects | GOOD |
| S3 | 03_kafka_connect.png | `source-events-s3-sink` registered | GOOD |
| S4 | 04_minio_console.png | MinIO bucket browser after login | GOOD |
| S5 | 05_trino_ui.png | Trino cluster overview with 1 active worker | GOOD |
| S6 | 06_spark_master_ui.png | Spark Master with completed batch app | GOOD |
| S7 | 07_spark_history_server.png | Spark History completed applications list | GOOD |
| S8 | 08_flink_ui.png | 2 RUNNING Flink jobs | GOOD |
| S9 | 09_pinot_ui.png | Pinot cluster with 3 tables and healthy nodes | GOOD |
| S10 | 10_airflow_ui.png | Airflow DAG overview with all 6 required DAGs and `datahub_ingestion` history | GOOD |
| S11 | 11_gx_data_docs.png | GX Data Docs `bronze_raw_minio` expectation detail page | GOOD |
| S12 | 12_datahub_ui.png | DataHub `fact_order` lineage tab plus supplemental GraphQL evidence table | GOOD |

## Root Fixes Applied

1. Trino repaired with container-host discovery (`trino`) plus a dedicated `trino-worker` service.
2. Spark batch event logging forced to `s3a://checkpoints/spark-events`, which restored Spark History evidence.
3. Airflow rebuilt with the DataHub CLI/plugin, then the governance recipes were corrected and rerun successfully.
4. Login-only screenshots were replaced with live content using a local `npx` Playwright capture flow.
5. GX Data Docs now render expectation-level detail pages and preserve quality reports when DataHub ingestion docs are refreshed.
6. Airflow webserver startup timeouts were raised for the heavy local DataHub-plugin image.

## Verification Highlights

- Trino: `SHOW TABLES FROM iceberg.gold` returned 22 tables, `fact_order` returned 1,800 rows, `agg_hourly_reconciled_kpi` returned 164 rows, and `feat_stream_60m` returned 4,652 rows.
- Spark History: `http://localhost:18080/api/v1/applications` returned 4 completed `vina-bim-shop-batch` applications.
- Airflow: latest successful governance run is `adr08_datahub_20260603T003300Z`.
- DataHub: recipe outputs emitted 109 datasets total (8 Kafka, 4 S3, 45 Trino, 52 dbt), plus 20 Spark lineage entities, 3 Flink lineage entities, and 8 GX assertions.

## dbt Parity

119/119 PASS (52 models, 66 tests, 1 hook). Spark/Iceberg/Trino matches dbt-DuckDB for all audited row counts and KPIs.
