# Vina Bim Shop Master Plan

## Purpose

This master plan summarizes the implemented Vina Bim Shop data platform and keeps the architecture folder aligned with the official coursework deliverables. The project models a Shopee-inspired Vietnamese marketplace and demonstrates a staged local Lambda-style data platform for both realtime operations and reconciled batch analytics.

The root README is the entrypoint for setup and submission context. The official service documentation lives in `../deliverables/`, while this file gives the architecture-level view of how the pieces fit together.

## Current Evidence Boundary

The submitted platform evidence covers:

| Area | Implemented evidence |
| --- | --- |
| Data generation | Synthetic marketplace snapshots, Kafka-shaped event logs, bad records, issue manifests, and final raw dataset package. |
| Ingestion | Kafka KRaft, Schema Registry, Kafka Connect, Kafka UI, topic contracts, and Bronze event landing evidence. |
| Lakehouse | MinIO object storage, Hive Metastore, shared Postgres, Trino, Bronze/Silver/Gold layout, and Iceberg catalog evidence. |
| Batch | Spark batch transformation from Bronze to Silver/Gold, Trino smoke checks, dbt-DuckDB parity, and executive mart export. |
| Streaming | Flink event-time jobs, one-minute commerce metrics, operational alerts, late-event correction snapshots, checkpoints, and audit JSONL. |
| Serving | Trino canonical Gold SQL, Apache Pinot realtime OLAP, and DuckDB local evidence files. |
| Orchestration and quality | Airflow DAGs, Great Expectations validation policy, GX Data Docs, and evidence manifests. |
| Governance | DataHub metadata emission, lineage, tags, assertions, indexed search, and rendered entity-page checks. |

The current evidence excludes production deployment hardening, CI/CD rollout, security/RBAC enforcement beyond local defaults, drift scenario implementation, ML model training/serving, and LLM application design.

## Business Context

`vina-bim-shop` is a synthetic multi-seller e-commerce platform with customers, sellers, products, promotions, orders, payments, shipments, inventory, and behavior events. The domain is intentionally simplified enough for coursework but realistic enough to test common data engineering concerns.

Core actors:

| Actor | Role |
| --- | --- |
| Customer | Browses, searches, adds to cart, checks out, pays, and receives shipments. |
| Seller | Lists products, manages prices and inventory, and fulfills orders. |
| Platform | Owns taxonomy, promotions, reporting standards, pipeline contracts, and governance. |
| Logistics provider | Emits shipment lifecycle updates and delivery outcomes. |
| Payment provider | Emits payment attempts, successes, failures, and payment-blocked fulfillment conditions. |

The platform supports questions such as category and seller contribution to GMV, payment-failure patterns, delayed shipment risk, conversion behavior, inventory availability, and customer feature readiness.

## Source Contracts

The generator produces two complementary source streams.

| Source stream | Format | Purpose |
| --- | --- | --- |
| Periodic table-state snapshots | Parquet | Checkpointed source-of-record state for batch reconciliation. |
| Kafka-shaped event envelopes | JSON messages persisted as JSONL | Replayable event history for realtime metrics, event timing, and source observability. |

The overlap between snapshots and events is intentional. Snapshots answer what state is reliable at an export checkpoint; events answer what happened now and in what order.

Implemented snapshot datasets include `customers`, `sellers`, `products`, `product_category_map`, `inventory_snapshots`, `promotions`, `orders`, `order_items`, `payments`, `shipments`, and `bad_snapshots`.

Implemented source topics include `commerce_events`, `catalog_events`, `fulfillment_events`, `ops_events`, and `dead_letter_events`.

## Platform Architecture

The platform is split into a speed path and a batch truth path.

| Path | Flow | Truth role |
| --- | --- | --- |
| Realtime speed path | Generator events -> Kafka -> Flink -> derived Kafka topics -> Pinot | Fresh and provisional operational view. |
| Batch truth path | Generator snapshots/events -> Bronze -> Spark Silver/Gold Iceberg -> Trino | Canonical reconciled reporting truth. |
| Local evidence path | Raw files or Trino Gold -> DuckDB | Portable parity and executive review artifacts. |

Truth policy:

- Spark Gold through Trino is canonical for official historical reporting.
- Pinot is fresh and provisional for live metrics, alerts, and correction-aware dashboards.
- dbt-DuckDB is a local parity oracle, not the distributed serving surface.
- DuckDB Executive Mart is a local snapshot exported from Trino Gold and is stale until regenerated.

## Layering Model

| Layer | Storage shape | Responsibility |
| --- | --- | --- |
| Raw | Local generated Parquet and JSONL under `../data/raw/` | Source simulation and reproducible local evidence. |
| Bronze | Source-fidelity object data and raw SQL models | Preserve snapshots, event envelopes, schema versions, payloads, ingest metadata, and quarantine records. |
| Silver | Standardized views/tables | Deduplicate, cast, normalize nullable drift fields, and flatten event envelopes. |
| Gold | Iceberg tables and dbt-DuckDB Gold models | Provide business-ready dimensions, facts, OBT, aggregates, and feature tables. |
| Serving | Trino, Pinot, DuckDB | Expose canonical SQL, realtime OLAP, and portable local review files. |
| Governance | DataHub metadata graph | Catalog datasets, tags, lineage, and quality assertions. |

## Implemented Service Profiles

Docker Compose profiles are operated in stages rather than as one broad full-stack startup.

| Profile | Services | Official documentation |
| --- | --- | --- |
| `ingestion` | Kafka KRaft, Schema Registry, Kafka Connect, Kafka UI | [Kafka ingestion](../deliverables/03_kafka_ingestion.md) |
| `lakehouse` | MinIO, Hive Metastore, Trino, shared Postgres | [Lakehouse](../deliverables/04_lakehouse.md) |
| `batch` | Spark master, worker, history server | [Spark batch](../deliverables/05_spark_batch.md) |
| `streaming` | Flink JobManager, TaskManager, job submitter | [Flink streaming](../deliverables/06_flink_streaming.md) |
| `serving` | Pinot Zookeeper, controller, broker, server | [Pinot serving](../deliverables/07_pinot_serving.md) |
| `orchestration` | Airflow webserver, scheduler, init, GX Data Docs | [Airflow + GX](../deliverables/08_airflow_gx_orchestration.md) |
| `governance` | DataHub GMS, frontend, actions, Elasticsearch | [DataHub governance](../deliverables/09_datahub_governance.md) |
| local analytics | dbt-DuckDB and DuckDB Executive Mart files | [DuckDB/dbt local analytics](../deliverables/10_duckdb_dbt_local_analytics.md) |

## Data Formats

| Format | Used by | Rationale |
| --- | --- | --- |
| Parquet | Offline source snapshots and batch raw package | Compact columnar source-state format for Spark/dbt reads. |
| JSON | Kafka event messages | Flexible event envelopes with schema versions, timestamps, correlation IDs, and payloads. |
| JSONL | Local topic files, DLQ examples, audit outputs | One message per line keeps streams replayable, readable, and packageable. |
| Iceberg | Silver/Gold distributed lakehouse tables | Durable table format for Spark writes and Trino reads through Hive Metastore. |
| Pinot realtime segments | Flink-derived serving topics | Low-latency OLAP format for fresh dashboard and alert queries. |
| DuckDB files | Parity oracle and executive mart | Single-file local analytics artifacts for DBeaver and evidence packaging. |

## Data Model

The Gold model contains:

| Family | Implemented tables |
| --- | --- |
| Dimensions | `dim_customer`, `dim_seller`, `dim_product`, `dim_category`, `dim_date`, `dim_payment_method`, `dim_order_status`, `dim_shipment_status`, `dim_shipping_method`, `dim_promotion` |
| Bridge | `bridge_product_category` |
| Facts | `fact_order`, `fact_order_item`, `fact_payment_attempt`, `fact_shipment`, `fact_inventory_snapshot`, `fact_promotion_application` |
| OBT and aggregate | `obt_order_performance`, `agg_hourly_reconciled_kpi` |
| Features | `feat_customer_90d`, `feat_stream_60m`, `feat_customer_unified` |

Detailed schema documentation is in [Schema Design and Data Dictionary](../deliverables/02_schema_design.md). ERD assets live in `diagrams/erd/physical_gold_model.puml` and `diagrams/erd/gold_layer_ERD.dbml`.

## Quality And Governance

Quality policy:

- Bronze warnings quarantine malformed source records without hiding source drift.
- Silver and Gold failures block orchestration because those layers feed official reporting.
- Pinot query issues warn unless reconciliation fails.
- DataHub ingestion is verified through GMS health, indexed search, entity checks, tags, lineage counts, assertions, and frontend UI proof.

Governance evidence includes Kafka datasets, MinIO/S3 prefixes, Trino/Iceberg tables, dbt datasets, Spark lineage, Flink lineage, GX assertions, and representative tags such as `bronze`, `silver`, `gold`, `official`, `provisional`, and `quality_gate`.

The recovery evidence shows indexed `fact_order` search and a rendered lineage graph after a frontend restart; direct GMS/GraphQL verification remains a supplemental check rather than an acceptance substitute.

## Evidence Map

| Evidence area | Path |
| --- | --- |
| Data generator | `../evidence/01_data_generator/` |
| Schema design | `../evidence/02_schema_design/` |
| Kafka ingestion | `../evidence/03_kafka_ingestion/` |
| Lakehouse | `../evidence/04_lakehouse/` |
| Spark batch | `../evidence/05_spark_batch/` |
| Flink streaming | `../evidence/06_flink_streaming/` |
| Pinot serving | `../evidence/07_pinot_serving/` |
| Airflow + GX | `../evidence/08_airflow_gx/` |
| DataHub governance | `../evidence/09_datahub_governance/` |
| Final integration | `../evidence/final_integration/` |
| Final raw dataset | `../evidence/final_dataset/` |

## Repository Ownership

| Area | Responsibility |
| --- | --- |
| `domain/` | Business context, taxonomy, event catalog, and source-to-target mapping. |
| `diagrams/` | PlantUML, DBML, Excalidraw, and rendered architecture assets. |
| `../deliverables/` | Official coursework documentation. |
| `../configs/` | Generator, pipeline, service, and scenario configuration. |
| `../src/vina_bim_shop/` | Python implementation packages. |
| `../scripts/` | CLI entrypoints for generation, bootstrap, evidence, batch, streaming, serving, and governance. |
| `../evidence/` | Committed manifests, reports, screenshots, query outputs, and verification artifacts. |

## Assumptions And Defaults

- Python `3.12` and `uv` are the local runtime defaults.
- Large generated datasets remain gitignored under `../data/`; committed evidence packages are stored under `../evidence/`.
- Staged Docker Compose profile startup is the supported local operating model.
- Spark/Iceberg/Trino Gold is the canonical reporting path.
- Pinot is the realtime operational serving path.
- DuckDB files are local reproducibility and evidence artifacts.
- Drift scenarios, ML implementation, and LLM implementation remain outside the current platform evidence.
