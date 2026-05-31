# Vina Bim Shop Master Plan

## 1. Project Overview

### Objective

This repository is the implementation and design workspace for an end-to-end Data and AI coursework project in the e-commerce domain. The working product is a Shopee-inspired marketplace called `vina-bim-shop`.

The coursework requires both:

- design documents in Markdown
- runnable implementation artifacts with evidence

### Current Phase Scope

This master plan governs the first implementation phase only:

- Section `01`: data generator design and implementation
- Section `02`: storage, schema, and pipeline contract design and implementation

### Out of Scope for This Phase

The following sections are intentionally deferred, but the repo is structured to support them later:

- Section `03`: data generator improvement and drift scenarios
- Section `04.1`: ML design and implementation
- Section `04.2`: LLM design and implementation

### Success Criteria for the Current Phase

- Offline and streaming source datasets both exist.
- Dataset contracts, grains, timestamps, and controls are documented.
- Lambda architecture expectations are documented: Kafka ingestion, Spark batch, Flink streaming, and a MinIO-backed medallion lakehouse.
- Bronze, Silver, Gold, and feature-serving expectations are documented, while final Gold schemas remain a Section `02` task.
- The repo cleanly separates architecture docs, coursework deliverables, code, SQL, data, and evidence.
- The scaffold is reproducible with `uv sync` and executable later with `uv run`.

## 2. Business and Domain Definition

### Product Definition

`vina-bim-shop` is a Shopee-like e-commerce platform focused on consumer retail behavior, order lifecycles, and marketplace operations. The design is intentionally simplified for coursework, but realistic enough to support data engineering and downstream AI scenarios.

### Business Actors

- `customer`: browses products, places orders, pays, and receives shipments
- `seller`: lists products, manages price and inventory, and fulfills orders
- `platform`: owns catalog structure, promotions, data pipelines, and business reporting
- `logistics_provider`: ships orders and updates delivery milestones
- `payment_provider`: processes payment attempts and payment outcomes

### Key Business Questions

The data foundation in Sections `01` and `02` should answer or support:

- Which categories, sellers, and products drive GMV, order volume, and repeat purchases?
- How do browse and cart behaviors convert into purchases by device, source, and category?
- Which operational bottlenecks cause delayed delivery, failed payments, or abandoned checkouts?
- Which customer and product features should be available later for ML or LLM-powered use cases?

### Scope Boundaries

- This project models a single platform, not multiple marketplaces.
- International tax, returns, refunds, and seller finance workflows are out of scope in the first phase.
- Sensitive personal data is not required; synthetic identifiers and coarse location fields are sufficient.

## 3. Product Taxonomy Snapshot

### Taxonomy Policy

The taxonomy is Shopee-inspired, but project-owned and stable for the coursework. It is not a live mirror of Shopee and should not depend on external taxonomy changes after this phase starts.

The human-readable taxonomy lives in:

- `architecture/domain/category-taxonomy.md`

The machine-readable snapshot lives in:

- `data/reference/taxonomy/taxonomy_snapshot.yaml`

### Level-1 Categories

- `FMCG`
- `ELHA`
- `Fashion`
- `Home & Living`

### Fixed FMCG Subcategories

- `Health & Beauty`
- `Mom & Baby`
- `Household Goods`
- `Groceries`
- `Pet Care`

### Assumption for ELHA

For this coursework, `ELHA` means `Electronics and Home Appliances`. This keeps the category label close to the user's wording while making the business meaning explicit in documentation and future schemas.

## 4. Section 01 Source Data Design

### Section Goal

Build a configurable synthetic data generator that produces realistic e-commerce source data for both offline and streaming paths.

### Offline Datasets

The offline generator should produce the following domain tables as Parquet files:

| Dataset | Grain | Core Keys | Purpose |
| --- | --- | --- | --- |
| `customers` | one row per customer | `customer_id` | customer profile and segmentation |
| `sellers` | one row per seller | `seller_id` | seller reference and fulfillment traits |
| `products` | one row per product | `product_id`, `seller_id` | catalog, pricing, and active status |
| `product_category_map` | one row per product-category assignment | `product_id`, `subcategory_id` | taxonomy link |
| `inventory_snapshots` | one row per product snapshot per time | `product_id`, `snapshot_ts` | stock level history |
| `orders` | one row per order | `order_id`, `customer_id` | order header lifecycle |
| `order_items` | one row per order line | `order_item_id`, `order_id`, `product_id` | order detail and quantities |
| `payments` | one row per payment attempt | `payment_id`, `order_id` | payment outcomes |
| `shipments` | one row per shipment | `shipment_id`, `order_id` | logistics milestones |
| `promotions` | one row per promotion definition | `promotion_id` | campaign context and discounts |

### Streaming Datasets

The streaming generator produces human-readable JSON event payloads that are shaped like Kafka topic messages. In Section `01`, these are written as JSONL files rather than published to a running Kafka cluster.

The event catalog lives in:

- `architecture/domain/source-event-catalog.md`

Approved Kafka domain topics:

| Topic | Event Families |
| --- | --- |
| `commerce_events` | sessions, search, product views, cart, checkout, order, and payment outcomes |
| `catalog_events` | product, price, inventory, and promotion source changes |
| `fulfillment_events` | shipment lifecycle and payment-blocked fulfillment events |
| `ops_events` | heartbeat, burst, lateness, duplicate, and schema-version observability events |

Kafka-topic-shaped JSONL outputs are the authoritative streaming source contract for Lambda architecture. The older flat stream helper output is removed so Section `02` has one clear streaming contract.

### Lambda Architecture Contract

Section `01` models the source side of a Lambda architecture:

- Kafka is the ingestion layer for domain-grouped JSON events.
- Spark is the hourly batch compute path that prepares curated Silver/Gold tables for consumers who accept 1-hour freshness.
- Flink is the real-time streaming compute path for BI/livestreaming use cases that need revenue, payment issue, traffic burst, and anomaly visibility.
- Apache Pinot is the realtime OLAP serving sink for low-latency live dashboard queries over Flink-derived operational metrics.
- MinIO stores medallion lakehouse files; Hive Metastore stores table metadata; Trino provides the canonical SQL serving access for curated Gold tables.
- DuckDB is a local coursework-friendly executive mart generated from Gold tables; it is not the canonical multi-user warehouse.
- Runnable Kafka/Spark/Flink/MinIO/Hive Metastore/Trino/Pinot/DuckDB jobs are deferred to Section `02`; Section `01` only owns source contracts and generated raw payloads.

Architecture note:

- Offline source snapshots are table-state extracts such as hourly `orders`, `payments`, and `customers` Parquet dumps landed into the MinIO Bronze/raw layer.
- Replayable event logs are append-only Kafka event histories, such as `order_placed` or `payment_failed`, landed into the MinIO Bronze/raw layer as JSONL.
- Spark batch inputs come from landed Bronze data in MinIO, not directly from operational source systems.
- Flink reads Kafka directly for real-time processing, while Kafka-to-MinIO landing preserves replayable event history for later Spark recomputation.
- Raw Bronze files are not the normal business consumption interface. Executive teams consume Trino-accessible curated tables after Spark writes Silver/Gold outputs, with a DuckDB local mart available for coursework dashboards. BI/livestreaming teams consume low-latency Apache Pinot serving outputs and use Trino for reconciled historical SQL.

Consumption serving planes:

The consumption layer has two serving planes. The real-time plane uses Flink to compute event-time-correct operational metrics and publishes them to Apache Pinot for low-latency dashboard queries over recent streaming data. The reconciled analytical plane uses Spark to build hourly Gold tables in the MinIO lakehouse, served through Trino as the canonical SQL interface. For coursework portability, an hourly DuckDB executive mart is also generated from Gold tables so executive KPI dashboards can run locally without a full multi-user warehouse.

The durable decision record lives in `architecture/decisions/2026-05-30-consumption-serving-planes.md`.

| Plane | Consumers | Compute path | Serving interface | Truth role |
| --- | --- | --- | --- | --- |
| Real-time operational serving | BI/livestreaming teams | Kafka source events -> Flink event-time metrics and alerts | Apache Pinot realtime OLAP serving sink | Fresh operational view, subject to later reconciliation |
| Reconciled analytical serving | Executive teams and historical BI | MinIO Bronze -> Spark Silver/Gold | Trino canonical SQL over Gold tables, plus DuckDB local KPI mart | Reconciled KPI truth |

Consumption-layer contract:

| Need | Serving path | Hive Metastore needed? |
| --- | --- | --- |
| Executive hourly KPIs | Spark writes Gold tables to MinIO; Trino serves canonical hourly SQL dashboards; DuckDB provides a local KPI mart generated from Gold | Yes for Trino table metadata; no for the DuckDB file export |
| BI live operations | Flink writes metrics, alerts, or dashboard-ready values to an Apache Pinot realtime OLAP serving sink | No |
| BI reconciled history | Flink/Spark writes curated tables to MinIO; Trino queries them | Yes |
| Data engineering inspection | Direct file or object inspection when debugging | Optional |

Hive Metastore is catalog metadata only: it records table names, schemas, partitions, and object-store locations. Consumers query Trino, and Trino uses Hive Metastore to find and interpret the curated files in MinIO.

Apache Pinot and DuckDB do not replace the lakehouse truth layer. Pinot serves recent operational metrics for low-latency dashboards, while DuckDB is a regenerated local mart for coursework-friendly executive KPI consumption. Reconciled business truth comes from Spark-produced Gold tables served through Trino.

Snapshot and event-log distinction:

| Concept | Parquet snapshots | JSONL event log |
| --- | --- | --- |
| Core question | What did the source tables look like at this checkpoint? | What happened, when, and in what order? |
| Data shape | Tabular state such as `orders`, `payments`, `shipments`, and `customers` | Event envelopes such as `order_placed`, `payment_failed`, and `shipment_delivered` |
| Landing path | Source systems export files into MinIO Bronze batch landing | Kafka messages are persisted into MinIO Bronze event landing |
| Format rationale | Parquet is compact, columnar, and efficient for Spark batch scans | JSONL preserves the raw message shape and remains readable/replayable |
| Main value | Batch truth, joins, reconciliation, reference state, and backfills | Real-time monitoring, event replay, sequence analysis, and timing analysis |

The overlap between `orders`, `payments`, and `shipments` snapshots and similarly named events is intentional: snapshots provide checkpointed system-of-record state for reconciliation, while events provide the immediate business timeline.

Beginner-friendly examples:

- Offline example: at `11:00`, source systems export hourly `orders`, `payments`, and `customers` Parquet snapshots into MinIO Bronze; Spark reads those landed files on the next batch run and writes cleaned Silver outputs.
- Streaming example: an `order_placed` event enters Kafka at `10:07`; Flink consumes it immediately for real-time revenue monitoring, and a Kafka sink also lands the raw event as JSONL into MinIO Bronze so Spark can replay it later during the hourly batch cycle.

### Required Timestamps and Event-Time Semantics

All generated datasets must define timestamps consistently:

- `event_timestamp`: the business event time
- `created_ts`: the row creation or emit time
- `ingest_ts`: the pipeline ingestion time, added later in Bronze

Rules:

- Offline entities use business timestamps such as `signup_ts`, `order_timestamp`, `payment_timestamp`, and `snapshot_ts`.
- Streaming events must preserve both `event_timestamp` and `created_ts` so late-arrival and dedup logic can be tested.
- Point-in-time joins later must use `event_timestamp`, not load order.

### Generator Controls

The generator should be controlled by external config under `configs/generator/`, including:

- random seed
- history window and start date
- row-volume parameters by entity
- category and geography skew ratios
- duplicate rates
- late-arrival rates and delays
- schema evolution cutoff dates
- output paths for offline and streaming assets

### Intentional Data Challenges

Sections `01` and `02` must be able to exercise realistic data engineering problems:

- skew in cities, sellers, or categories
- high-cardinality identifiers
- duplicate records in offline or streaming paths
- missing values in selected optional fields
- late and out-of-order streaming events
- schema evolution between older and newer partitions

### Drift-Ready Extension

The scaffolding should keep a placeholder for later drift scenarios in Section `03`. The first reserved scenario is a category-mix and order-frequency shift in FMCG behavior, but the actual demonstration is deferred to the later phase.

## 5. Section 02 Storage and Schema Design

### Local Implementation Stack

The architecture target for Sections `01` and `02` is:

- Python for source generation and orchestration
- Kafka for JSON event ingestion
- Spark for hourly batch processing
- Flink for event-time streaming processing
- MinIO for medallion lakehouse object storage
- Hive Metastore for table metadata
- Trino for SQL serving access
- Apache Pinot for realtime OLAP dashboard serving
- DuckDB for an hourly local executive KPI mart generated from Gold tables
- Parquet for offline persisted source snapshots
- JSON for in-Kafka event envelopes and JSONL for persisted, human-readable event-log examples

### Layering Model

The repository and logical storage follow a medallion pattern:

- `raw`: generator outputs and raw source payloads
- `bronze`: append-oriented ingestion with Kafka metadata, file lineage, and ingest timestamps
- `silver`: cleaned, standardized, deduplicated records from both Spark and Flink paths
- `gold`: business-ready dimensions, facts, OBTs, and feature tables designed in Section `02`

### Naming Conventions

- raw/source assets use source-oriented names such as `orders`, `payments`, and `kafka_topics/<topic>`
- Bronze tables use `raw_` prefixes when represented as tables or SQL models
- Silver tables use `stg_` prefixes
- Gold dimension tables use `dim_`
- Gold fact tables use `fact_`
- Gold denormalized serving tables use `obt_`
- Gold feature-ready outputs use `feat_`

### Serving Models for Gold

Planned Gold entities include:

- dimensions: `dim_customer`, `dim_seller`, `dim_product`, `dim_date`, `dim_payment_method`, `dim_order_status`
- facts: `fact_order`, `fact_order_item`, `fact_payment_attempt`, `fact_shipment`
- OBT: `obt_order_performance`
- feature tables: `feat_customer_90d`, `feat_stream_60m`, `feat_customer_unified`

Consumer-facing dashboards and extracts should use the appropriate serving plane. Executive and historical BI dashboards use curated Silver/Gold tables through Trino, with DuckDB generated from Gold as a local coursework mart. BI/livestreaming dashboards use Apache Pinot for low-latency operational metrics produced by Flink. Flink outputs that become durable analytical tables are registered through Hive Metastore and served through Trino; live metrics and alerts in Pinot bypass Hive Metastore because they are operational serving values, not lakehouse tables. Raw/Bronze files remain available for data engineering inspection, replay, and lineage checks, but they are not the normal interface for executive or livestreaming consumers.

### Update Policy

- Raw and Bronze are append-oriented.
- Silver is incrementally rebuilt or merged using stable business keys plus event-time logic.
- Gold facts and dimensions are updated via idempotent merges or replace-partition strategies suitable for lakehouse table workflows.
- Feature tables retain the latest `created_ts` for each entity and `event_timestamp` pair.

### Backfill Policy

- Default backfill scope is the last one day for reruns during coursework implementation.
- Full history reload is allowed only for explicit local rebuild workflows.
- Reprocessing should be idempotent and should not multiply duplicates.

### Point-in-Time Correctness

- Feature generation must use timestamps that would have been available at prediction time.
- Late-arriving events should update the affected rolling windows without leaking future information into historical states.

### Schema Evolution Rules

- New optional fields may appear after a cutoff date.
- Older partitions may legitimately miss those fields.
- Silver normalization should standardize missing columns and preserve evolution metadata where useful.

## 6. Data Quality and Operations

### Data Contracts

Every source and modeled dataset should explicitly define:

- grain
- business keys
- timestamp columns
- nullable versus required columns
- expected update behavior

### Required Quality Checks

- uniqueness checks for primary business keys where applicable
- duplicate-rate monitoring for intentionally noisy datasets
- nullability checks on required columns
- referential integrity from facts to dimensions
- freshness checks for generated outputs and pipeline layers
- volume anomaly checks against simple historical baselines

### SLA Targets for the Local Coursework Stack

- Raw/Bronze freshness for generated source files: within 10 minutes of a generator run
- Spark batch path freshness for executive teams: within 1 hour
- Flink streaming path freshness for BI/livestreaming teams: target under 30 seconds in local design
- Silver freshness: within 30 minutes for batch-derived tables, near real-time for stream-derived monitoring views
- Gold freshness: Section `02` will define final targets by serving table
- Feature freshness: between 5 and 60 minutes depending on the feature table
- Scheduled pipeline success target: at least 99 percent weekly in design intent

These are design targets, not cloud production guarantees.

### Failure Handling and Recovery

- malformed records should be quarantined or logged for inspection
- retries should be supported for transient local job failures
- reruns should be safe for the last processed window
- late arrivals should trigger reprocessing of the affected time windows

### Observability and Evidence

The implementation should later emit:

- structured logs for generator and pipeline jobs
- run metadata such as row counts, status, timing, and output paths
- simple metrics for duplicates, lateness, freshness, and null rates
- evidence artifacts under `evidence/01_data_generator/` and `evidence/02_schema_design/`

## 7. Security and Delivery Basics

### Secrets and Configuration

- Local environment variables live in `.env` and are never committed.
- Example non-secret values live in `.env.example`.
- Runtime configuration lives in `configs/`.

### RBAC Assumptions

The first phase is local-only, but the design should assume future role separation:

- developer/engineer can run generators and local pipelines
- analyst can query published Gold outputs
- platform owner can change configs, schemas, and release flows

### CI/CD Expectations

The implementation should later be easy to validate in CI using:

- `uv sync --frozen`
- `uv run pytest`
- `uv run` commands for generator smoke tests and pipeline smoke tests

### Reproducibility

The repo standard is:

- install and lock dependencies with `uv`
- run Python entry points with `uv run`
- keep documentation, configs, SQL, and sample evidence under version control

## 8. Implementation Roadmap

### First Populate for Section 01

Focus first on:

- `configs/generator/`
- `src/vina_bim_shop/domain/`
- `src/vina_bim_shop/generators/`
- `data/reference/taxonomy/`
- `deliverables/01_data_generator.md`
- `evidence/01_data_generator/`

### Next Populate for Section 02

Then expand:

- `configs/pipelines/`
- `sql/`
- `src/vina_bim_shop/pipelines/`
- `src/vina_bim_shop/quality/`
- `deliverables/02_schema_design.md`
- `evidence/02_schema_design/`

### Reserved Extensions

The following areas are intentionally scaffolded now for later work:

- `configs/scenarios/` and `src/vina_bim_shop/domain/scenarios/` for Section `03`
- `ai/ml/` for Section `04.1`
- `ai/llm/` for Section `04.2`

### Repo Ownership Model

- `architecture/` holds working design and PRD documents
- `deliverables/` holds polished coursework submission documents
- `sample_design/` remains reference-only
- `src/`, `sql/`, `configs/`, and `scripts/` hold implementation assets

## 9. Assumptions and Defaults

- Python `3.12` is the default runtime version.
- `uv` is the package manager of record.
- Large generated datasets should stay out of Git; only small samples and evidence should be committed.
- Section `01` remains source-contract-first; runnable Spark, Flink, Kafka, MinIO, Hive Metastore, Trino, Pinot, and DuckDB jobs are deferred to Section `02`.
- Spark and Flink references are architectural targets inspired by the EDAI transformation-layer projects, not copied wholesale.
- The simplified taxonomy is stable once committed unless the coursework requirements change.
