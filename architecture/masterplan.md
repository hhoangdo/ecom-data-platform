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
- Bronze, Silver, Gold, and feature-serving expectations are documented for local implementation.
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

The streaming generator should produce JSON event payloads for session and commerce behavior.

Recommended event families:

- `view`
- `add_to_cart`
- `checkout_started`
- `order_placed`
- `payment_failed`

Additional events are allowed later if they directly support conversion analysis or operational monitoring.

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

The implementation target for this phase is:

- Python for generation and orchestration
- DuckDB for local analytical storage and transformation
- Parquet for offline persisted datasets
- JSON or JSONL-style event payloads for streaming examples

### Layering Model

The repository and logical storage follow a medallion pattern:

- `raw`: generator outputs and raw source payloads
- `bronze`: append-oriented ingestion with ingest metadata
- `silver`: cleaned, standardized, deduplicated records
- `gold`: business-ready dimensions, facts, OBTs, and feature tables

### Naming Conventions

- raw/source assets use source-oriented names such as `orders`, `payments`, `stream_events`
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

### Update Policy

- Raw and Bronze are append-oriented.
- Silver is incrementally rebuilt or merged using stable business keys plus event-time logic.
- Gold facts and dimensions are updated via idempotent merges or replace-partition strategies suitable for local DuckDB workflows.
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

- Raw/Bronze freshness: within 10 minutes of a generator run
- Silver freshness: within 30 minutes
- Gold freshness: within 30 minutes
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
- The project remains local-first for Sections `01` and `02`; Spark, dbt, and Airflow are not required in this phase.
- The simplified taxonomy is stable once committed unless the coursework requirements change.
