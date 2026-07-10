# 01 Data Generator

## Purpose

The data generator is the source-system simulator for `vina-bim-shop`, a Shopee-inspired Vietnamese e-commerce marketplace. It creates the raw material used by every downstream subsystem: Kafka ingestion, lakehouse Bronze landing, Spark batch processing, Flink streaming, Pinot realtime serving, DuckDB local analysis, Airflow quality runs, and DataHub governance evidence.

The generator is intentionally realistic enough for data engineering coursework:

- marketplace entities such as customers, sellers, products, promotions, orders, payments, shipments, and inventory snapshots
- behavioral events for browse, cart, checkout, order, payment, catalog, fulfillment, and operations workflows
- Vietnam-specific geography and category mix
- deterministic evidence artifacts for repeatable submission review
- intentionally injected data challenges that exercise quality, deduplication, quarantine, schema evolution, and late-data handling

## Why The Project Needs It

The rest of the platform cannot be meaningfully evaluated with toy rows only. A data platform needs source data that has:

| Need | Generator responsibility |
| --- | --- |
| Batch truth | Produce periodic table-state exports that represent checkpointed source-of-record state. |
| Realtime analysis | Produce Kafka-shaped event envelopes that represent business activity as it happens. |
| Quality handling | Include duplicates, missing values, late arrivals, malformed records, and schema drift. |
| Reproducibility | Use deterministic seeds and committed evidence so results can be regenerated locally. |
| Explainability | Emit manifests, row counts, samples, issue metrics, and quality reports. |

The generator therefore creates two source streams: offline snapshots for reconciled batch processing and event envelopes for realtime processing. That split is the foundation for the platform's Lambda architecture.

## Outputs

The generator is controlled by [configs/generator/base.yaml](../configs/generator/base.yaml) and executed through [scripts/generate/run_generator.py](../scripts/generate/run_generator.py).

```powershell
uv run python scripts/generate/run_generator.py --scale medium --mode full --clean --seed 42
```

Common options:

| Option | Purpose |
| --- | --- |
| `--scale smoke` | Generate a small, fast dataset for smoke tests and local contract checks. |
| `--scale medium` | Generate the submitted evidence-scale dataset used by the final coursework package. |
| `--scale coursework` | Generate the largest configured local coursework profile. |
| `--mode offline` | Write only Parquet table-state snapshots and bad snapshot examples. |
| `--mode streaming` | Write only Kafka-shaped JSONL topic events and dead-letter examples. |
| `--mode full` | Write both offline snapshots and streaming topic files. |
| `--seed <int>` | Override the deterministic random seed. |
| `--clean` | Remove generator-managed outputs before writing new data. |
| `--raw-root <path>` | Override the local raw output directory. |
| `--evidence-root <path>` | Override the evidence output directory. |
| `--publish-kafka` | Publish generated topic events to Kafka after writing JSONL files. |

Generated output families:

| Output | Location | Used by |
| --- | --- | --- |
| Offline snapshots | `data/raw/<dataset>/` | dbt-DuckDB, Spark Bronze reads, final raw dataset package. |
| Kafka-shaped JSONL events | `data/raw/kafka_topics/<topic>/events.jsonl` | Kafka smoke publish, Bronze event landing, Flink source contracts. |
| Bad snapshot examples | `data/raw/bad_snapshots/bad_snapshots.jsonl` | Quarantine models and quality evidence. |
| Evidence artifacts | `evidence/01_data_generator/` | Submission evidence, README summaries, schema design evidence. |
| Final dataset package | `evidence/final_dataset/` | Submitted raw dataset zip and manifest. |

## Offline Snapshot Design

Offline snapshots model checkpointed table-state exports. They answer: what did the operational systems believe the current state was at the export checkpoint?

| Dataset | Grain | Format | Key columns | Downstream role |
| --- | --- | --- | --- | --- |
| `customers` | one row per customer | Parquet | `customer_id`, `anonymous_id`, `signup_ts`, `segment`, `city` | Customer dimension and segmentation. |
| `sellers` | one row per seller | Parquet | `seller_id`, `seller_tier`, `primary_category`, `seller_rating` | Seller dimension and product ownership. |
| `products` | one row per product | Parquet | `product_id`, `seller_id`, `primary_category`, `brand` | Product dimension and category analysis. |
| `product_category_map` | one product-category assignment | Parquet | `product_id`, `category`, `subcategory`, `is_primary` | Many-to-many taxonomy bridge. |
| `inventory_snapshots` | one product inventory snapshot | Parquet | `snapshot_id`, `product_id`, `snapshot_ts`, `stock_on_hand` | Inventory fact table. |
| `promotions` | one promotion | Parquet | `promotion_id`, `funding_type`, `discount_rate` | Promotion dimension and discount attribution. |
| `orders` | one order header | Parquet | `order_id`, `customer_id`, `order_timestamp`, `status` | Reconciled order fact. |
| `order_items` | one order line | Parquet | `order_item_id`, `order_id`, `product_id`, `net_amount` | Order item fact and revenue/cost logic. |
| `payments` | one payment attempt | Parquet | `payment_id`, `order_id`, `payment_status` | Payment attempt fact and success-rate metrics. |
| `shipments` | one shipment | Parquet | `shipment_id`, `order_id`, `shipment_status`, `handoff_ts` | Shipment fact and delay metrics. |

Parquet is used for snapshots because the data is columnar, compact, and efficient for Spark/dbt batch reads. These snapshots become Bronze batch inputs before being standardized into Silver and modeled into Gold.

## Streaming Source Design

The streaming output is a local file representation of Kafka topic messages. It lets the project test event contracts before and after a real Kafka broker is running.

| Topic | Event types | Downstream consumers |
| --- | --- | --- |
| `commerce_events` | session, search, view, cart, checkout, order, cancellation, payment events | Kafka, Flink commerce metrics, Spark event Silver, Pinot via derived topics. |
| `catalog_events` | product, price, inventory, and promotion changes | Kafka, Flink ops alerts, Spark event Silver. |
| `fulfillment_events` | shipment creation, handoff, delay, delivery, payment block | Kafka, Flink ops alerts, Spark event Silver. |
| `ops_events` | source heartbeat, traffic burst, late arrival, duplicate observation, schema version change | Kafka, Flink alert path, operations evidence. |
| `dead_letter_events` | wrapper records for malformed event examples | Kafka DLQ contract and Bronze quarantine. |

All normal events use a common envelope:

| Field | Type | Purpose |
| --- | --- | --- |
| `event_id` | string | Business event identifier and deduplication key. |
| `event_type` | string | Event name inside the topic. |
| `event_topic` | string | Topic contract name. |
| `schema_version` | integer | Event schema version used by Schema Registry and drift evidence. |
| `event_timestamp` | timestamp string | Business event time used by Flink watermarks and batch windows. |
| `created_ts` | timestamp string | Source emit time used for late-arrival detection and deduplication. |
| `producer` | string | Source producer name. |
| `correlation_ids` | JSON object | Related IDs such as session, customer, product, order, payment, or shipment. |
| `payload` | JSON object | Event-specific fields. |

## Data Challenges Injected

The generator intentionally mixes realistic data problems into the outputs. These are not accidental defects; they are designed scenarios so the platform can prove how it handles imperfect data.

The observed medium-run issues are recorded in [evidence/01_data_generator/issue_manifest.csv](../evidence/01_data_generator/issue_manifest.csv) and summarized in [evidence/01_data_generator/quality_report.md](../evidence/01_data_generator/quality_report.md).

The rubric-facing configuration and observation summary is [rubric_evidence_summary.md](../evidence/01_data_generator/rubric_evidence_summary.md). It keeps configured controls separate from measurements produced by the selected generator run.

| Challenge | Example | Why it matters downstream |
| --- | --- | --- |
| Geographic skew | Ho Chi Minh City and Ha Noi represent about 45% of customers. | Tests whether BI summaries and customer dimensions reflect realistic urban concentration. |
| Category skew | FMCG and ELHA dominate the product catalog. | Creates realistic demand imbalance for product, inventory, and margin analysis. |
| Offline duplicate payloads | `order_items` includes exact duplicate order-line payloads, observed at `0.01959` in the medium evidence run. | Silver must deduplicate by stable business keys before Gold revenue and margin formulas are trusted. |
| Streaming duplicate events | `commerce_events` includes exact duplicate event envelopes, observed at `0.01475`. | Flink and Spark event models must avoid double-counting live metrics and conversion events. |
| Late arrivals | Some commerce events have `created_ts` delayed 5 to 45 minutes after `event_timestamp`, observed at `0.12536`. | Flink must use event time and allowed lateness; batch reconciliation must remain the final truth. |
| Missing shipping method | `orders.shipping_method` is intentionally nullable, observed at `0.02889`. | Silver/Gold must normalize unknown shipping values instead of dropping orders. |
| Missing brand | `products.brand` is intentionally nullable, observed at `0.02833`. | Product dimensions must preserve unknown brands for realistic catalog data. |
| Missing device metadata | `commerce_events.payload.device_type` can be missing, observed at `0.03921`. | Streaming metrics must tolerate nullable dimensions. |
| Schema evolution | Older product slices miss newer `category_attributes`; observed `schema_evolution_category_attributes` rate is `0.425`. | Silver must keep optional fields nullable and retain `schema_version`. |
| Bad event payloads | `dead_letter_events` includes `missing_required_key`, `invalid_json`, `invalid_timestamp`, and `unknown_schema_version`. | Kafka/DLQ and Bronze quarantine paths can be demonstrated without corrupting normal readers. |
| Bad snapshot payloads | `bad_snapshots` includes missing keys, broken JSON, invalid timestamps, and unknown schema versions. | Batch quarantine models can explain malformed source extracts separately from valid snapshots. |
| Operational signals | `ops_events` emits `traffic_burst_detected`, `late_arrival_observed`, `duplicate_event_observed`, and `schema_version_changed`. | Realtime alerting and governance evidence have explicit source observability events. |

## Rubric Cardinality Evidence

[cardinality_summary.csv](../evidence/01_data_generator/cardinality_summary.csv) is generated with DuckDB `approx_count_distinct` for evidence-only rubric support. It does not affect source generation, duplicate injection, or downstream business-key deduplication.

| Entity | Identifier | Reported evidence |
| --- | --- | --- |
| Customers | `customer_id` | Row count, approximate distinct count, and uniqueness ratio. |
| Products | `product_id` | Row count, approximate distinct count, and uniqueness ratio. |
| Orders | `order_id` | Row count, approximate distinct count, and uniqueness ratio. |
| Topic events | `event_id` | Row count, approximate distinct count, and uniqueness ratio. |

Concrete examples from committed sample rows:

| Example artifact | Example value |
| --- | --- |
| DLQ missing key | `DLQ-EVENT-0001` wraps a commerce event without `event_id`. |
| DLQ invalid JSON | `DLQ-EVENT-0002` contains a broken JSON fragment for `order_placed`. |
| DLQ invalid timestamp | `DLQ-EVENT-0003` uses `event_timestamp = not-a-timestamp`. |
| DLQ unknown schema | `DLQ-EVENT-0004` uses `schema_version = 99`. |
| Bad snapshot missing key | `BAD-SNAPSHOT-0001` is an order-like record missing `order_id`. |
| Bad snapshot invalid JSON | `BAD-SNAPSHOT-0002` is a broken payment record. |
| Bad snapshot invalid timestamp | `BAD-SNAPSHOT-0003` uses `handoff_ts = tomorrow-ish`. |
| Ops late-arrival signal | `late_arrival_observed` reports `late_event_count = 1589`. |
| Ops traffic burst signal | `traffic_burst_detected` reports lunch and evening burst windows. |

Malformed records are wrapped as valid JSONL rows so local file readers remain stable. They are routed into quarantine contracts and excluded from normal Bronze, Silver, and Gold business tables.

## Final Medium Evidence

Final evidence baseline:

```powershell
uv run python scripts/generate/run_generator.py --scale medium --mode full --clean --seed 42
```

Generated row counts:

| Dataset | Rows |
| --- | ---: |
| `bad_snapshots` | 4 |
| `customers` | 12,000 |
| `sellers` | 600 |
| `products` | 6,000 |
| `product_category_map` | 7,340 |
| `inventory_snapshots` | 60,000 |
| `promotions` | 80 |
| `orders` | 45,000 |
| `order_items` | 168,496 |
| `payments` | 45,000 |
| `shipments` | 45,000 |
| `kafka_topics` | 639,541 |

Kafka topic row counts:

| Topic | Rows |
| --- | ---: |
| `catalog_events` | 69,160 |
| `commerce_events` | 442,451 |
| `dead_letter_events` | 4 |
| `fulfillment_events` | 127,921 |
| `ops_events` | 5 |

Key evidence files:

- [run_manifest.json](../evidence/01_data_generator/run_manifest.json)
- [row_counts.csv](../evidence/01_data_generator/row_counts.csv)
- [quality_metrics.csv](../evidence/01_data_generator/quality_metrics.csv)
- [issue_manifest.csv](../evidence/01_data_generator/issue_manifest.csv)
- [event_topic_row_counts.csv](../evidence/01_data_generator/event_topic_row_counts.csv)
- [schema_version_summary.csv](../evidence/01_data_generator/schema_version_summary.csv)
- [cardinality_summary.csv](../evidence/01_data_generator/cardinality_summary.csv)
- [rubric_evidence_summary.md](../evidence/01_data_generator/rubric_evidence_summary.md)
- [quality_report.md](../evidence/01_data_generator/quality_report.md)
- [final dataset manifest](../evidence/final_dataset/final_dataset_manifest.json)
- [final medium raw dataset zip](../evidence/final_dataset/vina_bim_shop_medium_raw.zip)

## Implementation Files

| File | Responsibility |
| --- | --- |
| [src/vina_bim_shop/generators/config.py](../src/vina_bim_shop/generators/config.py) | Loads generator config, scale profiles, taxonomy, and output roots. |
| [src/vina_bim_shop/generators/offline/generator.py](../src/vina_bim_shop/generators/offline/generator.py) | Builds offline marketplace snapshots and offline issue records. |
| [src/vina_bim_shop/generators/streaming/generator.py](../src/vina_bim_shop/generators/streaming/generator.py) | Builds Kafka-shaped event streams and operational events. |
| [src/vina_bim_shop/generators/runner.py](../src/vina_bim_shop/generators/runner.py) | Coordinates output writing, DLQ examples, bad snapshots, and modes. |
| [src/vina_bim_shop/generators/evidence.py](../src/vina_bim_shop/generators/evidence.py) | Writes manifests, metrics, sample rows, and quality reports. |
| [scripts/generate/run_generator.py](../scripts/generate/run_generator.py) | CLI entrypoint. |

## Service Interactions

| Service | Relationship |
| --- | --- |
| Kafka | Generator output can be published to Kafka topics and validated against JSON Schemas. |
| Kafka Connect | Source-topic events can be landed from Kafka into MinIO Bronze. |
| MinIO lakehouse | Batch snapshots and event replay logs become Bronze objects. |
| Spark | Reads Bronze snapshots/events and builds Silver/Gold. |
| Flink | Consumes live Kafka topics built from the same event contract. |
| Pinot | Receives Flink-derived topics, not raw generator topics. |
| DuckDB/dbt | Rebuilds a local parity model from generated raw files. |
| Airflow/GX | Orchestrates evidence runs and validates Bronze/Gold expectations. |
| DataHub | Catalogs generated topics, lakehouse tables, lineage, tags, and quality assertions. |

## Data Challenge Handling Overview

Every challenge listed in this document is intentionally designed as a test case for the
batch and streaming engines. The map of "challenge -> Spark/Flink handling" lives in
[11 Solving Data Challenges](11_solving_data_challenges.md). It points at the actual code
paths in `src/vina_bim_shop/lakehouse/spark/` and `src/vina_bim_shop/flink/`, and it
documents honest gaps (for example, that no Spark salting is implemented today).

## Convenience Make Targets

The root [Makefile](../Makefile) wraps the most common commands for this deliverable under a `make + verb` convention. It delegates to the official scripts in [scripts/](../scripts/) and to `scripts/ctl.py` for Docker Compose lifecycle; compose targets expand documented profile bundles and prebuild shared images where needed.

| Target | What it does |
| --- | --- |
| `make install` | `uv sync` to install Python dependencies. |
| `make generate` | Run the data generator with default options (`SCALE=medium`, `MODE=full`, `SEED=42`). Override with `make generate SCALE=smoke MODE=streaming SEED=7`. |
