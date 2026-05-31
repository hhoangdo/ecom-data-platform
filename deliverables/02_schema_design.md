# 02 Schema Design

## 1. Goal And Architecture Fit

Section `02` turns the Section `01` source contracts into a local, runnable schema design for `vina-bim-shop`.

The target architecture remains:

- Kafka JSON event envelopes for realtime ingestion.
- Flink for low-latency operational metrics.
- Apache Pinot for realtime dashboard serving.
- MinIO medallion storage with Spark for reconciled batch processing.
- Hive Metastore and Trino for canonical SQL over curated lakehouse tables.
- DuckDB as a local coursework mart generated from curated Gold outputs.

For local implementation, `dbt-DuckDB is the local execution and test harness`. This lets the coursework run schema transformations and data quality tests without standing up the full Spark/Trino/Pinot stack. The dbt models mirror the intended Bronze, Silver, and Gold contracts so the implementation stays aligned with the current Lambda architecture.

Spark, Flink, Apache Pinot, and Trino remain architectural target contracts in this local coursework phase. The runnable implementation for Section `02` is dbt-DuckDB, with evidence generated from the final medium Section `01` raw dataset.

### Central Source Rationale

`JSON event envelopes answer what happened now`. They preserve producer intent, event time, creation time, correlation IDs, and payload details. They are best for immediate operational visibility, replay, and timing analysis.

`Periodic table-state exports answer what state is reliable at checkpoint`. They are checkpointed source-of-record extracts for customers, products, orders, payments, shipments, and related entities. They are better for reconciliation because order status, payment status, shipment state, and financial totals can change after the first event is emitted.

This means overlap is intentional:

| Pair | Why both exist | Truth policy |
| --- | --- | --- |
| Event envelopes vs table-state exports | Events capture the timeline; snapshots capture reconciled state at an export checkpoint. | Batch snapshots override overlapping order, payment, and shipment events for official historical reporting. |
| Streaming path vs batch path | Streaming keeps simple metrics fresh; batch supports heavier joins, deduplication, business logic, and dimensional modeling. | Streaming is fresh and provisional; batch is reconciled truth. |
| Pinot vs historical SQL | Pinot serves low-latency operational metrics; Trino-served Gold tables provide governed history. | `Apache Pinot is fresh but provisional`; `Trino-served Gold tables are the canonical reconciled truth`. |

The older flat stream helper output is removed from the public contract. Kafka-topic-shaped JSONL files under `data/raw/kafka_topics/<topic>/events.jsonl` are the only streaming raw source for Section `02`.

## 2. Bronze And Silver Design

Bronze preserves source fidelity and adds ingestion metadata. Silver standardizes types, deduplicates records, flattens event envelopes, and keeps fields needed for late-arrival and schema-evolution reasoning.

| Source type | Bronze tables | Silver tables | Notes |
| --- | --- | --- | --- |
| Periodic table-state exports | `raw_customers`, `raw_sellers`, `raw_products`, `raw_product_category_map`, `raw_inventory_snapshots`, `raw_promotions`, `raw_orders`, `raw_order_items`, `raw_payments`, `raw_shipments` | matching `stg_` tables | Parquet snapshots are compact and efficient for batch joins and reconciliation. |
| Kafka event envelopes | `raw_kafka_commerce_events`, `raw_kafka_catalog_events`, `raw_kafka_fulfillment_events`, `raw_kafka_ops_events` | `stg_commerce_events`, `stg_catalog_events`, `stg_fulfillment_events`, `stg_ops_events` | Bronze preserves nested `correlation_ids`, `payload`, `schema_version`, `event_timestamp`, and `created_ts`; Silver flattens common fields. |
| Bad records | `raw_bad_events`, `raw_bad_snapshots`, Kafka DLQ topic `dead_letter_events` | inspection only | Generated malformed examples are quarantined instead of silently dropped. |

Silver deduplication uses stable business keys plus latest `created_ts`. Event Silver tables deduplicate by `event_id`, keeping the latest created copy. New optional columns from schema evolution remain nullable, and event `schema_version` is retained.

## 3. Gold Snowflake, OBT, And Serving Contracts

The reconciled analytical model is a snowflake/star hybrid. Normalized dimensions keep business entities reusable, fact tables preserve grains, and one OBT reduces join overhead for the most common executive BI workflow.

Gold DuckDB tables enforce primary-key and foreign-key constraints through dbt contracts so DBeaver can render physical ERD relationship lines from database metadata. Bronze and Silver remain views, but they are included in the physical model for lineage context. The physical data model is committed as `architecture/diagrams/physical_gold_model.puml` with a white-background rendered PNG at `architecture/diagrams/physical_gold_model.png`.

Physical key policy:

- Surrogate keys such as `customer_key`, `product_key`, and `order_key` are the main dimensional join path.
- Natural/source IDs such as `customer_id`, `product_id`, `order_id`, `payment_id`, `shipment_id`, and `snapshot_id` are retained for auditability and declared as alternate keys where they are unique.
- `dim_promotion` includes a single `NO_PROMOTION` sentinel row so `fact_order_item.promotion_key` is never null; source `promotion_id` remains nullable for non-promoted order items.
- Current v1 dimensions include SCD Type 2 support columns (`valid_from_ts`, `valid_to_ts`, `is_current`) but still contain one current row per natural entity.

### Dimensions And Bridge

| Table | Grain | Purpose |
| --- | --- | --- |
| `dim_customer` | one customer | Customer profile, segment, geography, acquisition context. |
| `dim_seller` | one seller | Seller tier, location, rating, and fulfillment traits. |
| `dim_product` | one product | Product attributes with seller link. |
| `dim_category` | one category and subcategory | Normalized taxonomy with `category_cost_rate`. |
| `bridge_product_category` | one product-category assignment | Supports many-to-many taxonomy assignments. |
| `dim_date` | one calendar date | Date filters and calendar grouping. |
| `dim_payment_method` | one payment method | Payment method normalization. |
| `dim_order_status` | one order status | Order lifecycle normalization. |
| `dim_shipment_status` | one shipment status | Fulfillment status normalization. |
| `dim_shipping_method` | one shipping method | Shipping method normalization, including `unknown`. |
| `dim_promotion` | one promotion | Funding type, discount rate, funding split, active period. |

### Facts

| Table | Grain | Core measures |
| --- | --- | --- |
| `fact_order` | one order | gross amount, discount, net amount, official paid revenue, GMV, payment attempt count. |
| `fact_order_item` | one order item | quantity, gross amount, discount, net amount, estimated cost, estimated margin. |
| `fact_payment_attempt` | one payment attempt | amount, success flag, failed flag, failure reason. |
| `fact_shipment` | one shipment | delayed flag, payment-blocked flag, handoff and estimated delivery timestamps. |
| `fact_inventory_snapshot` | one product snapshot | stock on hand, reserved stock, available stock. |
| `fact_promotion_application` | one promoted order item | discount amount, platform discount, seller discount. |

### OBT And Aggregates

`obt_order_performance` is one row per order. It joins order, customer, payment, shipment, item rollups, discount, revenue, estimated cost, and margin fields for executive BI. It intentionally avoids item-level grain so dashboard queries do not multiply orders.

`agg_hourly_reconciled_kpi` is the canonical hourly KPI aggregate built from Gold. It is the comparison target for Pinot realtime metrics.

### Feature Tables

| Table | Grain | Purpose |
| --- | --- | --- |
| `feat_customer_90d` | one customer at feature timestamp | Simple customer order and paid revenue history. |
| `feat_stream_60m` | one customer-hour | Simple streaming activity counters from commerce events. |
| `feat_customer_unified` | one customer at latest available feature timestamp | Joins offline and streaming features for later ML sections. |

Feature tables retain `event_timestamp` for point-in-time joins and `created_ts` for deduplication.

### Realtime Serving Contracts

Pinot artifacts are contracts only in v1, not runnable JSON configs:

| Pinot table | Grain | Source | Purpose |
| --- | --- | --- | --- |
| `pinot_realtime_commerce_metrics_1m` | one event-time minute by category/source/status | Flink from Kafka commerce events | Fresh revenue, GMV proxy, payment failures, checkout/order conversion. |
| `pinot_realtime_ops_alerts` | one alert event | Flink from Kafka ops and derived stream checks | Traffic bursts, late arrivals, duplicate spikes, anomaly-like operational alerts. |

Flink uses simple watermark updates for late events. It does not emit visible correction or retraction records in v1. Product and customer SCD joins stay out of the streaming path unless the needed fields are already present in the event payload.

DuckDB receives a small executive mart from Gold tables and aggregates. It is a local demo surface, not the canonical multi-user warehouse.

## 4. Business Logic Formulas

All official financial formulas are batch-reconciled formulas from Gold, not Pinot-only formulas.

| Metric | Formula | Notes |
| --- | --- | --- |
| Gross order amount | `sum(order_gross_amount)` | Pre-discount order amount. |
| Net order amount | `sum(order_net_amount)` | After discounts, before truth filtering. |
| Official paid revenue | `sum(order_net_amount where order_status = 'paid' and payment_status = 'success')` | This is the official revenue definition. |
| GMV | `sum(order_gross_amount where order_status = 'paid' and payment_status = 'success')` | Failed-payment and cancelled orders are excluded. |
| Discount amount | `sum(gross_amount - net_amount)` or source `discount_amount` | Uses order/item source totals after Silver deduplication. |
| Platform discount | `discount_amount * platform_funding_share` | Platform-funded uses 1.0, seller-funded uses 0.0, mixed uses `funding_detail` or 0.5 fallback. |
| Seller discount | `discount_amount * seller_funding_share` | Seller-funded uses 1.0, platform-funded uses 0.0, mixed uses `funding_detail` or 0.5 fallback. |
| AOV | `official_paid_revenue / paid_order_count` | Uses paid successful orders only. |
| Payment success rate | `successful_payment_attempts / payment_attempts` | Attempt-level payment metric. |
| Cancellation or failed-order rate | `payment_failed_order_count / order_count` | Current source has `payment_failed` as the failed order status. |
| Delivery delay rate | `delayed_shipments / shipments` | Uses shipment status `delayed`. |
| Conversion rate | `order_placed_events / checkout_started_events` | Streaming-derived behavior metric; reconciled batch can compare it hourly. |
| Estimated cost | `line_net_amount * category_cost_rate` | Category rates: FMCG `0.72`, ELHA `0.82`, Fashion `0.55`, Home & Living `0.62`. |
| Estimated margin | `official paid revenue - estimated cost` | Item-level margin is summed to order/hourly aggregates. |

These formulas are intentionally clear rather than over-complex. The project does not currently generate true cost of goods, returns, refunds, tax, or seller finance data.

## 5. Data Quality, Reconciliation, And Tests

dbt tests cover:

- uniqueness for business keys such as `order_id`, `order_item_id`, `payment_id`, `shipment_id`, and `event_id`.
- not-null checks on required keys and timestamps.
- accepted values for order, payment, and event topic fields.
- relationships from facts to dimensions.
- expression checks for non-negative revenue, cost, and margin fields.
- reconciliation between `stg_orders` and `fact_order`.
- reconciliation between item net totals and order net totals.

Operational quality rules:

- Bronze keeps raw records and reads generated malformed examples into `raw_bad_events`, `raw_bad_snapshots`, or Kafka `dead_letter_events`.
- Silver normalizes nullable schema-evolution fields and keeps `schema_version`.
- Gold facts and OBTs are rebuilt idempotently in local dbt-DuckDB. In the target Spark lakehouse, these would map to partition replacement or merge jobs.
- Pinot metrics are compared against `agg_hourly_reconciled_kpi` using hourly windows and a small tolerance for late events and event-time watermarking.
- When Pinot and Gold differ, Gold wins for official historical reporting.

## 6. Local Run And Evidence Boundary

The local Section `02` flow is:

```powershell
uv sync
uv run python scripts/generate/run_generator.py --scale smoke --mode full --clean
uv run dbt build --project-dir dbt --profiles-dir dbt
uv run pytest
```

The smoke generator run produces ignored raw files under `data/raw/` and Section `01` evidence. The dbt build writes a local DuckDB database to `data/gold/vina_bim_shop.duckdb`, which is ignored by Git.

Section `02` evidence is generated with:

```powershell
uv run python scripts/qa/generate_section02_evidence.py
```

The evidence package is written under `evidence/02_schema_design/` and includes:

- `evidence/02_schema_design/dbt_build_report.md`
- `evidence/02_schema_design/dbt_test_results.csv`
- `evidence/02_schema_design/dbt_model_results.csv`
- `evidence/02_schema_design/dbt_catalog_summary.csv`
- `evidence/02_schema_design/schema_inventory.csv`
- `evidence/02_schema_design/table_row_counts.csv`
- `evidence/02_schema_design/run_manifest.json`
- `evidence/02_schema_design/screenshots/schema_design.png`
- `evidence/02_schema_design/screenshots/gold_schema_inventory.png`
- `evidence/02_schema_design/screenshots/dbt_test_summary.png`
- `evidence/final_dataset/final_dataset_manifest.json`

The full dbt docs site in `dbt/target/` remains a transient local artifact and is not committed.
