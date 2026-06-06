# Source Event Catalog

## Purpose

This catalog defines the implemented Kafka-shaped source event contracts for `vina-bim-shop`. The generator writes human-readable JSONL files that mirror Kafka topic messages, and the same events can optionally be published to Kafka with `--publish-kafka`.

Every normal source event is modeled as a JSON envelope. When persisted to local files or object storage, each envelope is written as one JSONL row so the event log remains readable and replayable.

## Common JSON Envelope

| Field | Required | Description |
| --- | --- | --- |
| `event_id` | yes | Globally unique event identifier before intentional duplicate scenarios. |
| `event_type` | yes | Event name inside the domain topic. |
| `event_topic` | yes | Kafka domain topic name. |
| `schema_version` | yes | Integer schema version used by generator evidence and Schema Registry contracts. |
| `event_timestamp` | yes | Business event time used for Flink watermarks, late-data handling, and point-in-time logic. |
| `created_ts` | yes | Producer emit time used to measure late arrivals and deduplicate repeated events. |
| `producer` | yes | Source producer name, currently `vina_bim_shop.synthetic_source`. |
| `correlation_ids` | yes | Context IDs such as `session_id`, `customer_id`, `order_id`, `payment_id`, `shipment_id`, or `product_id`. |
| `payload` | yes | Event-specific JSON object. |

## Source Topics

### `commerce_events`

Customer session, cart, checkout, order, cancellation, and payment events.

| Event | Meaning |
| --- | --- |
| `session_started` | A customer or anonymous visitor starts a browse session. |
| `search_performed` | A query or category search is submitted. |
| `product_viewed` | A product detail, search, or feed impression is viewed. |
| `add_to_cart` | A product is added to cart. |
| `remove_from_cart` | A product is removed from cart. |
| `checkout_started` | The user starts checkout. |
| `checkout_abandoned` | Checkout starts but no order is completed for that session. |
| `coupon_applied` | A coupon or promotion is applied during checkout. |
| `order_placed` | An order is submitted and correlated to offline order state. |
| `order_cancelled` | An order is cancelled or blocked after placement. |
| `payment_succeeded` | A payment attempt succeeds. |
| `payment_failed` | A payment attempt fails. |

### `catalog_events`

Catalog, price, inventory, and promotion source changes.

| Event | Meaning |
| --- | --- |
| `product_created` | A seller creates a product listing. |
| `product_updated` | A product listing receives an attribute or fulfillment update. |
| `price_changed` | A product price changes. |
| `inventory_snapshot` | A product stock snapshot is emitted. |
| `inventory_low_stock` | A stock level falls below the low-stock threshold. |
| `promotion_created` | A platform, seller, or mixed-funded promotion is created. |
| `promotion_activated` | A promotion becomes active. |

### `fulfillment_events`

Shipment lifecycle events.

| Event | Meaning |
| --- | --- |
| `shipment_created` | A shipment record is created after order placement. |
| `shipment_handoff` | The order is handed to a logistics provider. |
| `shipment_delayed` | Shipment is delayed after handoff. |
| `shipment_delivered` | Shipment reaches the customer. |
| `shipment_blocked_payment_failed` | Shipment is blocked because payment failed. |

### `ops_events`

Source observability and pipeline-readiness events.

| Event | Meaning |
| --- | --- |
| `source_heartbeat` | Synthetic source heartbeat for liveness checks. |
| `traffic_burst_detected` | The source observes lunch or evening traffic bursts. |
| `late_arrival_observed` | The source observes events emitted after business event time. |
| `duplicate_event_observed` | The source observes intentional duplicate event IDs. |
| `schema_version_changed` | The source records a schema evolution boundary. |

### `dead_letter_events`

`dead_letter_events` contains wrapper rows for malformed event examples. These rows are valid JSONL records that preserve invalid source payloads and error reasons without breaking normal readers.

Implemented examples include:

| Error reason | Purpose |
| --- | --- |
| `missing_required_key` | Demonstrates an event missing a required envelope field. |
| `invalid_json` | Demonstrates malformed source payload capture. |
| `invalid_timestamp` | Demonstrates timestamp contract failure handling. |
| `unknown_schema_version` | Demonstrates schema-version drift and quarantine handling. |

## Downstream Derived Topics

Flink consumes source topics and emits derived Kafka topics for realtime serving. These are not generator source topics; they are streaming outputs.

| Derived topic | Produced by | Consumed by |
| --- | --- | --- |
| `realtime_commerce_metrics_1m` | Flink commerce metrics job | Apache Pinot realtime commerce table and reconciliation evidence. |
| `realtime_ops_alerts` | Flink ops-alert job | Apache Pinot alert table and streaming audit evidence. |
| `realtime_metric_corrections` | Flink late-event correction path | Apache Pinot correction table and reconciliation evidence. |

## Freshness And Truth Policy

| Consumer | Path | Truth role |
| --- | --- | --- |
| BI/livestreaming teams | Kafka -> Flink -> derived Kafka topics -> Apache Pinot | Fresh provisional operations. |
| Executive teams | Bronze -> Spark Gold Iceberg -> Trino -> DuckDB Executive Mart snapshot | Reconciled historical truth. |
| Data engineering and governance | Kafka, MinIO/S3, Trino/Iceberg, dbt, Spark, Flink, GX -> DataHub | Metadata, lineage, and quality evidence. |
