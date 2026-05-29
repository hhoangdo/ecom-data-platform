# Source Event Catalog

## Purpose

This catalog defines the Section `01` Kafka-shaped source event contracts for `vina-bim-shop`.
The generator writes human-readable JSONL files that mirror Kafka domain topics; a real Kafka
producer is deferred until the Section `02` pipeline implementation.

## Common JSON Envelope

Every source event uses the same envelope:

| Field | Required | Description |
| --- | --- | --- |
| `event_id` | yes | Globally unique event identifier before intentional duplicates. |
| `event_type` | yes | Event name inside the domain topic. |
| `event_topic` | yes | Kafka domain topic name. |
| `schema_version` | yes | Integer schema version from `configs/generator/base.yaml`. |
| `event_timestamp` | yes | Business event time used for watermarks and point-in-time logic. |
| `created_ts` | yes | Producer emit time; always greater than or equal to `event_timestamp`. |
| `producer` | yes | Source producer name, currently `vina_bim_shop.synthetic_source`. |
| `correlation_ids` | yes | Context IDs such as `session_id`, `customer_id`, `order_id`, or `product_id`. |
| `payload` | yes | Event-specific JSON object. |

## Kafka Topics

### `commerce_events`

Customer session, cart, checkout, order, and payment events.

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
| `order_placed` | An order is submitted; generated from the offline `orders` source for consistency. |
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

## Consumer Freshness Targets

| Consumer | Path | Target Freshness |
| --- | --- | --- |
| Executive teams | Spark batch path | 1 hour |
| BI/livestreaming teams | Flink streaming path | real-time, target under 30 seconds in local design |

## Section Boundary

Section `01` owns synthetic source contracts, topic-shaped JSONL outputs, and evidence.
Section `02` will own the runnable Kafka, Spark, Flink, MinIO, Hive Metastore, Trino, and
Bronze/Silver/Gold pipeline implementation details.
