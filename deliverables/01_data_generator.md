# 01 Data Generator

## 1. Goal and Scope

`vina-bim-shop` is a Shopee-inspired Vietnamese e-commerce marketplace with many independent sellers, explicit customer behavior segments, Vietnam-specific geography, realistic product taxonomy, and intentionally injected data issues.

The Section `01` generator produces:

- offline source datasets as Parquet files under `data/raw/<dataset>/`
- one legacy flat streaming event dataset as JSONL under `data/raw/stream_events/`
- Kafka-topic-shaped JSONL events under `data/raw/kafka_topics/<topic>/`
- evidence files under `evidence/01_data_generator/`
- a run manifest, quality report, issue manifest, schema summary, row counts, event-topic summaries, and sample rows

The generator is optimized for a balanced DE and AI foundation. It is realistic enough for downstream Bronze/Silver/Gold pipelines and structured enough for later ML/LLM experiments.

Section `01` now explicitly follows a Lambda architecture source design. Kafka is the ingestion layer, Spark is the hourly batch path for executive teams, Flink is the real-time path for BI/livestreaming teams, MinIO stores lakehouse files, Hive Metastore provides catalog metadata, and Trino is the SQL serving layer. Runnable Spark/Flink jobs and final Gold schemas are intentionally deferred to Section `02`.

## 2. How to Run

The generator is controlled by `configs/generator/base.yaml` and executed through `scripts/generate/run_generator.py`.

```powershell
uv run python scripts/generate/run_generator.py --scale smoke --mode full --clean
```

Common options:

- `--scale smoke|medium|coursework`: choose the configured generation profile
- `--mode offline|streaming|full`: choose which source paths to write
- `--seed <int>`: override the deterministic seed
- `--clean`: remove generator-managed outputs before writing
- `--raw-root <path>` and `--evidence-root <path>`: override output roots for tests or experiments

The default implementation profile is `medium`, with `smoke` used for fast verification and `coursework` reserved for larger evidence runs.

## 3. Offline Dataset Design

| Dataset | Grain | Format | Key Columns |
| --- | --- | --- | --- |
| `customers` | one row per customer | Parquet | `customer_id`, `anonymous_id`, `signup_ts`, `segment`, `city` |
| `sellers` | one row per seller | Parquet | `seller_id`, `seller_tier`, `primary_category`, `seller_rating` |
| `products` | one row per product | Parquet | `product_id`, `seller_id`, `primary_category`, `primary_subcategory`, `brand` |
| `product_category_map` | one row per product-category assignment | Parquet | `product_id`, `category`, `subcategory`, `is_primary` |
| `inventory_snapshots` | one row per product snapshot | Parquet | `snapshot_id`, `product_id`, `snapshot_ts`, `stock_on_hand` |
| `promotions` | one row per promotion | Parquet | `promotion_id`, `funding_type`, `seller_id`, `category`, `discount_rate` |
| `orders` | one row per order | Parquet | `order_id`, `customer_id`, `session_id`, `order_timestamp`, `status` |
| `order_items` | one row per order line | Parquet | `order_item_id`, `order_id`, `product_id`, `quantity`, `net_amount` |
| `payments` | one row per payment attempt | Parquet | `payment_id`, `order_id`, `payment_timestamp`, `payment_status` |
| `shipments` | one row per shipment | Parquet | `shipment_id`, `order_id`, `shipment_status`, `handoff_ts` |

Marketplace realism included in v1:

- sellers have tier, rating, fulfillment speed, inventory reliability, and price-band traits
- customers belong to behavioral segments such as budget shoppers, loyal FMCG repeat buyers, high-value ELHA buyers, fashion browsers, and home improvers
- geography uses Vietnamese cities and regions with intentional HCMC/Ha Noi skew
- product counts skew toward `FMCG` and `ELHA`
- products include reusable category-affine brands and category-specific optional attributes
- promotions can be platform-funded, seller-funded, or mixed-funded and are applied to orders/items
- `product_category_map` supports many-to-many assignments from v1
- inventory snapshots are occasional, lightweight stock snapshots

## 4. Lambda Streaming Source Design

The generator writes two streaming-friendly raw outputs:

- `data/raw/stream_events/stream_events.jsonl`: a flat convenience stream for fast local analysis and continuity with the first implementation.
- `data/raw/kafka_topics/<topic>/events.jsonl`: the authoritative Kafka-topic-shaped source contract for Lambda architecture.

Kafka topic outputs use domain topics rather than one topic per event:

| Topic | Generated Events |
| --- | --- |
| `commerce_events` | `session_started`, `search_performed`, `product_viewed`, `add_to_cart`, `remove_from_cart`, `checkout_started`, `checkout_abandoned`, `coupon_applied`, `order_placed`, `order_cancelled`, `payment_succeeded`, `payment_failed` |
| `catalog_events` | `product_created`, `product_updated`, `price_changed`, `inventory_snapshot`, `inventory_low_stock`, `promotion_created`, `promotion_activated` |
| `fulfillment_events` | `shipment_created`, `shipment_handoff`, `shipment_delayed`, `shipment_delivered`, `shipment_blocked_payment_failed` |
| `ops_events` | `source_heartbeat`, `traffic_burst_detected`, `late_arrival_observed`, `duplicate_event_observed`, `schema_version_changed` |

All Kafka-shaped events use this common JSON envelope:

| Field | Purpose |
| --- | --- |
| `event_id` | globally unique event identifier before intentional duplicates |
| `event_type` | event name within the domain topic |
| `event_topic` | Kafka topic name |
| `schema_version` | configured topic schema version |
| `event_timestamp` | business event time for watermarks and point-in-time logic |
| `created_ts` | source emit time; always greater than or equal to `event_timestamp` |
| `producer` | source producer name |
| `correlation_ids` | IDs such as session, customer, product, order, payment, or shipment |
| `payload` | event-specific JSON object |

Streaming behavior included in v1:

- sessions are path-driven and can traverse browse, cart, checkout, order, payment, cancellation, and abandonment states
- anonymous first events are allowed before later events attach a known `customer_id`
- `order_placed` events are derived from offline orders for consistency
- payment success/failure and shipment events are derived from offline source tables
- sessions can include repeated views, multi-product browsing, search, cart removal, and coupon application
- Kafka payloads remain JSON for human readability and easier coursework inspection

The source event catalog is maintained in `architecture/domain/source-event-catalog.md`.

## 5. Data Challenges Injected

The generator intentionally mixes known problems into the primary outputs and records them in `evidence/01_data_generator/issue_manifest.csv`.

| Challenge | Implementation |
| --- | --- |
| Skew | HCMC/Ha Noi city skew and FMCG/ELHA catalog skew |
| Duplicates | exact duplicate order-item payloads and exact duplicate stream-event payloads |
| Late arrivals | configured portion of stream events have delayed `created_ts` |
| Missing values | intentional missingness in `shipping_method`, `brand`, and `device_type` |
| Schema evolution | older marketplace slices miss newer fields such as fulfillment channel, promotion funding detail, device metadata, and category attributes |
| Burst traffic | stream events mark lunch and evening burst windows |

Lambda-specific generated evidence also records Kafka topic row counts and schema-version counts so Section `02` can validate ingestion coverage before building Bronze/Silver tables.

## 6. Latest Smoke Evidence

Latest command:

```powershell
uv run python scripts/generate/run_generator.py --scale smoke --mode full --clean
```

Generated row counts:

| Dataset | Rows |
| --- | ---: |
| `customers` | 800 |
| `sellers` | 80 |
| `products` | 600 |
| `product_category_map` | 729 |
| `inventory_snapshots` | 1,200 |
| `promotions` | 16 |
| `orders` | 1,800 |
| `order_items` | 6,891 |
| `payments` | 1,800 |
| `shipments` | 1,800 |
| `stream_events` | 12,676 |
| `kafka_topics` | 24,855 |

Selected quality metrics:

| Metric | Observed Value |
| --- | ---: |
| `hcmc_hanoi_customer_share` | 0.45125 |
| `fmcg_elha_product_share` | 0.66 |
| `offline_order_item_duplicate_rate` | 0.01959 |
| `stream_late_arrival_rate` | 0.12536 |
| `stream_event_duplicate_id_rate` | 0.01475 |
| `stream_missing_device_type_rate` | 0.03921 |
| `stream_burst_event_count` | 687 |

Kafka topic row counts:

| Topic | Rows |
| --- | ---: |
| `catalog_events` | 1,940 |
| `commerce_events` | 17,810 |
| `fulfillment_events` | 5,100 |
| `ops_events` | 5 |

Evidence files:

- `evidence/01_data_generator/run_manifest.json`
- `evidence/01_data_generator/row_counts.csv`
- `evidence/01_data_generator/schema_summary.csv`
- `evidence/01_data_generator/quality_metrics.csv`
- `evidence/01_data_generator/issue_manifest.csv`
- `evidence/01_data_generator/event_topic_row_counts.csv`
- `evidence/01_data_generator/schema_version_summary.csv`
- `evidence/01_data_generator/sample_rows/*.csv`
- `evidence/01_data_generator/quality_report.md`

## 7. Sample Rows

Sample `orders` row:

| order_id | customer_id | session_id | primary_category | status | shipping_city | promotion_id | order_net_amount |
| --- | --- | --- | --- | --- | --- | --- | ---: |
| `ORD-HCM-20260429-00000001` | `CUS-HCM-00000751` | `SES-HCM-20260429-00000001` | `Fashion` | `paid` | `Ho Chi Minh City` | `PRM-FASHION-00000005` | 511064.0 |

Sample `products` row:

| product_id | seller_id | primary_category | primary_subcategory | brand | fulfillment_channel |
| --- | --- | --- | --- | --- | --- |
| `PRD-FASHION-00000003` | `SEL-HCM-STA-00000051` | `Fashion` | `Men's Fashion` | `DenimSaigon` | `platform_fulfilled` |

Sample `stream_events` row:

| event_id | event_type | session_id | customer_id | product_id | event_timestamp | is_late_arrival |
| --- | --- | --- | --- | --- | --- | --- |
| `EVT-VIE-20260425-00000001` | `view` | `SES-HUI-20260426-00001107` | `CUS-HUI-00000241` | `PRD-FASHION-00000294` | `2026-04-25 22:53:05` | `False` |

Sample Kafka-shaped `commerce_events` row:

| event_id | event_type | event_topic | schema_version | producer |
| --- | --- | --- | ---: | --- |
| `KEVT-COM-SEARCH-PERFORMED-20260425-0000000001` | `search_performed` | `commerce_events` | 1 | `vina_bim_shop.synthetic_source` |

## 8. Implementation Notes

Implementation files:

- `src/vina_bim_shop/generators/config.py`: config and taxonomy loading
- `src/vina_bim_shop/generators/offline/generator.py`: offline source generation
- `src/vina_bim_shop/generators/streaming/generator.py`: flat stream plus Kafka-topic-shaped source event generation
- `src/vina_bim_shop/generators/evidence.py`: manifest, metrics, issue, and sample-row evidence
- `src/vina_bim_shop/generators/runner.py`: orchestration and output writing
- `scripts/generate/run_generator.py`: CLI entrypoint

Acceptance tests cover:

- config scale overrides and taxonomy loading
- required dataset contracts
- causal payment/order timestamp ordering
- category and geography skew
- duplicate, missingness, late-arrival, and burst-event behavior
- CLI execution and evidence generation
- Kafka topic output existence, common envelope fields, and event-topic evidence summaries
- Lambda architecture diagram artifact presence
