# 01 Data Generator

## 1. Goal and Scope

`vina-bim-shop` is a Shopee-inspired Vietnamese e-commerce marketplace with many independent sellers, explicit customer behavior segments, Vietnam-specific geography, realistic product taxonomy, and intentionally injected data issues.

The Section `01` generator produces:

- offline source datasets as Parquet files under `data/raw/<dataset>/`
- one unified streaming event dataset as JSONL under `data/raw/stream_events/`
- evidence files under `evidence/01_data_generator/`
- a run manifest, quality report, issue manifest, schema summary, row counts, and sample rows

The generator is optimized for a balanced DE and AI foundation. It is realistic enough for downstream Bronze/Silver/Gold pipelines and structured enough for later ML/LLM experiments.

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

## 4. Streaming Dataset Design

The generator writes one unified JSONL stream: `data/raw/stream_events/stream_events.jsonl`.

Core event types:

- `view`
- `add_to_cart`
- `checkout_started`
- `order_placed`
- `payment_failed`

Streaming behavior included in v1:

- sessions are path-driven and can traverse browse, cart, checkout, order, and payment-failure states
- anonymous first events are allowed before later events attach a known `customer_id`
- `order_placed` events are derived from offline orders for consistency
- sessions can include repeated views and multi-product browsing
- abandoned checkout behavior is generated separately from successful orders

Core stream columns:

| Column | Purpose |
| --- | --- |
| `event_id` | globally unique event identifier before intentional duplicates |
| `event_type` | event family |
| `event_timestamp` | business event time |
| `created_ts` | event emit time; delayed only for late-arrival cases |
| `session_id` | links browse behavior to orders |
| `anonymous_id` | stable anonymous browser/app identity |
| `customer_id` | nullable for anonymous events |
| `product_id` | nullable by event type |
| `order_id` | populated for order/payment events |
| `device_type`, `source` | channel context |
| `is_burst_window`, `is_late_arrival` | generated quality/traffic indicators |

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

Evidence files:

- `evidence/01_data_generator/run_manifest.json`
- `evidence/01_data_generator/row_counts.csv`
- `evidence/01_data_generator/schema_summary.csv`
- `evidence/01_data_generator/quality_metrics.csv`
- `evidence/01_data_generator/issue_manifest.csv`
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

## 8. Implementation Notes

Implementation files:

- `src/vina_bim_shop/generators/config.py`: config and taxonomy loading
- `src/vina_bim_shop/generators/offline/generator.py`: offline source generation
- `src/vina_bim_shop/generators/streaming/generator.py`: unified stream generation
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
