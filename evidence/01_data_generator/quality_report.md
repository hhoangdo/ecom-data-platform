# Section 01 Data Generator Quality Report

## Run Context

- Platform: `vina-bim-shop`
- Scale: `smoke`
- History days: `7`
- Seed: `42`

## Row Counts

- `bad_snapshots`: 4 rows
- `customers`: 800 rows
- `inventory_snapshots`: 1,200 rows
- `order_items`: 6,891 rows
- `orders`: 1,800 rows
- `payments`: 1,800 rows
- `product_category_map`: 729 rows
- `products`: 600 rows
- `promotions`: 16 rows
- `sellers`: 80 rows
- `shipments`: 1,800 rows
- `kafka_topics`: 24,859 rows

## Quality Metrics

- `hcmc_hanoi_customer_share`: 0.45125
- `fmcg_elha_product_share`: 0.66
- `missing_brand_rate`: 0.02833
- `missing_shipping_method_rate`: 0.02889
- `order_session_link_rate`: 1.0
- `offline_order_item_duplicate_rate`: 0.01959
- `issue_order_items_exact_duplicate_payload`: 0.01959
- `issue_products_missing_brand`: 0.02833
- `issue_orders_missing_shipping_method`: 0.02889
- `issue_products_schema_evolution_category_attributes`: 0.425
- `issue_bad_snapshots_invalid_json`: 0.25
- `issue_bad_snapshots_invalid_timestamp`: 0.25
- `issue_bad_snapshots_missing_required_key`: 0.25
- `issue_bad_snapshots_unknown_schema_version`: 0.25
- `issue_commerce_events_exact_duplicate_event_payload`: 0.01475
- `issue_commerce_events_missing_device_type`: 0.03921
- `issue_commerce_events_late_arrival`: 0.12536
- `issue_dead_letter_events_invalid_json`: 0.25
- `issue_dead_letter_events_invalid_timestamp`: 0.25
- `issue_dead_letter_events_missing_required_key`: 0.25
- `issue_dead_letter_events_unknown_schema_version`: 0.25

## Kafka Topic Row Counts

- `catalog_events`: 1,940 events
- `commerce_events`: 17,810 events
- `dead_letter_events`: 4 events
- `fulfillment_events`: 5,100 events
- `ops_events`: 5 events

## Schema Version Summary

- `catalog_events` schema `1`: 1,940 events
- `commerce_events` schema `1`: 17,810 events
- `dead_letter_events` schema `1`: 4 events
- `fulfillment_events` schema `1`: 5,100 events
- `ops_events` schema `1`: 5 events

## Issue Manifest Summary

- `order_items` / `exact_duplicate_payload`: 135 rows, observed rate 0.01959
- `products` / `missing_brand`: 17 rows, observed rate 0.02833
- `orders` / `missing_shipping_method`: 52 rows, observed rate 0.02889
- `products` / `schema_evolution_category_attributes`: 255 rows, observed rate 0.425
- `bad_snapshots` / `invalid_json`: 1 rows, observed rate 0.25
- `bad_snapshots` / `invalid_timestamp`: 1 rows, observed rate 0.25
- `bad_snapshots` / `missing_required_key`: 1 rows, observed rate 0.25
- `bad_snapshots` / `unknown_schema_version`: 1 rows, observed rate 0.25
- `commerce_events` / `exact_duplicate_event_payload`: 187 rows, observed rate 0.01475
- `commerce_events` / `missing_device_type`: 497 rows, observed rate 0.03921
- `commerce_events` / `late_arrival`: 1,589 rows, observed rate 0.12536
- `dead_letter_events` / `invalid_json`: 1 rows, observed rate 0.25
- `dead_letter_events` / `invalid_timestamp`: 1 rows, observed rate 0.25
- `dead_letter_events` / `missing_required_key`: 1 rows, observed rate 0.25
- `dead_letter_events` / `unknown_schema_version`: 1 rows, observed rate 0.25
