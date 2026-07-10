# Section 01 Data Generator Quality Report

## Run Context

- Platform: `vina-bim-shop`
- Scale: `medium`
- History days: `60`
- Seed: `42`

## Row Counts

- `bad_snapshots`: 4 rows
- `customers`: 12,000 rows
- `inventory_snapshots`: 60,000 rows
- `order_items`: 168,496 rows
- `orders`: 45,000 rows
- `payments`: 45,000 rows
- `product_category_map`: 7,340 rows
- `products`: 6,000 rows
- `promotions`: 80 rows
- `sellers`: 600 rows
- `shipments`: 45,000 rows
- `kafka_topics`: 639,541 rows

## Quality Metrics

- `hcmc_hanoi_customer_share`: 0.453
- `fmcg_elha_product_share`: 0.66933
- `missing_brand_rate`: 0.021
- `missing_shipping_method_rate`: 0.03062
- `order_session_link_rate`: 1.0
- `offline_order_item_duplicate_rate`: 0.0196
- `issue_order_items_exact_duplicate_payload`: 0.0196
- `issue_products_missing_brand`: 0.021
- `issue_orders_missing_shipping_method`: 0.03062
- `issue_products_schema_evolution_category_attributes`: 0.45367
- `issue_bad_snapshots_invalid_json`: 0.25
- `issue_bad_snapshots_invalid_timestamp`: 0.25
- `issue_bad_snapshots_missing_required_key`: 0.25
- `issue_bad_snapshots_unknown_schema_version`: 0.25
- `issue_commerce_events_exact_duplicate_event_payload`: 0.01478
- `issue_commerce_events_missing_device_type`: 0.03988
- `issue_commerce_events_late_arrival`: 0.11959
- `issue_dead_letter_events_invalid_json`: 0.25
- `issue_dead_letter_events_invalid_timestamp`: 0.25
- `issue_dead_letter_events_missing_required_key`: 0.25
- `issue_dead_letter_events_unknown_schema_version`: 0.25

## Kafka Topic Row Counts

- `catalog_events`: 69,160 events
- `commerce_events`: 442,451 events
- `dead_letter_events`: 4 events
- `fulfillment_events`: 127,921 events
- `ops_events`: 5 events

## Schema Version Summary

- `catalog_events` schema `1`: 69,160 events
- `commerce_events` schema `1`: 442,451 events
- `dead_letter_events` schema `1`: 4 events
- `fulfillment_events` schema `1`: 127,921 events
- `ops_events` schema `1`: 5 events

## Issue Manifest Summary

- `order_items` / `exact_duplicate_payload`: 3,303 rows, observed rate 0.0196
- `products` / `missing_brand`: 126 rows, observed rate 0.021
- `orders` / `missing_shipping_method`: 1,378 rows, observed rate 0.03062
- `products` / `schema_evolution_category_attributes`: 2,722 rows, observed rate 0.45367
- `bad_snapshots` / `invalid_json`: 1 rows, observed rate 0.25
- `bad_snapshots` / `invalid_timestamp`: 1 rows, observed rate 0.25
- `bad_snapshots` / `missing_required_key`: 1 rows, observed rate 0.25
- `bad_snapshots` / `unknown_schema_version`: 1 rows, observed rate 0.25
- `commerce_events` / `exact_duplicate_event_payload`: 4,640 rows, observed rate 0.01478
- `commerce_events` / `missing_device_type`: 12,523 rows, observed rate 0.03988
- `commerce_events` / `late_arrival`: 37,553 rows, observed rate 0.11959
- `dead_letter_events` / `invalid_json`: 1 rows, observed rate 0.25
- `dead_letter_events` / `invalid_timestamp`: 1 rows, observed rate 0.25
- `dead_letter_events` / `missing_required_key`: 1 rows, observed rate 0.25
- `dead_letter_events` / `unknown_schema_version`: 1 rows, observed rate 0.25

## Rubric Evidence

- Consolidated rows 5-14 evidence: [rubric_evidence_summary.md](rubric_evidence_summary.md)
