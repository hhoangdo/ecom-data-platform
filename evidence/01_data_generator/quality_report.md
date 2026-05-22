# Section 01 Data Generator Quality Report

## Run Context

- Platform: `vina-bim-shop`
- Scale: `smoke`
- History days: `7`
- Seed: `42`

## Row Counts

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
- `stream_events`: 12,676 rows

## Quality Metrics

- `hcmc_hanoi_customer_share`: 0.45125
- `fmcg_elha_product_share`: 0.66
- `missing_brand_rate`: 0.02833
- `missing_shipping_method_rate`: 0.02889
- `order_session_link_rate`: 1.0
- `offline_order_item_duplicate_rate`: 0.01959
- `stream_late_arrival_rate`: 0.12536
- `stream_event_duplicate_id_rate`: 0.01475
- `stream_missing_device_type_rate`: 0.03921
- `stream_burst_event_count`: 687.0
- `issue_order_items_exact_duplicate_payload`: 0.01959
- `issue_products_missing_brand`: 0.02833
- `issue_orders_missing_shipping_method`: 0.02889
- `issue_products_schema_evolution_category_attributes`: 0.425
- `issue_stream_events_exact_duplicate_event_payload`: 0.01475
- `issue_stream_events_missing_device_type`: 0.03921
- `issue_stream_events_late_arrival`: 0.12536

## Issue Manifest Summary

- `order_items` / `exact_duplicate_payload`: 135 rows, observed rate 0.01959
- `products` / `missing_brand`: 17 rows, observed rate 0.02833
- `orders` / `missing_shipping_method`: 52 rows, observed rate 0.02889
- `products` / `schema_evolution_category_attributes`: 255 rows, observed rate 0.425
- `stream_events` / `exact_duplicate_event_payload`: 187 rows, observed rate 0.01475
- `stream_events` / `missing_device_type`: 497 rows, observed rate 0.03921
- `stream_events` / `late_arrival`: 1,589 rows, observed rate 0.12536
