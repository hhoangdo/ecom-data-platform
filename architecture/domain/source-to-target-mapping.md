# Source to Target Mapping

## Purpose

This document maps planned source datasets from Section `01` to the Bronze, Silver, and Gold targets expected in Section `02`.

## Core Mapping

| Source Dataset | Bronze Target | Silver Target | Gold Consumers |
| --- | --- | --- | --- |
| `customers` | `raw_customers` | `stg_customers` | `dim_customer`, `feat_customer_90d` |
| `sellers` | `raw_sellers` | `stg_sellers` | `dim_seller`, `dim_product` |
| `products` | `raw_products` | `stg_products` | `dim_product`, `fact_order_item` |
| `product_category_map` | `raw_product_category_map` | `stg_product_category_map` | `dim_product`, category rollups |
| `inventory_snapshots` | `raw_inventory_snapshots` | `stg_inventory_snapshots` | inventory analytics, future features |
| `orders` | `raw_orders` | `stg_orders` | `fact_order`, `obt_order_performance`, `feat_customer_90d` |
| `order_items` | `raw_order_items` | `stg_order_items` | `fact_order_item`, `obt_order_performance`, `feat_customer_90d` |
| `payments` | `raw_payments` | `stg_payments` | `fact_payment_attempt`, `obt_order_performance`, `feat_customer_90d` |
| `shipments` | `raw_shipments` | `stg_shipments` | `fact_shipment`, `obt_order_performance` |
| `promotions` | `raw_promotions` | `stg_promotions` | promotion analysis, future features |
| `stream_events` | `raw_stream_events` | `stg_stream_events` | `feat_stream_60m`, `feat_customer_unified` |

## Contract Reminders

- Bronze adds ingest metadata and preserves source fidelity.
- Silver standardizes types, fills missing optional columns, and applies deduplication rules.
- Gold exposes business-ready keys, measures, and point-in-time-safe feature inputs.
