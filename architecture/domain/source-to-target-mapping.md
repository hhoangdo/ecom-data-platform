# Source To Target Mapping

## Purpose

This document maps implemented source datasets and event topics to Bronze, Silver, Gold, realtime serving, local analytics, and governance targets.

## Offline Snapshot Mapping

| Source dataset | Bronze target | Silver target | Gold targets |
| --- | --- | --- | --- |
| `customers` | `raw_customers` | `stg_customers` | `dim_customer`, `feat_customer_90d`, `feat_customer_unified` |
| `sellers` | `raw_sellers` | `stg_sellers` | `dim_seller`, `dim_product` |
| `products` | `raw_products` | `stg_products` | `dim_product`, `fact_order_item`, `bridge_product_category` |
| `product_category_map` | `raw_product_category_map` | `stg_product_category_map` | `dim_category`, `bridge_product_category` |
| `inventory_snapshots` | `raw_inventory_snapshots` | `stg_inventory_snapshots` | `fact_inventory_snapshot` |
| `promotions` | `raw_promotions` | `stg_promotions` | `dim_promotion`, `fact_promotion_application`, `fact_order_item` |
| `orders` | `raw_orders` | `stg_orders` | `fact_order`, `obt_order_performance`, `agg_hourly_reconciled_kpi`, `feat_customer_90d` |
| `order_items` | `raw_order_items` | `stg_order_items` | `fact_order_item`, `obt_order_performance`, `agg_hourly_reconciled_kpi`, `feat_customer_90d` |
| `payments` | `raw_payments` | `stg_payments` | `fact_payment_attempt`, `fact_order`, `obt_order_performance`, `agg_hourly_reconciled_kpi` |
| `shipments` | `raw_shipments` | `stg_shipments` | `fact_shipment`, `obt_order_performance` |
| `bad_snapshots` | `raw_bad_snapshots` | quarantine evidence | Quality reports, GX evidence, DataHub assertions |

## Event Topic Mapping

| Source topic | Bronze target | Silver target | Downstream targets |
| --- | --- | --- | --- |
| `commerce_events` | `raw_kafka_commerce_events` | `stg_commerce_events` | `feat_stream_60m`, `feat_customer_unified`, Flink `realtime_commerce_metrics_1m`, Flink `realtime_metric_corrections` |
| `catalog_events` | `raw_kafka_catalog_events` | `stg_catalog_events` | Flink `realtime_ops_alerts`, catalog/inventory context, DataHub lineage |
| `fulfillment_events` | `raw_kafka_fulfillment_events` | `stg_fulfillment_events` | Flink `realtime_ops_alerts`, shipment context, DataHub lineage |
| `ops_events` | `raw_kafka_ops_events` | `stg_ops_events` | Flink `realtime_ops_alerts`, operational evidence |
| `dead_letter_events` | `raw_bad_events` | quarantine evidence | DLQ evidence, GX evidence, DataHub assertions |

## Realtime Serving Mapping

| Flink-derived topic | Pinot table | Role |
| --- | --- | --- |
| `realtime_commerce_metrics_1m` | `pinot_realtime_commerce_metrics_1m` | Fresh one-minute commerce metrics for live dashboards. |
| `realtime_ops_alerts` | `pinot_realtime_ops_alerts` | Operational alert review for bursts, lateness, duplicates, inventory, and fulfillment signals. |
| `realtime_metric_corrections` | `pinot_realtime_metric_corrections` | Late-event correction snapshots for correction-aware Pinot queries. |

Pinot is fresh and provisional. `agg_hourly_reconciled_kpi` in Spark Gold through Trino is the canonical reconciliation target.

## Local Analytics And Governance Mapping

| Asset | Source | Role |
| --- | --- | --- |
| `data/gold/vina_bim_shop.duckdb` | dbt rebuild from generated raw data | Local parity oracle for schema tests and row/KPI comparison. |
| `data/gold/vina_bim_shop_executive.duckdb` | Trino Gold snapshot export | DuckDB Executive Mart for DBeaver and offline evidence review. |
| DataHub Kafka datasets | Kafka source and derived topics | Topic catalog and lineage metadata. |
| DataHub Trino/Iceberg datasets | Spark-written Gold tables served through Trino | Canonical model catalog, lineage, and governance tags. |
| DataHub GX assertions | Great Expectations validation evidence | Quality metadata for Bronze, Silver, Gold, and reconciliation checks. |

## Contract Reminders

- Bronze preserves source fidelity and adds ingestion metadata.
- Silver standardizes formats, casts analytical fields, fills missing optional columns, and deduplicates by business keys.
- Gold exposes business-ready dimensions, facts, OBTs, aggregates, and feature tables.
- Trino-served Gold is the canonical SQL surface.
- Pinot consumes Flink-derived topics, not raw source topics.
- DuckDB files are local evidence artifacts and should be regenerated when their sources change.
