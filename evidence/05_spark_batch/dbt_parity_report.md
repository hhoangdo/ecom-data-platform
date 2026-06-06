# dbt Parity Report

Overall success: PASS

| Check | DuckDB | Spark/Trino | Delta | Success |
| --- | ---: | ---: | ---: | --- |
| dim_customer.row_count | 800 | 800 | 0 | PASS |
| dim_seller.row_count | 80 | 80 | 0 | PASS |
| dim_product.row_count | 600 | 600 | 0 | PASS |
| dim_category.row_count | 20 | 20 | 0 | PASS |
| dim_date.row_count | 9 | 9 | 0 | PASS |
| dim_payment_method.row_count | 5 | 5 | 0 | PASS |
| dim_order_status.row_count | 2 | 2 | 0 | PASS |
| dim_shipment_status.row_count | 4 | 4 | 0 | PASS |
| dim_shipping_method.row_count | 5 | 5 | 0 | PASS |
| dim_promotion.row_count | 17 | 17 | 0 | PASS |
| bridge_product_category.row_count | 729 | 729 | 0 | PASS |
| fact_order.row_count | 1800 | 1800 | 0 | PASS |
| fact_order_item.row_count | 6756 | 6756 | 0 | PASS |
| fact_payment_attempt.row_count | 1800 | 1800 | 0 | PASS |
| fact_shipment.row_count | 1800 | 1800 | 0 | PASS |
| fact_inventory_snapshot.row_count | 1200 | 1200 | 0 | PASS |
| fact_promotion_application.row_count | 2233 | 2233 | 0 | PASS |
| obt_order_performance.row_count | 1800 | 1800 | 0 | PASS |
| agg_hourly_reconciled_kpi.row_count | 164 | 164 | 0 | PASS |
| feat_customer_90d.row_count | 688 | 688 | 0 | PASS |
| feat_stream_60m.row_count | 4652 | 4652 | 0 | PASS |
| feat_customer_unified.row_count | 688 | 688 | 0 | PASS |
| fact_order.official_paid_revenue | 5488980096.0 | 5488980096.0 | 0.0 | PASS |
| fact_order.gross_merchandise_value | 5737896000.0 | 5737896000.0 | 0.0 | PASS |
| fact_order_item.estimated_cost | 4068563658.33 | 4068563658.33 | 0.0 | PASS |
| fact_order_item.estimated_margin | 1420416437.67 | 1420416437.67 | 0.0 | PASS |
| agg_hourly_reconciled_kpi.official_paid_revenue | 5488980096.0 | 5488980096.0 | 0.0 | PASS |