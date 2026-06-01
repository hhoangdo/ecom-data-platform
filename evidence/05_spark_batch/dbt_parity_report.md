# dbt Parity Report

Overall success: PASS

| Check | DuckDB | Spark/Trino | Delta | Success |
| --- | ---: | ---: | ---: | --- |
| dim_customer.row_count | 800 | 800 | 0 | PASS |
| dim_seller.row_count | 80 | 80 | 0 | PASS |
| dim_product.row_count | 600 | 600 | 0 | PASS |
| dim_category.row_count | 20 | 20 | 0 | PASS |
| dim_date.row_count | 8 | 8 | 0 | PASS |
| dim_payment_method.row_count | 5 | 5 | 0 | PASS |
| dim_order_status.row_count | 2 | 2 | 0 | PASS |
| dim_shipment_status.row_count | 4 | 4 | 0 | PASS |
| dim_shipping_method.row_count | 5 | 5 | 0 | PASS |
| dim_promotion.row_count | 17 | 17 | 0 | PASS |
| bridge_product_category.row_count | 738 | 738 | 0 | PASS |
| fact_order.row_count | 1800 | 1800 | 0 | PASS |
| fact_order_item.row_count | 6688 | 6688 | 0 | PASS |
| fact_payment_attempt.row_count | 1800 | 1800 | 0 | PASS |
| fact_shipment.row_count | 1800 | 1800 | 0 | PASS |
| fact_inventory_snapshot.row_count | 1200 | 1200 | 0 | PASS |
| fact_promotion_application.row_count | 2194 | 2194 | 0 | PASS |
| obt_order_performance.row_count | 1800 | 1800 | 0 | PASS |
| agg_hourly_reconciled_kpi.row_count | 159 | 159 | 0 | PASS |
| feat_customer_90d.row_count | 681 | 681 | 0 | PASS |
| feat_stream_60m.row_count | 4612 | 4612 | 0 | PASS |
| feat_customer_unified.row_count | 681 | 681 | 0 | PASS |
| fact_order.official_paid_revenue | 5305136289.0 | 5305136289.0 | 0.0 | PASS |
| fact_order.gross_merchandise_value | 5615446000.0 | 5615446000.0 | 0.0 | PASS |
| fact_order_item.estimated_cost | 3967767902.4 | 3967767902.4 | 0.0 | PASS |
| fact_order_item.estimated_margin | 1337368386.6 | 1337368386.6 | 0.0 | PASS |
| agg_hourly_reconciled_kpi.official_paid_revenue | 5305136289.0 | 5305136289.0 | 0.0 | PASS |