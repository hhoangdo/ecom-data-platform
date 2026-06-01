# Pinot Reconciliation Report

- Window: `2026-05-01T10:00:00+00:00` -> `2026-05-01T11:00:00+00:00`
- Pinot is fresh and provisional; Spark Gold through Trino is canonical.
- Correction handling uses the latest correction row per `metric_key` when correction rows exist.

## Comparison

- Pinot order_count: `0`
- Pinot revenue_amount: `0`
- Pinot gmv_proxy_amount: `0`
- Trino order_count: `15`
- Trino official_paid_revenue: `56494113.0`
- Trino gross_merchandise_value: `62822000.0`
- Trino conversion_rate: `1.0714285714285714`