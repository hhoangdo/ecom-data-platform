# Pinot Reconciliation Report

- Window: `2026-05-01T10:00:00+00:00` -> `2026-05-01T11:00:00+00:00`
- Pinot is fresh and provisional; Spark Gold through Trino is canonical.
- Correction handling uses the latest correction row per `metric_key` when correction rows exist.
- Comparison mode: contract check by default. Do not expect numerical parity unless Pinot and Gold were intentionally rebuilt for the same closed hour.
- Pinot returned no rows for the selected window, so this is not a like-for-like numerical comparison.

## Comparison

- Pinot order_count: `0`
- Pinot revenue_amount: `0`
- Pinot gmv_proxy_amount: `0`
- Trino order_count: `10`
- Trino official_paid_revenue: `25361824.0`
- Trino gross_merchandise_value: `25689000.0`
- Trino conversion_rate: `1.0`
- Pinot metric minutes returned: `0`