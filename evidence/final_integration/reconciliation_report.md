# Reconciliation Report - Pinot vs Spark Gold

## Methodology

- Canonical truth: Spark Gold -> Iceberg -> Trino (`iceberg.gold.*`)
- Provisional view: Apache Pinot fed from Flink-derived Kafka topics
- Comparison basis: repaired ADR 08 integration run with dbt parity still at 119/119 PASS

## Spark/Trino Gold (Canonical Truth)

| Metric | Value |
|--------|-------|
| `fact_order.row_count` | 1,800 |
| `fact_order.official_paid_revenue` | 5,488,980,096.00 |
| `fact_order.gross_merchandise_value` | 5,737,896,000.00 |
| `fact_order_item.estimated_cost` | 4,068,563,658.33 |
| `fact_order_item.estimated_margin` | 1,420,416,437.67 |
| `agg_hourly_reconciled_kpi.row_count` | 164 |
| `feat_stream_60m.row_count` | 4,652 |

Source: `evidence/05_spark_batch/dbt_parity_report.json` and live Trino verification after the Trino worker repair.

## Pinot (Provisional Realtime)

Pinot remains the low-latency serving layer for the Flink-derived realtime topics:

- `realtime_commerce_metrics_1m`
- `realtime_ops_alerts`
- `realtime_metric_corrections`

Pinot is healthy and bootstrapped, but it is still treated as provisional by design.

## Truth Policy

1. Pinot is fresh and operationally useful.
2. Trino-served Gold remains the official reconciled truth for reporting.
3. When realtime and batch views differ, Gold wins for historical and financial evidence.

## Evidence Location

- Spark batch evidence: `evidence/05_spark_batch/`
- Pinot serving evidence: `evidence/07_pinot_serving/`
- Final screenshots: `evidence/final_integration/ui_screenshots/`
