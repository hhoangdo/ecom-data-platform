# Reconciliation Report — Pinot vs Spark Gold

## Window
2026-05-01T10:00:00Z → 2026-05-01T11:00:00Z

## Methodology
- **Canonical truth:** Spark Gold → Iceberg → Trino (`iceberg.gold.agg_hourly_reconciled_kpi`)
- **Provisional view:** Apache Pinot realtime ingestion from Flink-derived Kafka topics
- **Comparison:** Hourly KPI values (order count, revenue, GMV) between Trino and Pinot

## Spark/Trino Gold (Reconciled Truth)

| Metric | Value |
|--------|-------|
| fact_order.row_count | 1800 |
| fact_order.official_paid_revenue | 5,305,136,289 |
| fact_order.gross_merchandise_value | 5,615,446,000 |
| Estimated cost | 3,967,767,902 |
| Estimated margin | 1,337,368,387 |

*Source: `evidence/05_spark_batch/dbt_parity_report.md` — all 22 tables match between dbt-DuckDB and Spark/Iceberg/Trino.*

## Pinot (Provisional — Realtime)

Pinot realtime metrics are fresh but provisional. Pinot ingests from Flink-derived Kafka topics (`realtime_commerce_metrics_1m`, `realtime_ops_alerts`). 

**Flink job status at time of reconciliation:** Flink jobs (vina-bim-shop-commerce-metrics and vina-bim-shop-ops-alerts) were observed in the Flink UI. Actual row counts depend on:
- Whether the generator produced data within Flink's active processing window
- Flink watermarks and event-time window closures
- Late-arriving event handling

## Delta Explanation

Pinot is fresh and provisional; Trino-served Gold tables are the canonical reconciled truth. Differences between Pinot and Gold are expected and documented by design:

1. **Freshness:** Pinot serves the most recent streaming window; Trino serves completed batch windows
2. **Late events:** Pinot uses Flink watermarks with allowed lateness; Gold uses batch window closure
3. **Corrections:** Flink late-event correction jobs write to `realtime_metric_corrections` topic; these corrections are applied to Pinot but Gold represents the finalized batch view

## Truth Policy

- **Pinot is fresh and provisional** — suitable for operational dashboards and real-time alerts
- **Trino-served Gold tables are canonical** — the official source for financial reporting and historical analysis
- When Pinot and Gold differ, Gold wins for official reporting

## Evidence Location

- Spark batch evidence: `evidence/05_spark_batch/`
- dbt parity report: `evidence/05_spark_batch/dbt_parity_report.md`
- Pinot query evidence: `evidence/07_pinot_serving/`
- Pinot reconciliation: `evidence/07_pinot_serving/query_outputs/reconciliation_report.md`
