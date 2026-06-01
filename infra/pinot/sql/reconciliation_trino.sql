SELECT
  CAST(metric_hour AS VARCHAR) AS metric_hour,
  order_count,
  official_paid_revenue,
  gross_merchandise_value,
  payment_attempt_count,
  payment_success_count,
  checkout_started_count,
  order_placed_event_count,
  conversion_rate
FROM iceberg.gold.agg_hourly_reconciled_kpi
WHERE metric_hour >= from_iso8601_timestamp('{start_ts}')
  AND metric_hour < from_iso8601_timestamp('{end_ts}')
ORDER BY metric_hour;
