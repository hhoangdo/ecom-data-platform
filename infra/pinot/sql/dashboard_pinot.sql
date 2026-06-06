-- Dashboard SQL for Pinot live operational serving.
-- Pinot is fresh and provisional; Spark Gold through Trino is canonical.

WITH latest_corrections AS (
  SELECT metric_key, MAX(correction_version) AS correction_version
  FROM pinot_realtime_metric_corrections
  GROUP BY metric_key
),
effective_commerce_metrics AS (
  SELECT
    metric_key,
    metric_minute,
    primary_category,
    source,
    device_type,
    payment_method,
    order_status,
    order_count,
    order_placed_count,
    checkout_started_count,
    payment_failure_count,
    revenue_amount,
    gmv_proxy_amount,
    checkout_conversion_rate,
    late_event_count,
    duplicate_event_count,
    correction_version
  FROM pinot_realtime_commerce_metrics_1m
  WHERE metric_key NOT IN (SELECT metric_key FROM latest_corrections)

  UNION ALL

  SELECT
    corr.metric_key,
    corr.metric_minute,
    corr.primary_category,
    corr.source,
    corr.device_type,
    corr.payment_method,
    corr.order_status,
    corr.order_count,
    corr.order_placed_count,
    corr.checkout_started_count,
    corr.payment_failure_count,
    corr.revenue_amount,
    corr.gmv_proxy_amount,
    corr.checkout_conversion_rate,
    corr.late_event_count,
    corr.duplicate_event_count,
    corr.correction_version
  FROM pinot_realtime_metric_corrections AS corr
  INNER JOIN latest_corrections AS latest
    ON corr.metric_key = latest.metric_key
   AND corr.correction_version = latest.correction_version
)
SELECT
  metric_minute,
  primary_category,
  SUM(revenue_amount) AS revenue_amount,
  SUM(gmv_proxy_amount) AS gmv_proxy_amount,
  SUM(payment_failure_count) AS payment_failure_count,
  SUM(order_placed_count) AS order_placed_count,
  SUM(checkout_started_count) AS checkout_started_count
FROM effective_commerce_metrics
GROUP BY metric_minute, primary_category
ORDER BY metric_minute, primary_category;
