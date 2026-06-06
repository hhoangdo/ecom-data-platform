-- Pinot-side hourly reconciliation contract.
-- Use latest correction rows per metric_key so append-only correction records do not double count.

WITH latest_corrections AS (
  SELECT metric_key, MAX(correction_version) AS correction_version
  FROM pinot_realtime_metric_corrections
  GROUP BY metric_key
),
effective_commerce_metrics AS (
  SELECT metric_key, metric_minute, order_count, order_placed_count, checkout_started_count, revenue_amount, gmv_proxy_amount
  FROM pinot_realtime_commerce_metrics_1m
  WHERE metric_key NOT IN (SELECT metric_key FROM latest_corrections)

  UNION ALL

  SELECT
    corr.metric_key,
    corr.metric_minute,
    corr.order_count,
    corr.order_placed_count,
    corr.checkout_started_count,
    corr.revenue_amount,
    corr.gmv_proxy_amount
  FROM pinot_realtime_metric_corrections AS corr
  INNER JOIN latest_corrections AS latest
    ON corr.metric_key = latest.metric_key
   AND corr.correction_version = latest.correction_version
)
SELECT
  metric_minute,
  SUM(order_count) AS order_count,
  SUM(revenue_amount) AS revenue_amount,
  SUM(gmv_proxy_amount) AS gmv_proxy_amount,
  SUM(order_placed_count) AS order_placed_count,
  SUM(checkout_started_count) AS checkout_started_count
FROM effective_commerce_metrics
GROUP BY metric_minute
ORDER BY metric_minute;
