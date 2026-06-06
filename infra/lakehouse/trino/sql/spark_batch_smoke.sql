SHOW TABLES FROM iceberg.gold;
SELECT count(*) AS fact_order_count
FROM iceberg.gold.fact_order;
SELECT round(sum(official_paid_revenue), 2) AS official_paid_revenue
FROM iceberg.gold.fact_order;
SELECT metric_hour, order_count, official_paid_revenue
FROM iceberg.gold.agg_hourly_reconciled_kpi
ORDER BY metric_hour
LIMIT 10;
