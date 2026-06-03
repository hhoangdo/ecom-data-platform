-- Trino canonical verification queries for ADR 08 final integration
-- Verified against the repaired Trino 476 cluster on 2026-06-03.

SHOW TABLES FROM iceberg.gold;
-- Result: 22 tables visible

SELECT count(*) AS row_count
FROM iceberg.gold.fact_order;
-- Result: 1800

SELECT count(*) AS row_count
FROM iceberg.gold.fact_order_item;
-- Result: 6756

SELECT count(*) AS row_count
FROM iceberg.gold.dim_customer;
-- Result: 800

SELECT count(*) AS row_count
FROM iceberg.gold.fact_shipment;
-- Result: 1800

SELECT count(*) AS row_count
FROM iceberg.gold.agg_hourly_reconciled_kpi;
-- Result: 164

SELECT count(*) AS row_count
FROM iceberg.gold.feat_stream_60m;
-- Result: 4652

SELECT
  SUM(gross_merchandise_value) AS total_gmv,
  SUM(official_paid_revenue) AS total_official_paid_revenue
FROM iceberg.gold.fact_order;
-- Result: total_gmv = 5737896000.0, total_official_paid_revenue = 5488980096.0
