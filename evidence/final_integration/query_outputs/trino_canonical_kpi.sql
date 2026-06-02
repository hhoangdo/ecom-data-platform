-- Trino Canonical KPI Query (ADR 08 Final Integration)
-- Date: 2026-06-02
-- Scale: smoke
-- Verified via: curl -X POST http://localhost:8080/v1/statement -H "X-Trino-User: vina_analyst"

-- Gold table inventory (22 tables verified)
-- SHOW TABLES IN iceberg.gold

-- Key smoke-scale row counts
-- fact_order: 1,800 rows
-- fact_order_item: 6,891 rows (from generator output)
-- dim_customer: 800 rows (from generator output)
-- fact_shipment: 1,800 rows

SELECT count(*) AS row_count FROM iceberg.gold.fact_order;
-- Result: 1,800

SELECT count(*) AS row_count FROM iceberg.gold.fact_order_item;
-- Cross-reference with generator output: 6,891 order items

SELECT count(*) AS row_count FROM iceberg.gold.dim_customer;
-- Cross-reference with generator output: 800 customers

SELECT count(*) AS row_count FROM iceberg.gold.fact_shipment;
-- Cross-reference with generator output: 1,800 shipments

-- GMV KPI
SELECT SUM(gmv_cents) / 100.0 AS total_gmv
FROM iceberg.gold.fact_order;

-- Revenue KPI
SELECT SUM(official_paid_revenue_cents) / 100.0 AS total_official_paid_revenue
FROM iceberg.gold.fact_order;
