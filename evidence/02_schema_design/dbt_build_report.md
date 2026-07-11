# Section 02 dbt Evidence Report

## Command Summary

- `dbt build --project-dir infra/analytics/dbt --profiles-dir infra/analytics/dbt` completed before artifact extraction.
- `dbt docs generate --project-dir infra/analytics/dbt --profiles-dir infra/analytics/dbt` produced `manifest.json` and `catalog.json` for summarized evidence.

## dbt Results

- Models: 52 (success=52)
- Tests: 66 (pass=66)
- Cataloged models by schema: bronze=16, gold=22, silver=14

## Gold Row Counts

- `agg_hourly_reconciled_kpi`: 164 rows
- `bridge_product_category`: 729 rows
- `dim_category`: 20 rows
- `dim_customer`: 800 rows
- `dim_date`: 9 rows
- `dim_order_status`: 2 rows
- `dim_payment_method`: 5 rows
- `dim_product`: 600 rows
- `dim_promotion`: 17 rows
- `dim_seller`: 80 rows
- `dim_shipment_status`: 4 rows
- `dim_shipping_method`: 5 rows
- `fact_inventory_snapshot`: 1,200 rows
- `fact_order`: 1,800 rows
- `fact_order_item`: 6,756 rows
- `fact_payment_attempt`: 1,800 rows
- `fact_promotion_application`: 2,233 rows
- `fact_shipment`: 1,800 rows
- `feat_customer_90d`: 688 rows
- `feat_customer_unified`: 688 rows
- `feat_stream_60m`: 4,652 rows
- `obt_order_performance`: 1,800 rows

## Evidence Files

- `evidence/02_schema_design/dbt_test_results.csv`
- `evidence/02_schema_design/dbt_model_results.csv`
- `evidence/02_schema_design/dbt_catalog_summary.csv`
- `evidence/02_schema_design/schema_inventory.csv`
- `evidence/02_schema_design/table_row_counts.csv`
- `evidence/02_schema_design/run_manifest.json`
- `evidence/02_schema_design/screenshots/schema_design.png`
- `evidence/02_schema_design/screenshots/gold_schema_inventory.png`
- `evidence/02_schema_design/screenshots/dbt_test_summary.png`
