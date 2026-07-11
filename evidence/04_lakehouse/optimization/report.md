# Iceberg Storage Optimization Report

## Blocked Physical Layout

No approved table had an eligible partition-level group of at least two data files.
No rewrite was forced and no after-rewrite Trino timing was collected.

| Table | Status | Input files | Rewritten files |
| --- | --- | ---: | ---: |
| silver.stg_orders | skipped_insufficient_files | 1 | 0 |
| silver.stg_order_items | skipped_insufficient_files | 1 | 0 |
| gold.fact_order | skipped_no_eligible_file_groups | 7 | 0 |
| gold.fact_order_item | skipped_no_eligible_file_groups | 7 | 0 |

Tables evaluated: silver.stg_orders, silver.stg_order_items, gold.fact_order, gold.fact_order_item.
