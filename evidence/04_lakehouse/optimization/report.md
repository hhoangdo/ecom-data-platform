# Iceberg Storage Optimization Report

## Method

- Captured Iceberg data-file metadata and logical invariants before every rewrite.
- Rewrote only approved tables with a 128 MiB target and at least two input files.
- Used two warmups and seven measured Trino executions per table before and after compaction.

## Rewrite Results

| Table | Status | Input files | Rewritten files | Duration ms |
| --- | --- | ---: | ---: | ---: |
| silver.stg_orders | skipped_insufficient_files | 1 | 0 | 0.0 |
| silver.stg_order_items | skipped_insufficient_files | 1 | 0 | 0.0 |
| gold.fact_order | rewritten | 13 | 12 | 2718.366 |
| gold.fact_order_item | rewritten | 13 | 12 | 1263.747 |

## Timing Interpretation

The timing samples are observed local measurements. They do not establish a guaranteed performance improvement.
- before `silver.stg_orders` median client time: 211.490 ms.
- before `silver.stg_order_items` median client time: 174.374 ms.
- before `gold.fact_order` median client time: 158.547 ms.
- before `gold.fact_order_item` median client time: 158.560 ms.
- after `silver.stg_orders` median client time: 137.492 ms.
- after `silver.stg_order_items` median client time: 137.388 ms.
- after `gold.fact_order` median client time: 128.030 ms.
- after `gold.fact_order_item` median client time: 137.837 ms.

Tables evaluated: silver.stg_orders, silver.stg_order_items, gold.fact_order, gold.fact_order_item.
