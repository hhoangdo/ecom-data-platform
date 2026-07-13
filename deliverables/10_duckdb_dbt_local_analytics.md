# DuckDB, dbt, And Local Analytics

## Purpose

DuckDB provides the portable local analytics layer for the Vina Bim Shop platform. It is used in two distinct ways:

| Local asset | Path | Role |
| --- | --- | --- |
| dbt-DuckDB parity oracle | `data/gold/vina_bim_shop.duckdb` | Rebuilds the Bronze/Silver/Gold modeling path locally through dbt for fast regression checks and Section 01/02 reproducibility. |
| DuckDB Executive Mart | `data/gold/vina_bim_shop_executive.duckdb` | Stores a local snapshot exported from Trino-served Spark Gold tables for offline inspection and evidence packaging. |

These files are gitignored local outputs. They are meant to be rebuilt, inspected in DBeaver or the DuckDB CLI, and packaged as evidence when needed.

## What DuckDB And dbt Do In This Project

DuckDB is an embedded analytical database. It runs locally as a single file and is well suited for coursework evidence because it does not require a separate database server.

dbt is the SQL transformation and testing framework used with DuckDB for the local compatibility path. The dbt project defines Bronze views, Silver views, Gold tables, tests, macros, and documentation-friendly model structure.

Together, dbt-DuckDB gives the project an independent local implementation of the core schema design:

| Responsibility | dbt-DuckDB behavior |
| --- | --- |
| Rebuild local Gold models | Reads generated local raw data and materializes modeled tables into DuckDB. |
| Validate modeling contracts | Runs dbt tests and produces build/test evidence. |
| Support Section 01/02 reproducibility | Works without starting Kafka, Spark, Trino, Pinot, or Airflow. |
| Provide parity comparison | Row counts and key KPIs are compared against Spark/Trino Gold evidence. |

## Why DuckDB/dbt Is Needed

The full distributed platform is useful, but it is not always the right tool for fast local verification. DuckDB/dbt solves several practical pain points:

| Pain point | DuckDB/dbt value |
| --- | --- |
| Distributed services are expensive to start on a laptop. | dbt-DuckDB can rebuild the local model path with no Docker services. |
| Coursework evidence needs a portable artifact. | A single `.duckdb` file can be opened in DBeaver and shared as evidence. |
| Spark changes need regression checks. | dbt-DuckDB acts as an independent parity oracle for row counts and KPI values. |
| Instructors may want to inspect tables without running the stack. | The Executive Mart exports canonical Trino Gold into a local file. |

DuckDB is not a replacement for Spark in the full platform. It is the local reproducibility and inspection layer.

## dbt-DuckDB Parity Oracle

The dbt profile points to:

```yaml
path: data/gold/vina_bim_shop.duckdb
schema: main
```

The local build command is:

```powershell
uv run dbt build --project-dir infra/analytics/dbt --profiles-dir infra/analytics/dbt
```

The parity oracle is used to compare local dbt output with Spark-written Gold tables queried through Trino. Evidence in `evidence/05_spark_batch/dbt_parity_report.md` records matching row counts and KPI values for dimensions, facts, OBT tables, aggregates, and feature tables.

Examples of compared outputs include:

| Output | Validation role |
| --- | --- |
| `dim_customer`, `dim_product`, `dim_seller` | Dimension row-count parity. |
| `fact_order`, `fact_order_item`, `fact_payment_attempt` | Core fact row-count parity. |
| `obt_order_performance` | Denormalized order analysis parity. |
| `agg_hourly_reconciled_kpi` | Reconciled aggregate parity. |
| `feat_customer_90d`, `feat_stream_60m`, `feat_customer_unified` | Feature-table parity. |
| `official_paid_revenue`, `gross_merchandise_value`, `estimated_cost`, `estimated_margin` | KPI value parity. |

## DuckDB Executive Mart

The DuckDB Executive Mart is different from the dbt-DuckDB parity oracle.

| Aspect | dbt-DuckDB parity oracle | DuckDB Executive Mart |
| --- | --- | --- |
| Source | Local generated raw inputs. | Trino query results over `iceberg.gold.*`. |
| Transformation ownership | dbt local models. | No second transformation layer; exported snapshot only. |
| Truth role | Independent parity check. | Portable copy of canonical Spark Gold. |
| Typical user | Data engineer or reviewer checking model consistency. | Instructor, analyst, or reviewer inspecting final Gold tables locally. |
| Output path | `data/gold/vina_bim_shop.duckdb` | `data/gold/vina_bim_shop_executive.duckdb` |

Export command:

```powershell
uv run python scripts/spark/export_executive_mart.py --duckdb-path data/gold/vina_bim_shop_executive.duckdb --evidence-root evidence/05_spark_batch
```

The export reads each required Gold table from Trino as `iceberg.gold.<table>`, creates matching DuckDB tables under the `gold` schema, and writes metadata tables under `mart_metadata`.

When run, the export writes:

| Output | Purpose |
| --- | --- |
| `mart_metadata.export_manifest` | Stores export timestamp, source, destination path, table count, and total row count. |
| `mart_metadata.table_manifest` | Stores table-level source relation, row count, column count, and export timestamp. |
| `evidence/05_spark_batch/executive_mart_export_manifest.json` | Machine-readable export evidence. |
| `evidence/05_spark_batch/executive_mart_export_report.md` | Human-readable export summary. |

## Service Interactions

| Service or asset | Relationship |
| --- | --- |
| Data generator | Produces local raw data consumed by the dbt-DuckDB path. |
| dbt | Defines local SQL models and tests against DuckDB. |
| Spark | Produces the distributed Gold tables used for parity comparison and executive mart export. |
| Trino | Serves Spark-written Iceberg Gold tables to the executive mart export script. |
| DBeaver or DuckDB CLI | Opens local `.duckdb` files for inspection. |
| Evidence folders | Store dbt build reports, parity reports, and export manifests. |
| README and schema deliverable | Point reviewers to the data dictionary and truth-policy explanation. |

## Evidence And Limitations

Important evidence paths:

| Path | Role |
| --- | --- |
| `evidence/02_schema_design/dbt_build_report.md` | dbt build/test evidence for the local schema path. |
| `evidence/02_schema_design/dbt_test_results.csv` | Tabular dbt test results. |
| `evidence/05_spark_batch/dbt_parity_report.md` | Human-readable DuckDB vs Spark/Trino parity report. |
| `evidence/05_spark_batch/dbt_parity_report.json` | Machine-readable parity report. |

## Isolated ART Index Evidence (Row 27)

After the smoke dbt rebuild (52 models and 66 tests), the index experiment copied only `gold.fact_order` into the disposable database at `tmp/rubic-check/runtime/duckdb_index_benchmark.duckdb`. The canonical `data/gold/vina_bim_shop.duckdb` was opened read-only by the benchmark and had the same SHA-256 before and after: `b0bb76a932bb1686259b332e50130fb70fb513cd6e1b6b731207c032ccb7b438`.

The experiment queried the same existing `order_id`, `ORD-BDG-20260426-00000006`, before and after creating `idx_benchmark_fact_order_order_id` on `benchmark_fact_order(order_id)`. Each variant used two warmups and seven measured executions; raw samples, result rows, hashes, index inventory, and both explain outputs are preserved below.

| Variant | Seven measured client times (ms) | Median (ms) | Result hash |
| --- | --- | ---: | --- |
| Baseline | 1.4715, 1.3721, 1.4490, 1.4326, 1.4292, 1.3582, 1.2887 | 1.4292 | `ecfe42c8ba49a03ee49642ccc7a043cdba1230fecebc2602c69b73ee2c74771f` |
| Named ART index present | 1.1436, 1.0804, 1.0631, 1.1219, 1.1242, 1.1611, 1.1119 | 1.1219 | `ecfe42c8ba49a03ee49642ccc7a043cdba1230fecebc2602c69b73ee2c74771f` |

The result hash is identical, `duckdb_indexes()` records the named index, and the captured explain text did not visibly change. The second local median is lower in this one run, but this small, warm-cache-sensitive measurement and an unchanged plan do not establish a general ART-index speedup.

Code and evidence:

- [isolated benchmark script](../scripts/analytics/benchmark_duckdb_index.py)
- [machine-readable index result](../evidence/10_duckdb_dbt_local_analytics/index_optimization/index_benchmark.json) and [run manifest](../evidence/10_duckdb_dbt_local_analytics/index_optimization/run_manifest.json)
- [baseline explain](../evidence/10_duckdb_dbt_local_analytics/index_optimization/baseline_explain.txt) and [indexed explain](../evidence/10_duckdb_dbt_local_analytics/index_optimization/indexed_explain.txt)

Known boundaries:

- `data/gold/vina_bim_shop.duckdb` is rebuilt from local raw data and represents the dbt parity path.
- `data/gold/vina_bim_shop_executive.duckdb` is a snapshot exported from Trino Gold and is stale until regenerated.
- DuckDB files are local evidence artifacts, not shared production databases.
- The canonical full-platform truth remains Spark Gold served through Trino.
- The Row-27 timings are isolated local observations, not a production tuning recommendation or a promise of index acceleration.

## Convenience Make Targets

The root [Makefile](../Makefile) wraps the most common commands for this deliverable under a `make + verb` convention. It delegates to the official scripts in [scripts/](../scripts/) and to `scripts/ctl.py` for Docker Compose lifecycle; compose targets expand documented profile bundles and prebuild shared images where needed.

| Target | What it does |
| --- | --- |
| `make install` | `uv sync` to install Python dependencies. |
| `make generate` | Run the data generator (the dbt-DuckDB parity oracle consumes its raw outputs). |
| `make build-dbt` | Run `dbt build` against the local DuckDB profile that produces the parity oracle. |
| `make test` | Run `pytest` to verify the parity contracts. |
