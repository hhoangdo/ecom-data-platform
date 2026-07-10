# Storage Optimization Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Demonstrate measured Lakehouse compaction and data-warehouse indexing while proving unchanged data results and documenting observed performance honestly.

**Architecture:** Add a Spark/Iceberg maintenance command that snapshots file layout, rewrites small files, rechecks row/checksum invariants, and benchmarks representative Trino queries. Add an isolated DuckDB benchmark database that compares the same equality query before and after an ART index without mutating the canonical analytics database.

**Tech Stack:** Spark 4.0, Iceberg 1.10.1, MinIO, Trino, DuckDB, dbt, Python 3.12, pytest, JSON, CSV, and Markdown.

## Global Constraints

- Never run compaction until a before snapshot is durably written.
- Compact only `silver.stg_orders`, `silver.stg_order_items`, `gold.fact_order`, and `gold.fact_order_item`.
- Use target file size `134217728` bytes and require at least two input files before rewriting.
- Preserve exact row counts and deterministic aggregate checksums before and after each Iceberg rewrite.
- Performance success is measured evidence, not a promise that every small local query becomes faster.
- The DuckDB experiment must use `tmp/rubic-check/runtime/duckdb_index_benchmark.duckdb`; never create or drop indexes in `data/gold/vina_bim_shop.duckdb`.
- Run two warmups and seven measured executions per query variant; report median milliseconds and both explain plans.
- Do not stage or commit unless explicitly requested. Preserve unrelated changes and prefix commands with `rtk`.

---

## Rubric Coverage

| Row | Requirement | Points | Current status | Effort | Value added |
|---:|---|---:|---|---|---|
| 26 | Optimize Lakehouse storage and compare optimized versus unoptimized. | 2 | Partial | M | High |
| 27 | Optimize data warehouse, for example indexing, with code and analysis. | 2 | Partial | S | Medium |

## Current Implementation and Evidence

- Spark creates partitioned Iceberg tables through `src/vina_bim_shop/lakehouse/spark/job.py` and partition constants.
- `deliverables/04_lakehouse.md` and `evidence/04_lakehouse/` document catalog/storage health, but not file rewrite outcomes.
- DuckDB/dbt local analytics exists under `infra/analytics/dbt/` with canonical output at `data/gold/vina_bim_shop.duckdb`.
- Pinot already uses inverted indexes, but an isolated DuckDB experiment gives row 27 a deterministic before/after workflow without changing serving-table runtime state.

## Gap, Scope, and Non-Goals

**Gap:** There is no compaction command, no file-layout comparison, no before/after query analysis, and no explicit warehouse index experiment.

**Scope:** Add maintenance and benchmark scripts, focused tests, reproducible evidence artifacts, and documentation for rows 26-27.

**Non-goals:** Do not add Z-ordering, schedule maintenance in Airflow, tune every Iceberg table, mutate canonical DuckDB, or claim causation from noisy timing alone.

## Dependencies

- Topic 03 must produce populated Iceberg Silver and Gold tables.
- A successful `rtk make build-dbt` must produce the canonical DuckDB file.
- Lakehouse and batch Compose profiles must be healthy for Iceberg maintenance and Trino measurement.

## Exact File Map

| Action | Path | Responsibility |
|---|---|---|
| Create | `src/vina_bim_shop/lakehouse/spark/maintenance.py` | File statistics, invariant snapshots, rewrite procedure calls, query timing, and report payloads. |
| Create | `scripts/lakehouse/optimize_iceberg.py` | CLI for before capture, rewrite, after capture, and evidence output. |
| Create | `tests/unit/test_lakehouse_optimization.py` | SQL generation, target allowlist, invariant comparison, and failure behavior. |
| Create | `scripts/analytics/benchmark_duckdb_index.py` | Isolated database creation, baseline/indexed explains, timing, and evidence writing. |
| Create | `tests/unit/test_duckdb_index_benchmark.py` | Canonical-file protection, index SQL, equality, timing schema, and cleanup behavior. |
| Create | `tmp/rubic-check/runtime/duckdb_index_benchmark.duckdb` | Ignored, disposable benchmark database copied from the canonical analytics database. |
| Modify | `deliverables/04_lakehouse.md` | Row-26 method, table-by-table comparison, limitations, and evidence links. |
| Modify | `deliverables/10_duckdb_dbt_local_analytics.md` | Row-27 index method, plans, timings, and honest interpretation. |
| Create | `evidence/04_lakehouse/optimization/before_file_stats.csv` | Per-table file count and byte distribution before rewrite. |
| Create | `evidence/04_lakehouse/optimization/after_file_stats.csv` | Matching statistics after rewrite. |
| Create | `evidence/04_lakehouse/optimization/compaction_results.json` | Procedure outputs, rewrite counts, durations, and invariant results. |
| Create | `evidence/04_lakehouse/optimization/query_benchmark.json` | Warmup/measured policy, raw timings, medians, and result hashes. |
| Create | `evidence/04_lakehouse/optimization/report.md` | Row-26 optimized/unoptimized analysis. |
| Create | `evidence/04_lakehouse/optimization/run_manifest.json` | Commands, versions, tables, and artifacts. |
| Create | `evidence/10_duckdb_dbt_local_analytics/index_optimization/baseline_explain.txt` | Explain output before index creation. |
| Create | `evidence/10_duckdb_dbt_local_analytics/index_optimization/indexed_explain.txt` | Explain output after index creation. |
| Create | `evidence/10_duckdb_dbt_local_analytics/index_optimization/index_benchmark.json` | Index metadata, equality result, raw timings, and medians. |
| Create | `evidence/10_duckdb_dbt_local_analytics/index_optimization/report.md` | Row-27 analysis and limitations. |
| Create | `evidence/10_duckdb_dbt_local_analytics/index_optimization/run_manifest.json` | Canonical source hash, temporary database path, query, and artifacts. |
| Test | `tests/unit/test_lakehouse_optimization.py` | Iceberg maintenance contract. |
| Test | `tests/unit/test_duckdb_index_benchmark.py` | DuckDB isolation and benchmark contract. |
| Test | `tests/unit/test_spark_batch_runtime.py` | Canonical Spark regression. |
| Test | `tests/unit/test_section02_schema_design.py` | dbt schema regression. |
| Regenerate | `evidence/04_lakehouse/optimization/` | Re-run compaction against a freshly rebuilt multi-file dataset. |
| Regenerate | `evidence/10_duckdb_dbt_local_analytics/index_optimization/` | Rebuild the isolated benchmark from the current canonical DuckDB file. |

## Interfaces and Evidence Contract

- `TABLE_ALLOWLIST` is exactly `silver.stg_orders`, `silver.stg_order_items`, `gold.fact_order`, and `gold.fact_order_item`.
- `collect_file_stats(spark, table_name) -> dict[str, object]` returns file count, total bytes, min/max/average bytes, row count, and deterministic aggregate hash.
- `rewrite_data_files(spark, table_name, target_file_size_bytes=134217728, min_input_files=2) -> dict[str, object]` calls the Iceberg procedure and returns its rewrite counts.
- `compare_invariants(before, after)` exits nonzero on any row-count or aggregate-hash change.
- `benchmark_index(source_db, benchmark_db, evidence_root) -> dict[str, object]` refuses identical source/destination paths, creates `benchmark_fact_order`, queries one fixed `order_id`, creates `idx_benchmark_fact_order_order_id`, and records two warmups plus seven measured runs per variant.
- Timing JSON stores all raw samples, median milliseconds, result rows, result hash, index metadata from `duckdb_indexes()`, and whether the explain plan visibly changed.

## Ordered Tasks

### Task 1: Define Iceberg maintenance invariants

**Files:**
- Create: `tests/unit/test_lakehouse_optimization.py`
- Create: `src/vina_bim_shop/lakehouse/spark/maintenance.py`

- [ ] Test that only the four allowlisted tables are accepted.
- [ ] Test generated procedure SQL contains target size `134217728` and minimum input files `2`.
- [ ] Test invariant comparison accepts file-layout changes but rejects row-count or aggregate-hash changes.
- [ ] Implement the minimum pure helpers and mocked Spark call boundary.
- [ ] Run `rtk uv run pytest tests/unit/test_lakehouse_optimization.py -q`.

Expected: focused tests pass without requiring a live cluster.

### Task 2: Implement and run Iceberg compaction evidence

**Files:**
- Create: `scripts/lakehouse/optimize_iceberg.py`
- Create: `evidence/04_lakehouse/optimization/`

- [ ] Make the CLI write all before statistics before issuing any rewrite call.
- [ ] Add `--tables`, `--target-file-size-bytes`, `--min-input-files`, and `--evidence-root`; reject non-allowlisted values.
- [ ] Build a fresh coursework dataset and run at least two batch windows so target tables contain multiple data files.
- [ ] Start services with `rtk docker compose --profile lakehouse --profile batch up -d --build`.
- [ ] Run the CLI inside Spark with `rtk docker compose exec -T spark-master bash -lc "cd /workspace && PYTHONPATH=/workspace/src spark-submit scripts/lakehouse/optimize_iceberg.py --tables silver.stg_orders silver.stg_order_items gold.fact_order gold.fact_order_item --target-file-size-bytes 134217728 --min-input-files 2 --evidence-root evidence/04_lakehouse/optimization"`.
- [ ] Require at least one rewritten file overall, non-increasing file counts per rewritten table, and passing invariants for all tables.
- [ ] Run two warmups and seven measured Trino queries before and after; store all samples and result hashes.

Expected: compaction evidence proves physical layout changed while logical results remained identical; timing interpretation reflects observed data.

### Task 3: Define and implement the isolated DuckDB index benchmark

**Files:**
- Create: `tests/unit/test_duckdb_index_benchmark.py`
- Create: `scripts/analytics/benchmark_duckdb_index.py`

- [ ] Test that source and benchmark paths cannot be equal and canonical database writes are rejected.
- [ ] Test the fixed index name and column: `idx_benchmark_fact_order_order_id` on `order_id`.
- [ ] Test output includes source SHA-256, both explains, index metadata, fourteen measured samples, medians, equality, and result hashes.
- [ ] Implement a fresh benchmark database that materializes `gold.fact_order` from the read-only source connection.
- [ ] Use one deterministic existing `order_id` for both variants and require identical result rows and hashes.
- [ ] Run `rtk uv run pytest tests/unit/test_duckdb_index_benchmark.py -q`.

Expected: focused tests prove isolation, repeatability, and output schema.

### Task 4: Execute the DuckDB experiment and document both rows

**Files:**
- Regenerate: `evidence/10_duckdb_dbt_local_analytics/index_optimization/`
- Modify: `deliverables/04_lakehouse.md`
- Modify: `deliverables/10_duckdb_dbt_local_analytics.md`

- [ ] Build current dbt output with `rtk uv run dbt build --project-dir infra/analytics/dbt --profiles-dir infra/analytics/dbt`.
- [ ] Run `rtk uv run python scripts/analytics/benchmark_duckdb_index.py --source-db data/gold/vina_bim_shop.duckdb --benchmark-db tmp/rubic-check/runtime/duckdb_index_benchmark.duckdb --evidence-root evidence/10_duckdb_dbt_local_analytics/index_optimization`.
- [ ] Verify `duckdb_indexes()` contains the named index and baseline/indexed result hashes match.
- [ ] Document whether the physical plan changed and whether the median moved; do not rewrite an inconclusive result as a performance gain.
- [ ] Add row-26 and row-27 proof tables with direct code and evidence links.

Expected: both rubric rows have reproducible code, optimized/unoptimized artifacts, and a measured interpretation.

### Task 5: Run regression verification

- [ ] Run `rtk uv run pytest tests/unit/test_lakehouse_optimization.py tests/unit/test_duckdb_index_benchmark.py tests/unit/test_spark_batch_runtime.py tests/unit/test_section02_schema_design.py -q`.
- [ ] Run `rtk uv run pytest -q`.
- [ ] Run `rtk git status --short` and confirm only authorized files plus pre-existing user changes appear.

Expected: focused and full suites pass, with no canonical data-contract regression.

## Required Evidence

- Iceberg before/after file statistics, compaction procedure results, invariant checks, and query benchmark.
- DuckDB baseline/indexed explain plans, index inventory, result equality, raw timing samples, and medians.
- Two reports and two run manifests with exact commands and version context.

## Definition of Done

- At least one Iceberg data file is genuinely rewritten and all logical invariants pass.
- The temporary DuckDB database contains the named index; canonical DuckDB remains byte-for-byte untouched by the benchmark script.
- Both rows have before/after analysis, even when a local timing result is neutral.
- Focused and full tests pass and evidence commands are reproducible.
- Audit status changes only after all named artifacts exist.

## Completion Record

The implementing session records date, source-data scale, table rewrite counts, invariant hashes, query samples/medians, canonical DuckDB hashes before and after, test outputs, artifacts, and limitations here. Unchecked tasks remain authoritative until then.
