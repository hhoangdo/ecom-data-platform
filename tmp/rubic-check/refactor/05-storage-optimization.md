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
| 26 | Optimize Lakehouse storage and compare optimized versus unoptimized. | 2 | Complete: controlled two-bucket profile produced and compacted eligible Gold file groups | M | High |
| 27 | Optimize data warehouse, for example indexing, with code and analysis. | 2 | Complete: isolated ART-index evidence regenerated | S | Medium |

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

## Completion Record — 2026-07-13

**Status:** Rows 26 and 27 are complete. Row 26 has a controlled smoke-scale compaction result; neither Row 26 nor Row 27 claims a general performance improvement.

### Controlled Inputs and Runtime

- Regenerated only smoke local input with `rtk uv run python scripts/generate/run_generator.py --scale smoke --mode full --clean --raw-root data/raw --evidence-root tmp/topic05-remediation-smoke-evidence`. The generator evidence records 800 customers, 1,800 orders, 6,891 raw order-item rows, and 1,800 payments.
- Rebuilt the canonical post-Topic-06 dbt database using `rtk make build-dbt`; all 52 models and 66 tests completed successfully. Topic 06's final feature contract is therefore included in this build; another dbt rebuild is needed only if Topic 06 changes again.
- Claimed the shared lakehouse/batch runtime and preflighted all required persisted `2026-04-26` and `2026-05-01` Bronze objects: ten batch datasets plus `commerce_events`, `catalog_events`, `fulfillment_events`, and `ops_events`.
- Ran one canonical full backfill through `scripts/spark/job.py` for `2026-04-21T00:00:00Z` to `2026-05-04T00:00:00Z`, with `--layout-profile compaction-evidence`. The disabled-by-default profile hashes only `fact_order` and `fact_order_item` into two temporary writer buckets, restores its session-only `spark.sql.iceberg.distribution-mode=none` setting, and preserves the existing schemas, partition specs, and business rows. Spark and GX validation both passed.
- The seeded Iceberg row counts were 1,800 for `fact_order` and 6,756 for `fact_order_item`; the backfill manifest is [here](../../../evidence/04_lakehouse/optimization/seed_batch/spark_job_manifest.json).

### Iceberg Allowlist, Results, and Invariants

The fixed allowlist was `silver.stg_orders`, `silver.stg_order_items`, `gold.fact_order`, and `gold.fact_order_item`; the procedure policy remained a 134,217,728-byte target with `min-input-files=2`.

| Table | Files before → after | Rewritten files | Rows | Aggregate hash |
| --- | ---: | ---: | ---: | --- |
| `silver.stg_orders` | 1 → 1 | 0 (skipped: insufficient files) | 1,800 | `6fd2502b6e457d25a05d71d1cc1f2cf6745a8598d51f169ae3152f4644d6d6bf` |
| `silver.stg_order_items` | 1 → 1 | 0 (skipped: insufficient files) | 6,756 | `ca567c19b9906f2c8602b87ed2220b7a9757908664f0df23a19092fd70e22d44` |
| `gold.fact_order` | 13 → 7 | 12 | 1,800 | `f4002002c04ef6f1e9fdfc8d26222bf6c6dc94f86c2d8c3bdd6cd218bb185425` |
| `gold.fact_order_item` | 13 → 7 | 12 | 6,756 | `9f4abc6265dc9ca11f3b6ec191f8aeec5ee590ef0cd19f85fe32bfa66be109e1` |

All four before/after row-count and aggregate-hash invariants passed. The controlled profile made six `order_date_key` partitions eligible in each Gold fact; the Iceberg procedure rewrote 12 files for each fact and reduced both to seven files. It did not lower the two-file threshold, use `rewrite-all`, or alter a persistent table property. The earlier generic writer-limit attempt is preserved under `evidence/04_lakehouse/optimization/attempts/2026-07-11-generic-writer-limit-blocked/`.

Trino has two warmups and seven measured samples per table in both phases, with equal before/after result hashes: `5fdcd793148082c0becabf6d346d794be4f1c85711fdb2752e404c275338f69d` (`stg_orders`), `fa41b1b8374d8d2fe42fb5e18d47b74f2746d74aa7b24d7087098090b5542947` (`stg_order_items`), `3b90748fc1a3afbe9713761928ff791fe46e6b2b9fc5f37db5222c0884724702` (`fact_order`), and `7105276d9a49978ad2b981917d166ff56ec9e427249e238c7c3902d6c28b0ffb` (`fact_order_item`). The before/after median client times were 211.490/137.492 ms, 174.374/137.388 ms, 158.547/128.030 ms, and 158.560/137.837 ms respectively. These smoke-scale local observations are not a storage-performance claim.

Iceberg artifacts:

- [before file stats](../../../evidence/04_lakehouse/optimization/before_file_stats.csv) and [after file stats](../../../evidence/04_lakehouse/optimization/after_file_stats.csv)
- [controlled-layout manifest](../../../evidence/04_lakehouse/optimization/seed_batch/compaction_layout_manifest.json), [procedure and invariant result](../../../evidence/04_lakehouse/optimization/compaction_results.json), [raw Trino samples](../../../evidence/04_lakehouse/optimization/query_benchmark.json), [report](../../../evidence/04_lakehouse/optimization/report.md), and [manifest](../../../evidence/04_lakehouse/optimization/run_manifest.json)

### Isolated DuckDB ART Index Experiment

The benchmark used only `tmp/rubic-check/runtime/duckdb_index_benchmark.duckdb`, a disposable materialization of `gold.fact_order`; it did not create or drop an index in `data/gold/vina_bim_shop.duckdb`. The canonical SHA-256 was equal before and after: `b0bb76a932bb1686259b332e50130fb70fb513cd6e1b6b731207c032ccb7b438`.

- Named index: `idx_benchmark_fact_order_order_id` on `benchmark_fact_order(order_id)`; confirmed by `duckdb_indexes()`.
- Fixed result hash before/after: `ecfe42c8ba49a03ee49642ccc7a043cdba1230fecebc2602c69b73ee2c74771f` for `ORD-BDG-20260426-00000006`.
- Timing policy: two warmups and seven measured samples per variant. Baseline samples were `1.4715, 1.3721, 1.4490, 1.4326, 1.4292, 1.3582, 1.2887` ms (median 1.4292 ms); indexed samples were `1.1436, 1.0804, 1.0631, 1.1219, 1.1242, 1.1611, 1.1119` ms (median 1.1219 ms).
- The captured explain text did not visibly change. The lower indexed median is an observed local sample only, not a general ART-index speed claim.

DuckDB artifacts:

- [benchmark JSON](../../../evidence/10_duckdb_dbt_local_analytics/index_optimization/index_benchmark.json), [baseline explain](../../../evidence/10_duckdb_dbt_local_analytics/index_optimization/baseline_explain.txt), [indexed explain](../../../evidence/10_duckdb_dbt_local_analytics/index_optimization/indexed_explain.txt), and [manifest](../../../evidence/10_duckdb_dbt_local_analytics/index_optimization/run_manifest.json)

### Implementation and Limitations

- The maintenance helper continues to label a zero-result Iceberg procedure as `skipped_no_eligible_file_groups`. The canonical job now additionally exposes a disabled-by-default `compaction-evidence` profile that creates two deterministic writer buckets only for the two approved Gold fact tables and records its physical layout.
- The source is local smoke scale. The profile exists solely to make the required physical compaction precondition reproducible; it is not a production writer-tuning recommendation.
- No canonical DuckDB file was mutated by the index experiment. No unsupported storage or index performance claim is made.

### Verification and Runtime Release

- Focused Topic-05, lakehouse configuration/profile, Spark runtime, script-inventory, DuckDB-index, and schema tests: **69 passed**.
- Full regression: **356 passed, 1 skipped**.
- Evidence inventory check passed: all required active and archived Iceberg artifacts exist; the controlled layout has two buckets and eligible partitions; both Gold rewrites are genuine; all logical invariants and before/after Trino result hashes match; every stored variant has two warmups and seven measured samples; the ART index is present; and the canonical DuckDB SHA-256/result hashes match.
- Stopped only the lakehouse/batch Compose services started for this session with `rtk docker compose --profile lakehouse --profile batch stop`. `docker compose ps` then returned no running services. Volumes and evidence were preserved, and the shared runtime slot was released.
