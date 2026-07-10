# Spark Skew, Cardinality, and Baseline Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a controlled coursework-scale Spark baseline-versus-optimized experiment for rows 15 through 20 that proves targeted salting and repartitioning without changing canonical batch semantics.

**Architecture:** A new, standalone Spark experiment builds deterministic coursework-scale input from the existing generator profile. Four exact variants (`skew-baseline`, `skew-optimized`, `high-cardinality-baseline`, and `high-cardinality-optimized`) run as four separate `spark-submit` processes so Spark creates four application IDs and four History Server pages. Optimized variants compare their exact result payloads with the corresponding baseline artifacts; the canonical `run_job` Bronze-to-Silver-to-Gold path remains untouched.

**Tech Stack:** PySpark 4.0.0, Spark event logs and History Server, Docker Compose, Python, PyYAML, pytest, Airflow documentation.

## Global Constraints

- This is an implementation plan only; do not treat proposed experiments, screenshots, or metric files as already produced.
- Preserve user work. Do not modify, delete, revert, stage, or commit unrelated files.
- Do not stage or commit automatically. Staging and commits require an explicit user request.
- Use ASCII text and `apply_patch` for every repository write.
- Run experiments from the existing `coursework` profile: 90 history days, 50,000 customers, 30,000 products, and 150,000 orders.
- Use the exact hot keys `Ho Chi Minh City` and `Ha Noi`; no city normalization or replacement keys are permitted.
- Use deterministic salting `pmod(xxhash64(customer_id), 16)` only for those two hot keys and salt bucket `0` for every other city.
- Experiments must never call or modify canonical `run_job`, `_persist_silver_tables_for_window`, `_persist_gold_tables`, or the production Iceberg tables.
- Experiment modules and scripts must not import `vina_bim_shop.lakehouse.spark.job` or import/call `run_job` by any alias.
- Disable adaptive query execution for both compared runs so measured repartitioning is not silently rewritten by Spark.

## Rubric Coverage

| Row | Requirement | Points | Current status | Effort | Value added |
|---:|---|---:|---|---|---|
| 15 | Establish a Spark baseline without optimization and explain optimization steps, Spark UI proof, and Airflow integration. | 2 | Partial: runtime and historical UI images exist without a controlled comparison. | M | High |
| 16 | Handle skew with explanation and evidence of improvement. | 2 | Partial: documentation explicitly states no salting exists. | M | High |
| 17 | Handle high cardinality with explanation and optimization proof. | 2 | Partial: natural IDs exist without measured mitigation. | M | High |
| 18 | Handle schema evolution with explanation and proof. | 2 | Satisfied, but not closed in the row-15-to-20 comparison package. | XS | Medium |
| 19 | Handle another offline problem with explanation and proof. | 2 | Satisfied, but not closed in the comparison package. | XS | Medium |
| 20 | Integrate the Spark job into data pipelines. | 2 | Satisfied, but the Spark deliverable does not name the Airflow task as row-20 proof. | XS | Medium |

## Current Implementation and Evidence

- Existing `src/vina_bim_shop/lakehouse/spark/job.py` reads Bronze inputs, deduplicates Silver tables with `_dedupe_latest`, preserves `schema_version` on event tables, builds quarantine tables, and writes canonical Iceberg Silver and Gold tables.
- Existing `src/vina_bim_shop/lakehouse/spark/runner.py` submits canonical batch jobs with Spark event logging enabled at `s3a://checkpoints/spark-events`.
- Existing `scripts/spark/run_batch.py` is the canonical batch wrapper; it must remain a production-semantic wrapper.
- Existing `scripts/spark/capture_evidence.py` captures Spark master and History Server JSON, and existing screenshots are historical runtime evidence.
- Existing `infra/orchestration/airflow/dags/hourly_batch_lakehouse.py` exposes `run_hourly_batch_window`, which calls `run_hourly_batch_lakehouse` and then the canonical `run_batch_pipeline`.

## Gap, Scope, Non-Goals, and Dependencies

**Gap:** There is no controlled baseline, no deterministic Spark salting experiment, no cardinality-specific repartition measurement, and no result-equivalence or UI proof that ties those experiments to rows 15 through 20.

**Scope:** Add a standalone experiment module and script, unit tests, metrics/equivalence artifacts, UI capture instructions, and precise documentation closures for rows 15 through 20.

**Non-goals:** Do not replace canonical joins, change Iceberg partitioning, persist experiment tables, alter source data distributions, add salting to the production batch path, or claim that elapsed-time reduction is guaranteed on every local machine. The required correctness result is equivalence; performance is reported as measured.

**Dependencies:** Docker Compose batch and lakehouse profiles, the Spark History Server, MinIO checkpoint bucket, the existing generator config parser, local Spark test support, and a browser capable of saving Spark UI screenshots.

## Exact File Map

| Action | Path | Responsibility |
|---|---|---|
| Create | `src/vina_bim_shop/lakehouse/spark/optimization_experiments.py` | Build deterministic coursework-scale experiment data, run baseline and optimized skew/cardinality paths, assert equivalence, and write metrics. |
| Modify | `src/vina_bim_shop/lakehouse/spark/optimization_experiments.py` | Add the high-cardinality paths after the skew contract is independently reviewed. |
| Create | `scripts/spark/run_optimization_experiments.py` | Parse one required variant per process and invoke only that standalone experiment variant. |
| Create | `tests/unit/test_spark_optimization_experiments.py` | Verify variant contracts, exact hot keys, deterministic salt buckets, all-ID equivalence, and the canonical source boundary. |
| Modify | `deliverables/05_spark_batch.md` | Document baseline, optimization mechanics, measured evidence, screenshots, and the Airflow row-20 integration closure. |
| Modify | `deliverables/11_solving_data_challenges.md` | Replace the old future-only skew note with an explicit experiment boundary and link schema/duplicate/quarantine closures. |
| Create | `evidence/05_spark_batch/optimization/skew_baseline_metrics.json` | Baseline hot-key aggregation elapsed time, partition count, input rows, and per-city result. |
| Create | `evidence/05_spark_batch/optimization/skew_optimized_metrics.json` | Salted/repartitioned aggregation elapsed time, partition count, input rows, salt bucket count, and per-city result. |
| Create | `evidence/05_spark_batch/optimization/skew_equivalence.json` | Exact equality assertion for baseline and optimized city aggregates. |
| Create | `evidence/05_spark_batch/optimization/high_cardinality_baseline_metrics.json` | Baseline approximate and exact distinct counts for all four IDs, repartition count, elapsed time, and application ID. |
| Create | `evidence/05_spark_batch/optimization/high_cardinality_optimized_metrics.json` | Repartitioned approximate and exact distinct counts for all four IDs, elapsed time, and application ID. |
| Create | `evidence/05_spark_batch/optimization/high_cardinality_equivalence.json` | Per-ID exact distinct equality assertion for baseline and optimized runs. |
| Create | `evidence/05_spark_batch/optimization/optimization_report.md` | Controlled-method narrative, baseline/optimized comparison tables, and row-18-to-20 proof links. |
| Create | `evidence/05_spark_batch/optimization/run_manifest.json` | Four exact variant names, four application IDs, artifact paths, screenshot paths, and canonical-semantics boundary. |
| Create | `evidence/05_spark_batch/screenshots/spark_skew_baseline_history.png` | Spark History Server view for the baseline application. |
| Create | `evidence/05_spark_batch/screenshots/spark_skew_optimized_history.png` | Spark History Server view for the salted/repartitioned application. |
| Create | `evidence/05_spark_batch/screenshots/spark_high_cardinality_baseline_history.png` | Spark History Server view for the high-cardinality baseline application. |
| Create | `evidence/05_spark_batch/screenshots/spark_high_cardinality_optimized_history.png` | Spark History Server view for the high-cardinality optimized application. |
| Test | `tests/unit/test_spark_optimization_experiments.py` | Focused variant, salting, equivalence, and source-boundary tests. |
| Test | `tests/unit/test_spark_batch_runtime.py` | Canonical Spark runner regression coverage. |
| Test | `tests/unit/test_spark_challenge_handling.py` | Existing schema, duplicate, and quarantine regression coverage. |
| Test | `tests/unit/test_deliverables_documentation.py` | Spark deliverable link and wording regression coverage. |
| Regenerate | `evidence/05_spark_batch/optimization/skew_baseline_metrics.json` | Re-run `skew-baseline`. |
| Regenerate | `evidence/05_spark_batch/optimization/skew_optimized_metrics.json` | Re-run `skew-optimized`. |
| Regenerate | `evidence/05_spark_batch/optimization/skew_equivalence.json` | Recompare the two skew result payloads. |
| Regenerate | `evidence/05_spark_batch/optimization/high_cardinality_baseline_metrics.json` | Re-run `high-cardinality-baseline`. |
| Regenerate | `evidence/05_spark_batch/optimization/high_cardinality_optimized_metrics.json` | Re-run `high-cardinality-optimized`. |
| Regenerate | `evidence/05_spark_batch/optimization/high_cardinality_equivalence.json` | Recompare all four exact distinct counts. |
| Regenerate | `evidence/05_spark_batch/optimization/optimization_report.md` | Refresh measured comparison and row-closure narrative. |
| Regenerate | `evidence/05_spark_batch/optimization/run_manifest.json` | Refresh all four application/artifact mappings. |
| Regenerate | `evidence/05_spark_batch/screenshots/spark_skew_baseline_history.png` | Re-capture the `skew-baseline` History Server page. |
| Regenerate | `evidence/05_spark_batch/screenshots/spark_skew_optimized_history.png` | Re-capture the `skew-optimized` History Server page. |
| Regenerate | `evidence/05_spark_batch/screenshots/spark_high_cardinality_baseline_history.png` | Re-capture the `high-cardinality-baseline` History Server page. |
| Regenerate | `evidence/05_spark_batch/screenshots/spark_high_cardinality_optimized_history.png` | Re-capture the `high-cardinality-optimized` History Server page. |
| Regenerate | `evidence/05_spark_batch/spark_master_status.json` | Refresh Spark master API evidence after all four submissions. |
| Regenerate | `evidence/05_spark_batch/spark_history_applications.json` | Refresh History Server API evidence containing all four applications. |
| Regenerate | `evidence/05_spark_batch/run_manifest.json` | Refresh the existing Spark evidence inventory. |

## Interfaces and Outputs

- `run_optimization_variant(config_path, scale, variant, evidence_root) -> dict[str, object]` accepts exactly one of `skew-baseline`, `skew-optimized`, `high-cardinality-baseline`, or `high-cardinality-optimized`; one process creates one Spark application and writes only that variant's metric plus any pairwise equivalence artifact unlocked by the run.
- Variant-to-application names are exact: `skew-baseline` -> `vina-bim-shop-optimization-skew-baseline`, `skew-optimized` -> `vina-bim-shop-optimization-skew-optimized`, `high-cardinality-baseline` -> `vina-bim-shop-optimization-high-cardinality-baseline`, and `high-cardinality-optimized` -> `vina-bim-shop-optimization-high-cardinality-optimized`.
- `run_manifest.json` stores a `variants` mapping keyed by those four exact names. Each entry contains `application_id`, `application_name`, `metrics_path`, and `screenshot_path`; there must be four distinct nonempty application IDs before evidence is complete.
- The controlled skew input has 150,000 deterministic rows keyed by `customer_id`, with 27 percent `Ho Chi Minh City`, 18 percent `Ha Noi`, and the remaining configured city weights. It includes a numeric `order_amount` and no Iceberg read or write.
- Baseline skew path: `repartition(16, "city").groupBy("city").agg(sum("order_amount"), count("*"))`.
- Optimized skew path adds `salt_bucket = when(city.isin("Ho Chi Minh City", "Ha Noi"), pmod(xxhash64(customer_id), lit(16))).otherwise(lit(0))`, then uses `repartition(16, "city", "salt_bucket")`, aggregates by city and salt, and re-aggregates by city.
- The skew equivalence assertion compares sorted `city`, `order_count`, and decimal-normalized `order_amount` rows exactly; it fails before writing a success artifact if any row differs.
- The high-cardinality input derives deterministic `customer_id`, `product_id`, `order_id`, and `event_id` columns from the coursework counts. Each variant records `approx_count_distinct` and exact `countDistinct` for all four IDs. `high_cardinality_equivalence.json` compares the exact baseline/optimized value separately for `customer_id`, `product_id`, `order_id`, and `event_id` and reports overall success only when all four match.
- Baseline cardinality path uses `repartition(8, "city")`; optimized path uses `repartition(32, "order_id")`. Both include elapsed milliseconds and `rdd.getNumPartitions()` in metrics.

## Ordered Tasks

### Task 1: Lock the experiment boundary and correctness contract in tests

**Files:**
- Create: `tests/unit/test_spark_optimization_experiments.py`

- [ ] Add a local Spark test fixture with `spark.sql.adaptive.enabled=false` and a temporary evidence root.
- [ ] Add assertions that the experiment constants contain exactly `Ho Chi Minh City`, `Ha Noi`, and `16` salt buckets.
- [ ] Build a small deterministic DataFrame containing both hot cities and one non-hot city; assert hot-city salt buckets are stable across two evaluations and the non-hot city uses bucket `0`.
- [ ] Run baseline and optimized skew aggregations and assert their sorted city/count/amount rows are identical.
- [ ] Run baseline and optimized cardinality paths and assert exact `countDistinct` equality separately for `customer_id`, `product_id`, `order_id`, and `event_id`, while both metrics retain `approx_count_distinct` values for all four IDs.
- [ ] In `test_experiment_sources_cannot_import_or_call_canonical_run_job`, parse both new Python files with `ast`; fail on imports from `vina_bim_shop.lakehouse.spark.job`, imported names or aliases resolving to `run_job`, and calls whose function is `run_job` or an attribute named `run_job`.
- [ ] In the same source-boundary test, assert there are no `iceberg.`, `MERGE INTO`, `CREATE TABLE`, `_persist_silver_tables_for_window`, or `_persist_gold_tables` references.
- [ ] Run `rtk uv run pytest tests/unit/test_spark_optimization_experiments.py -q`.

Expected result: FAIL because the experiment module does not yet exist.

### Task 2: Implement deterministic skew baseline and optimization paths

**Files:**
- Create: `src/vina_bim_shop/lakehouse/spark/optimization_experiments.py`
- Create: `evidence/05_spark_batch/optimization/skew_baseline_metrics.json`
- Create: `evidence/05_spark_batch/optimization/skew_optimized_metrics.json`
- Create: `evidence/05_spark_batch/optimization/skew_equivalence.json`

- [ ] Implement `build_coursework_experiment_frame` using `spark.range(150000)` and configured profile counts/weights; derive deterministic IDs and amounts from the range value instead of reading or writing canonical data.
- [ ] Implement `add_targeted_salt(frame)` with the exact hot-key condition and `pmod(xxhash64(customer_id), 16)` expression.
- [ ] Implement baseline and optimized skew functions that return both a sorted result DataFrame and a metrics dictionary containing `input_rows`, `partition_count`, `elapsed_ms`, `hot_keys`, and, for optimized, `salt_bucket_count`.
- [ ] Make `skew-baseline` write only `skew_baseline_metrics.json`. Make `skew-optimized` read that baseline result payload, write `skew_optimized_metrics.json`, compare sorted city/count/amount rows exactly, and write `skew_equivalence.json` only after the assertion succeeds.
- [ ] Run `rtk uv run pytest tests/unit/test_spark_optimization_experiments.py -q`.

Expected result: PASS; salting is deterministic, affects only the two specified city keys, and preserves the result exactly.

### Task 3: Implement the high-cardinality comparison and CLI

**Files:**
- Modify: `src/vina_bim_shop/lakehouse/spark/optimization_experiments.py`
- Create: `scripts/spark/run_optimization_experiments.py`
- Create: `evidence/05_spark_batch/optimization/high_cardinality_baseline_metrics.json`
- Create: `evidence/05_spark_batch/optimization/high_cardinality_optimized_metrics.json`
- Create: `evidence/05_spark_batch/optimization/high_cardinality_equivalence.json`
- Create: `evidence/05_spark_batch/optimization/run_manifest.json`

- [ ] Implement the baseline cardinality aggregation after `repartition(8, "city")` and the optimized aggregation after `repartition(32, "order_id")`.
- [ ] Record DuckDB-free Spark `approx_count_distinct` and exact `countDistinct` metrics for `customer_id`, `product_id`, `order_id`, and `event_id` in both cardinality metric files.
- [ ] In `high-cardinality-optimized`, read the baseline exact-count payload, compare all four IDs independently, and write `high_cardinality_equivalence.json` only after every equality assertion passes.
- [ ] Add CLI arguments `--config` defaulting to `configs/generator/base.yaml`, `--scale` restricted to `coursework`, `--variant` required with the four exact choices, and `--evidence-root` defaulting to `evidence/05_spark_batch/optimization`.
- [ ] Make each CLI process execute exactly one variant, use the exact application-name mapping in Interfaces and Outputs, capture `spark.sparkContext.applicationId`, and update only that variant entry in the shared manifest.
- [ ] Run `rtk uv run pytest tests/unit/test_spark_optimization_experiments.py tests/unit/test_spark_batch_runtime.py -q`.

Expected result: PASS; all four exact and approximate cardinality metrics are present, repartition counts differ as designed, each process has one variant, and canonical batch runtime contracts are unchanged.

### Task 4: Execute the controlled coursework-scale experiment and capture UI proof

**Files:**
- Regenerate: `evidence/05_spark_batch/optimization/skew_baseline_metrics.json`
- Regenerate: `evidence/05_spark_batch/optimization/skew_optimized_metrics.json`
- Regenerate: `evidence/05_spark_batch/optimization/skew_equivalence.json`
- Regenerate: `evidence/05_spark_batch/optimization/high_cardinality_baseline_metrics.json`
- Regenerate: `evidence/05_spark_batch/optimization/high_cardinality_optimized_metrics.json`
- Regenerate: `evidence/05_spark_batch/optimization/high_cardinality_equivalence.json`
- Regenerate: `evidence/05_spark_batch/optimization/run_manifest.json`
- Create: `evidence/05_spark_batch/screenshots/spark_skew_baseline_history.png`
- Create: `evidence/05_spark_batch/screenshots/spark_skew_optimized_history.png`
- Create: `evidence/05_spark_batch/screenshots/spark_high_cardinality_baseline_history.png`
- Create: `evidence/05_spark_batch/screenshots/spark_high_cardinality_optimized_history.png`
- Regenerate: `evidence/05_spark_batch/spark_master_status.json`
- Regenerate: `evidence/05_spark_batch/spark_history_applications.json`
- Regenerate: `evidence/05_spark_batch/run_manifest.json`

- [ ] Start the runtime with `rtk docker compose --profile batch --profile lakehouse up -d --build`.
- [ ] Submit `skew-baseline` as its own application: `rtk docker compose exec -T spark-master bash -lc "cd /workspace && PYTHONPATH=/workspace/src spark-submit --name vina-bim-shop-optimization-skew-baseline --master spark://spark-master:7077 --deploy-mode client --conf spark.sql.adaptive.enabled=false --conf spark.eventLog.enabled=true --conf spark.eventLog.dir=s3a://checkpoints/spark-events scripts/spark/run_optimization_experiments.py --config configs/generator/base.yaml --scale coursework --variant skew-baseline --evidence-root /workspace/evidence/05_spark_batch/optimization"`.
- [ ] Submit `skew-optimized` as its own application: `rtk docker compose exec -T spark-master bash -lc "cd /workspace && PYTHONPATH=/workspace/src spark-submit --name vina-bim-shop-optimization-skew-optimized --master spark://spark-master:7077 --deploy-mode client --conf spark.sql.adaptive.enabled=false --conf spark.eventLog.enabled=true --conf spark.eventLog.dir=s3a://checkpoints/spark-events scripts/spark/run_optimization_experiments.py --config configs/generator/base.yaml --scale coursework --variant skew-optimized --evidence-root /workspace/evidence/05_spark_batch/optimization"`.
- [ ] Submit `high-cardinality-baseline` as its own application: `rtk docker compose exec -T spark-master bash -lc "cd /workspace && PYTHONPATH=/workspace/src spark-submit --name vina-bim-shop-optimization-high-cardinality-baseline --master spark://spark-master:7077 --deploy-mode client --conf spark.sql.adaptive.enabled=false --conf spark.eventLog.enabled=true --conf spark.eventLog.dir=s3a://checkpoints/spark-events scripts/spark/run_optimization_experiments.py --config configs/generator/base.yaml --scale coursework --variant high-cardinality-baseline --evidence-root /workspace/evidence/05_spark_batch/optimization"`.
- [ ] Submit `high-cardinality-optimized` as its own application: `rtk docker compose exec -T spark-master bash -lc "cd /workspace && PYTHONPATH=/workspace/src spark-submit --name vina-bim-shop-optimization-high-cardinality-optimized --master spark://spark-master:7077 --deploy-mode client --conf spark.sql.adaptive.enabled=false --conf spark.eventLog.enabled=true --conf spark.eventLog.dir=s3a://checkpoints/spark-events scripts/spark/run_optimization_experiments.py --config configs/generator/base.yaml --scale coursework --variant high-cardinality-optimized --evidence-root /workspace/evidence/05_spark_batch/optimization"`.
- [ ] Verify the manifest has exactly the four variant keys and four distinct nonempty application IDs; verify both equivalence JSON files report `success: true`, all four high-cardinality exact comparisons match, optimized metrics record required repartition/salt values, and the manifest states `canonical_batch_semantics_changed: false`.
- [ ] In `http://localhost:18080`, open the Jobs tab for the application ID stored under manifest key `skew-baseline` and save the completed page as `evidence/05_spark_batch/screenshots/spark_skew_baseline_history.png`.
- [ ] Open the Jobs tab for the application ID stored under `skew-optimized` and save the completed page as `evidence/05_spark_batch/screenshots/spark_skew_optimized_history.png`.
- [ ] Open the Jobs tab for the application ID stored under `high-cardinality-baseline` and save the completed page as `evidence/05_spark_batch/screenshots/spark_high_cardinality_baseline_history.png`.
- [ ] Open the Jobs tab for the application ID stored under `high-cardinality-optimized` and save the completed page as `evidence/05_spark_batch/screenshots/spark_high_cardinality_optimized_history.png`.
- [ ] Run `rtk uv run python scripts/spark/capture_evidence.py --evidence-root evidence/05_spark_batch` after the experiment so master/history JSON reflects the current runtime.

Expected result: four separate `spark-submit` commands produce four distinct application IDs, four History Server pages, four matching screenshots, and passing skew plus all-ID cardinality equivalence artifacts.

### Task 5: Document rows 15 through 20 and run regression tests

**Files:**
- Modify: `deliverables/05_spark_batch.md`
- Modify: `deliverables/11_solving_data_challenges.md`
- Create: `evidence/05_spark_batch/optimization/optimization_report.md`
- Test: `tests/unit/test_spark_optimization_experiments.py`
- Test: `tests/unit/test_spark_batch_runtime.py`
- Test: `tests/unit/test_spark_challenge_handling.py`
- Test: `tests/unit/test_deliverables_documentation.py`

- [ ] In `optimization_report.md`, state the controlled input, AQE-disabled comparison condition, exact baseline and optimized transformations, measured elapsed-time fields, partition counts, and equivalence outcomes. Report any elapsed-time change as observed, not guaranteed.
- [ ] Add a row-15-to-20 table to `deliverables/05_spark_batch.md` linking each result JSON and screenshot. Name `hourly_batch_lakehouse.run_hourly_batch_window` as the Airflow entry point and explain that it still invokes the unchanged canonical `run_batch_pipeline`.
- [ ] Update the Spark skew section in `deliverables/11_solving_data_challenges.md` to distinguish the standalone experiment from production semantics. Link row 18 to `schema_version` preservation and nullable JSON extraction; link row 19 to `_dedupe_latest`, `raw_bad_events`, and `raw_bad_snapshots` quarantine handling.
- [ ] Run `rtk uv run pytest tests/unit/test_spark_optimization_experiments.py tests/unit/test_spark_batch_runtime.py tests/unit/test_spark_challenge_handling.py tests/unit/test_deliverables_documentation.py -q`.

Expected result: documentation accurately reports a controlled experiment, preserves the production boundary, and regression contracts pass.

## Required Evidence Artifacts

- Create: `evidence/05_spark_batch/optimization/skew_baseline_metrics.json`
- Create: `evidence/05_spark_batch/optimization/skew_optimized_metrics.json`
- Create: `evidence/05_spark_batch/optimization/skew_equivalence.json`
- Create: `evidence/05_spark_batch/optimization/high_cardinality_baseline_metrics.json`
- Create: `evidence/05_spark_batch/optimization/high_cardinality_optimized_metrics.json`
- Create: `evidence/05_spark_batch/optimization/high_cardinality_equivalence.json`
- Create: `evidence/05_spark_batch/optimization/optimization_report.md`
- Create: `evidence/05_spark_batch/optimization/run_manifest.json`
- Create: `evidence/05_spark_batch/screenshots/spark_skew_baseline_history.png`
- Create: `evidence/05_spark_batch/screenshots/spark_skew_optimized_history.png`
- Create: `evidence/05_spark_batch/screenshots/spark_high_cardinality_baseline_history.png`
- Create: `evidence/05_spark_batch/screenshots/spark_high_cardinality_optimized_history.png`

## Definition of Done

- Coursework-scale deterministic baseline and optimized Spark runs complete with AQE disabled and event logs enabled.
- The two exact city keys are salted deterministically into 16 buckets, while all other cities use bucket 0.
- Baseline and optimized skew outputs are exactly equivalent, and high-cardinality paths record all four approximate-distinct metrics plus exact baseline/optimized equality for `customer_id`, `product_id`, `order_id`, and `event_id`.
- Measured repartition counts, elapsed times, four distinct event-log application IDs, and four History Server screenshots are stored as evidence without claiming a guaranteed speedup.
- Documentation closes rows 15 through 20, including schema evolution, duplicate/quarantine, and Airflow integration proof, while stating that canonical production semantics are unchanged.
- Focused Spark tests and Spark/documentation regression tests pass.
- No staging or commit occurs unless the user explicitly requests it.

## Completion Record

Completed on 2026-07-10 in the current repository folder on branch `feature/finalize-edai1`.
No files were staged or committed.

### Implementation Notes

- Added a standalone `optimization_experiments.py` module and one-variant CLI. Neither source imports or calls the canonical batch entry point, canonical persistence helpers, or table DDL.
- The controlled input is generated from the `coursework` profile: 150,000 rows, 50,000 customer IDs, 30,000 product IDs, and one deterministic order/event ID per row.
- Host pytest has no PySpark dependency, so source-boundary, CLI, configuration, and documentation contracts are static tests; Spark transformation correctness was executed by the four real `spark-submit` applications below.
- The existing no-salting policy test now applies only to canonical batch sources. Documentation distinguishes that production boundary from the standalone experiment.
- Root `capture_evidence` adds `optimization/run_manifest.json` when the experiment manifest exists.

### Successful Evidence-Bearing Applications

| Variant | Application ID | Elapsed ms | Repartition proof | History Server screenshot |
| --- | --- | ---: | --- | --- |
| `skew-baseline` | `app-20260710172933-0001` | 1412.994 | 16 partitions by `city`; maximum partition rows 40,500 | `evidence/05_spark_batch/screenshots/spark_skew_baseline_history.png` |
| `skew-optimized` | `app-20260710173018-0002` | 1392.164 | 16 partitions by `city,salt_bucket`; maximum partition rows 30,669 | `evidence/05_spark_batch/screenshots/spark_skew_optimized_history.png` |
| `high-cardinality-baseline` | `app-20260710173101-0003` | 7337.401 | 8 partitions by `city`; maximum partition rows 40,500 | `evidence/05_spark_batch/screenshots/spark_high_cardinality_baseline_history.png` |
| `high-cardinality-optimized` | `app-20260710173148-0004` | 8512.983 | 32 partitions by `order_id`; maximum partition rows 4,825 | `evidence/05_spark_batch/screenshots/spark_high_cardinality_optimized_history.png` |

Each application used AQE disabled, event logging enabled, and `s3a://checkpoints/spark-events`. The optimization manifest contains exactly these four distinct IDs and records `canonical_batch_semantics_changed: false`.

### Correctness Results

- `skew_equivalence.json` reports `success: true`: sorted city/count/fixed-precision amount results are exactly equal.
- The two hot cities each use all deterministic salt buckets `0` through `15`; every other configured city uses bucket `0` only.
- `high_cardinality_equivalence.json` reports `success: true`. Exact baseline/optimized counts are customer 50,000, product 30,000, order 150,000, and event 150,000.
- Approximate-distinct values are retained in both cardinality metrics: customer 49,401, product 31,069, order 142,797, and event 146,766.
- Root History Server evidence contains all four successful IDs and the root evidence manifest inventories `optimization/run_manifest.json`.

### Commands And Test Results

- `rtk git status --short` before editing: clean worktree.
- Initial new contract test: `rtk uv run pytest tests/unit/test_spark_optimization_experiments.py -q` -> `3 failed` because the experiment sources did not exist.
- After implementation and compatibility tests: `rtk uv run pytest tests/unit/test_spark_optimization_experiments.py tests/unit/test_spark_batch_runtime.py tests/unit/test_spark_challenge_handling.py tests/unit/test_deliverables_documentation.py -q` -> `48 passed`.
- Four successful `spark-submit` commands were run sequentially through the batch profile; `rtk uv run python scripts/spark/capture_evidence.py --evidence-root evidence/05_spark_batch` completed with 13 artifacts.
- Post-run JSON assertion verified four variants/IDs, exact skew and cardinality equality, targeted salt buckets, screenshot files, History Server IDs, and root-manifest inventory.
- Full regression: `rtk uv run pytest -q` -> `284 passed, 1 skipped, 3 failed in 98.74s`.
- `rtk docker compose --profile batch --profile lakehouse down` stopped and removed the runtime containers and network after evidence capture.

### Limitations And Follow-Up

- The first attempted baseline driver (`app-20260710172816-0000`) failed before metrics because the initial city-expression chain used `otherwise` more than once. A red test was added, the expression was corrected to chain `when`, and the successful baseline above was rerun. The failed driver has no optimization artifact and is not in the four-variant manifest.
- Background execution initially split the quoted `bash -lc` payload through PowerShell. The command was corrected to pass the Docker invocation as one argument string; this is an execution-wrapper correction, not a production semantic change.
- The full-suite failures are outside Topic 03: `scripts/README.md` does not document the already-tracked `scripts/kafka/capture_connect_image_optimization.py`, and two Section 02 tests require the absent gitignored `data/gold/vina_bim_shop.duckdb`. Neither was modified in this topic.
- The local single-worker measurements are observed evidence only. Skew partition balance improved, while the high-cardinality optimized run was slower; no speedup is claimed.
