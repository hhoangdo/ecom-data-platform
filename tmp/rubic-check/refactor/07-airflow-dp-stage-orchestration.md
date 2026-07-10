# Airflow DP Stage Orchestration Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Expose DP1, DP2, and DP3 as six real Airflow ingest/validate tasks with configured connections, deterministic dependencies, successful runtime evidence, and readable UI proof.

**Architecture:** Add a dedicated `mini_coursework_pipeline` DAG with three TaskGroups and two Python tasks per group. Refactor the existing pure orchestration/Spark runtime into idempotent stage functions, preserve the current one-call wrapper for compatibility, and seed all external endpoints and execution parameters into Airflow metadata.

**Tech Stack:** Airflow 2.10.5, Python 3.12, Spark, Iceberg, Trino, MinIO, Great Expectations 1.17.2, Docker Compose, pytest, JSON, HTML, and PNG.

## Global Constraints

- The DAG contains exactly six rubric-stage tasks in this order: `dp1_raw_to_bronze.ingest_raw_to_bronze`, `dp1_raw_to_bronze.validate_bronze`, `dp2_bronze_to_silver_gold.transform_bronze_to_silver_gold`, `dp2_bronze_to_silver_gold.validate_silver_gold`, `dp3_offline_features.compute_offline_features`, and `dp3_offline_features.validate_offline_features`.
- Each task must perform real work or validation and write a stage manifest; no empty wrapper task is allowed.
- Keep `run_hourly_batch_lakehouse(...)` as a backward-compatible composition of the same stage functions.
- Split core Gold and feature-table Spark queries without changing the result of `ordered_gold_queries()` or existing full-pipeline callers.
- Airflow owns endpoint/credential references through seeded Connections and owns run parameters through Variables; DAG code must not duplicate service URLs.
- The DP3 feature contract uses exact `event_timestamp` and `created` columns from Topic 06.
- Do not stage or commit unless explicitly requested. Preserve unrelated changes and prefix commands with `rtk`.

---

## Rubric Coverage

| Row | Requirement | Points | Current status | Effort | Value added |
|---:|---|---:|---|---|---|
| 28 | DP1 raw-to-Bronze ingest stage in Airflow. | 2 | Partial | S | High |
| 29 | DP1 raw-to-Bronze validation stage in Airflow. | 2 | Partial | S | High |
| 30 | DP2 Bronze-to-Silver/Gold transform stage in Airflow. | 2 | Partial | S | High |
| 31 | DP2 Silver/Gold validation stage in Airflow. | 2 | Partial | S | High |
| 32 | DP3 offline feature compute stage in Airflow. | 2 | Partial | S | High |
| 33 | DP3 offline feature validation stage in Airflow. | 2 | Partial | S | High |

## Current Implementation and Evidence

- `hourly_batch_lakehouse.py` currently exposes one `run_hourly_batch_window` task.
- `src/vina_bim_shop/orchestration/hourly_batch.py` already performs Bronze inventory/validation, Spark processing, Gold validation, GX rendering, and manifest writing inside one function.
- `kafka_topic_bootstrap.py` and `scripts/lakehouse/land_bronze_batch.py` cover ingestion pieces but do not present one DP1 stage chain.
- dbt/Spark feature tables and tests exist, but Airflow does not expose DP3 separately.
- `compose/orchestration.airflow.yml` seeds one DataHub REST connection through environment configuration; the other coursework endpoints and Variables are not centralized as rubric evidence.

## Gap, Scope, and Non-Goals

**Gap:** Real work exists, but stage boundaries, order, DP3 visibility, Airflow-owned metadata, and row-specific UI evidence are missing.

**Scope:** Add one six-task DAG, pure stage functions, a compatibility wrapper, idempotent metadata seeding, focused tests, a successful run, stage manifests, GX links, and graph/grid screenshots.

**Non-goals:** Do not move Flink under Airflow, replace existing operational DAGs, add dynamic task mapping, introduce a second scheduler, or change business transformations.

## Dependencies

- Topic 03 must preserve a passing canonical Spark pipeline.
- Topic 06 must finalize the feature `created` contract.
- Ingestion, lakehouse, batch, and orchestration profiles must be healthy for runtime evidence.

## Exact File Map

| Action | Path | Responsibility |
|---|---|---|
| Create | `infra/orchestration/airflow/dags/mini_coursework_pipeline.py` | Three TaskGroups, six exact task IDs, dependencies, and Airflow metadata lookup. |
| Create | `src/vina_bim_shop/orchestration/mini_coursework_pipeline.py` | Pure idempotent stage functions and per-stage manifests. |
| Create | `infra/orchestration/airflow/bootstrap/seed_coursework_metadata.py` | Idempotently create/update required Airflow Connections and Variables. |
| Modify | `compose/orchestration.airflow.yml` | Mount/run the seed script in `airflow-init` and share required non-secret defaults. |
| Modify | `src/vina_bim_shop/orchestration/hourly_batch.py` | Compose the new stages while preserving the existing public wrapper. |
| Modify | `src/vina_bim_shop/lakehouse/spark/sql.py` | Expose ordered core-Gold and feature query groups while preserving combined order. |
| Modify | `src/vina_bim_shop/lakehouse/spark/job.py` | Add explicit core transform and feature compute entry points. |
| Modify | `src/vina_bim_shop/lakehouse/spark/runner.py` | Provide stage-specific runner adapters and retain full-pipeline behavior. |
| Modify | `tests/unit/test_orchestration_dag_adapters.py` | Validate the six-task DAG and retain one-task contracts for legacy DAGs. |
| Modify | `tests/unit/test_orchestration_runtime.py` | Test stage order, idempotency, manifests, gates, and compatibility wrapper. |
| Modify | `tests/unit/test_orchestration_compose_profile.py` | Test metadata seeding and mounts. |
| Create | `tests/unit/test_airflow_coursework_metadata.py` | Test exact Connection/Variable upsert contract without a live scheduler. |
| Modify | `deliverables/08_airflow_gx_orchestration.md` | Add ordered DP1-DP3 row proof and UI/evidence links. |
| Create | `evidence/08_airflow_gx/coursework_pipeline/run_manifest.json` | DAG/run IDs, logical window, six task states, stage manifests, and artifact links. |
| Create | `evidence/08_airflow_gx/coursework_pipeline/dp1_ingest.json` | Raw/Bronze object and record counts. |
| Create | `evidence/08_airflow_gx/coursework_pipeline/dp1_validate.json` | Bronze GX suite and gate result. |
| Create | `evidence/08_airflow_gx/coursework_pipeline/dp2_transform.json` | Spark core table outputs and application ID. |
| Create | `evidence/08_airflow_gx/coursework_pipeline/dp2_validate.json` | Silver/Gold row counts, contract result, and GX link. |
| Create | `evidence/08_airflow_gx/coursework_pipeline/dp3_compute.json` | Three feature tables, rows, columns, and application ID. |
| Create | `evidence/08_airflow_gx/coursework_pipeline/dp3_validate.json` | Feature schema/test/gate results. |
| Create | `evidence/08_airflow_gx/screenshots/mini_coursework_pipeline_graph.png` | Expanded graph showing all TaskGroups and task order. |
| Create | `evidence/08_airflow_gx/screenshots/mini_coursework_pipeline_grid.png` | Successful run with six green task instances. |
| Test | `tests/unit/test_orchestration_dag_adapters.py` | DAG source contract. |
| Test | `tests/unit/test_orchestration_runtime.py` | Pure runtime contract. |
| Test | `tests/unit/test_orchestration_compose_profile.py` | Compose/init contract. |
| Test | `tests/unit/test_airflow_coursework_metadata.py` | Metadata seed contract. |
| Test | `tests/unit/test_spark_batch_runtime.py` | Split-query backward compatibility. |
| Regenerate | `evidence/08_airflow_gx/coursework_pipeline/` | Trigger one fresh successful logical-window run. |
| Regenerate | `evidence/08_airflow_gx/screenshots/mini_coursework_pipeline_graph.png` | Re-capture expanded task graph. |
| Regenerate | `evidence/08_airflow_gx/screenshots/mini_coursework_pipeline_grid.png` | Re-capture successful task states. |

## Interfaces and Runtime Contract

- Stage functions accept `run_id`, `start_ts`, `end_ts`, `settings`, and `run_root`; each returns a JSON-serializable mapping and writes one exact stage artifact.
- `CourseworkPipelineSettings` contains values resolved by the DAG from Connections `vbs_minio`, `vbs_trino`, `vbs_kafka`, and `datahub_rest_default`, plus Variables `vbs_raw_root`, `vbs_bronze_bucket`, `vbs_spark_evidence_root`, and `vbs_feature_tables`.
- Connection seeding uses idempotent upsert and never logs passwords or secret extras.
- `ordered_core_gold_queries() + ordered_feature_queries()` equals the existing `ordered_gold_queries()` exactly.
- DP2 creates all non-feature Gold outputs; DP3 creates only `feat_customer_90d`, `feat_stream_60m`, and `feat_customer_unified`.
- DP3 validation requires all three tables to have positive row counts and exact `event_timestamp` plus `created` columns.

## Ordered Tasks

### Task 1: Lock DAG, metadata, and query-split contracts

**Files:**
- Modify: `tests/unit/test_orchestration_dag_adapters.py`
- Modify: `tests/unit/test_orchestration_runtime.py`
- Modify: `tests/unit/test_orchestration_compose_profile.py`
- Create: `tests/unit/test_airflow_coursework_metadata.py`

- [ ] Assert the new DAG has the exact ID `mini_coursework_pipeline`, three TaskGroups, six task IDs, and the exact linear dependencies from Global Constraints.
- [ ] Assert legacy DAG source checks still expect one task where applicable.
- [ ] Assert the metadata seed upserts the four Connections and four Variables exactly and redacts secrets.
- [ ] Assert split Spark query groups compose to the existing combined order without duplicates.
- [ ] Assert the existing `run_hourly_batch_lakehouse` wrapper invokes all six stage functions in order.
- [ ] Run `rtk uv run pytest tests/unit/test_orchestration_dag_adapters.py tests/unit/test_orchestration_runtime.py tests/unit/test_orchestration_compose_profile.py tests/unit/test_airflow_coursework_metadata.py tests/unit/test_spark_batch_runtime.py -q`.

Expected: new assertions fail before the DAG, seed script, and stage interfaces exist.

### Task 2: Split the pure Spark and orchestration stages

**Files:**
- Create: `src/vina_bim_shop/orchestration/mini_coursework_pipeline.py`
- Modify: `src/vina_bim_shop/orchestration/hourly_batch.py`
- Modify: `src/vina_bim_shop/lakehouse/spark/sql.py`
- Modify: `src/vina_bim_shop/lakehouse/spark/job.py`
- Modify: `src/vina_bim_shop/lakehouse/spark/runner.py`

- [ ] Split ordered Gold queries into core and feature groups with the preserved combined API.
- [ ] Implement six idempotent stage functions and exact per-stage JSON schemas.
- [ ] Reuse existing Bronze/GX/Spark/Trino helpers instead of duplicating transformation logic.
- [ ] Make a rerun for the same `run_id` overwrite only that run's stage artifacts and produce identical business results.
- [ ] Preserve existing gate behavior: Bronze warnings do not block; failed Gold or feature contracts block.
- [ ] Re-run focused runtime and Spark tests.

Expected: pure functions pass without importing Airflow and the legacy wrapper remains compatible.

### Task 3: Add the six-task DAG and Airflow metadata seed

**Files:**
- Create: `infra/orchestration/airflow/dags/mini_coursework_pipeline.py`
- Create: `infra/orchestration/airflow/bootstrap/seed_coursework_metadata.py`
- Modify: `compose/orchestration.airflow.yml`

- [ ] Resolve Connections and Variables in the DAG adapter and pass a settings mapping to pure runtime functions.
- [ ] Add the three exact TaskGroups and six PythonOperators with no extra completion task.
- [ ] Seed metadata after database migration and before webserver/scheduler health dependencies complete.
- [ ] Run `rtk docker compose --profile orchestration config` and verify the init command/mount resolves.
- [ ] Run the focused DAG, compose, and metadata tests.

Expected: Airflow parses the new DAG and metadata seeding is repeatable.

### Task 4: Execute and capture one successful pipeline run

**Files:**
- Regenerate: `evidence/08_airflow_gx/coursework_pipeline/`
- Create: the two Airflow screenshot files listed in the Exact File Map

- [ ] Start `ingestion`, `lakehouse`, `batch`, and `orchestration` profiles with `rtk docker compose --profile ingestion --profile lakehouse --profile batch --profile orchestration up -d --build`.
- [ ] Verify seeded metadata with `rtk docker compose exec -T airflow-webserver airflow connections get vbs_minio` and corresponding checks for all Connections/Variables without printing secret fields.
- [ ] Trigger one logical window with `rtk docker compose exec -T airflow-webserver airflow dags trigger mini_coursework_pipeline --logical-date 2026-06-01T01:00:00+00:00`.
- [ ] Wait until all six task instances are `success`; require each stage artifact and a run manifest that maps every task ID to its artifact.
- [ ] Capture the expanded graph and successful grid from `http://localhost:8080` at the exact screenshot paths.
- [ ] Verify Bronze and Gold GX reports plus DP3 contract results are linked and readable.

Expected: one run proves the exact six-stage order and successful validation gates.

### Task 5: Document rows 28-33 and run regressions

**Files:**
- Modify: `deliverables/08_airflow_gx_orchestration.md`

- [ ] Add six ordered row sections, each naming the exact task ID, stage artifact, and relevant screenshot/GX link.
- [ ] Add a Connection/Variable inventory table with purpose and non-secret fields.
- [ ] State that Flink remains continuously managed outside Airflow.
- [ ] Run `rtk uv run pytest tests/unit/test_orchestration_dag_adapters.py tests/unit/test_orchestration_runtime.py tests/unit/test_orchestration_compose_profile.py tests/unit/test_airflow_coursework_metadata.py tests/unit/test_spark_batch_runtime.py tests/unit/test_deliverables_documentation.py -q`.
- [ ] Run `rtk uv run pytest -q` and inspect `rtk git status --short`.

Expected: all orchestration and full regressions pass and rows 28-33 are directly reviewable.

## Required Evidence

- Six stage JSON artifacts and one run manifest.
- One expanded graph screenshot and one successful grid screenshot.
- Bronze, Gold, and feature validation results with direct links.
- Non-secret Airflow Connection/Variable inventory.

## Definition of Done

- Airflow shows exactly six successful rubric-stage tasks in the required order.
- Each task performs real work/validation and maps to a durable artifact.
- DP3 is independently computed and validated using the final feature schema.
- Connections and Variables are seeded idempotently and consumed by DAG code.
- Legacy DAG/runtime behavior and full tests remain passing.

## Completion Record

The implementing session records date, DAG/run ID, logical window, six task states, Connection/Variable verification, Spark application IDs, GX/feature results, screenshot paths, test outputs, and residual limitations here. Until then, unchecked tasks define the outstanding work.
