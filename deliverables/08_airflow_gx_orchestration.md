# Airflow And Great Expectations Orchestration

## Purpose

Apache Airflow and Great Expectations form the control-plane and quality-evidence layer for the Vina Bim Shop platform.

Airflow coordinates repeatable local workflows such as topic bootstrap, batch lakehouse processing, Pinot bootstrap, reconciliation reporting, DataHub ingestion, and evidence packaging. Great Expectations validates data quality at selected points in the platform and publishes static GX Data Docs for inspection.

Together, they make the project auditable. The goal is not only to run services, but to show what ran, what was checked, which checks block a workflow, and where the resulting evidence is stored.

## Why Airflow And GX Are Needed

A local data platform can quickly become a collection of one-off scripts. Airflow and GX solve that by giving the coursework a clear operational structure:

| Pain point | Airflow/GX response |
| --- | --- |
| Manual commands are hard to repeat in the same order. | Airflow DAGs define the intended control-plane workflows. |
| Evidence runs need logical timestamps. | Hourly demo DAGs support closed logical windows. |
| Quality failures need consistent severity. | GX checks are wrapped in policy logic that decides warning vs blocking behavior. |
| Instructors need inspectable proof. | Manifests, validation JSON, query reports, and GX Data Docs are written to evidence folders; committed screenshots are historical review artifacts. |
| Streaming should remain independent. | Airflow documents the boundary instead of supervising Flink runtime jobs. |

Airflow must not monitor or restart Flink in v1. Flink jobs are long-running streaming runtime processes; Airflow owns batch and control-plane workflow orchestration.

## Implemented DAGs

The required DAG inventory is defined in `src/vina_bim_shop/orchestration/specs.py`.

| DAG | Schedule type | Responsibility |
| --- | --- | --- |
| `kafka_topic_bootstrap` | Manual | Creates or verifies Kafka topics and ingestion prerequisites. |
| `pinot_bootstrap` | Manual | Applies Pinot schemas and realtime table configs. |
| `hourly_batch_lakehouse` | Hourly demo | Runs the staged batch lakehouse flow for a closed logical hour. |
| `mini_coursework_pipeline` | Hourly demo | Exposes the six rubric-facing DP1 Bronze, DP2 core lakehouse, and DP3 offline-feature stages. |
| `reconciliation_report` | Hourly demo | Produces realtime-vs-canonical reconciliation evidence for a logical window. |
| `datahub_ingestion` | Manual | Runs governance metadata ingestion after source assets are available. |
| `local_evidence_build` | Manual | Packages local run evidence into committed evidence structure. |

Manual DAGs stay paused by default and are triggered intentionally. `datahub_ingestion` is the one exception: it is unpaused (`is_paused_upon_creation=False`) so governance ingestion can run on demand. Hourly demo DAGs are also controlled deliberately so evidence can be tied to a known logical date.

DAG files under `infra/orchestration/airflow/dags/` are intentionally thin adapters. Each one defines a single `PythonOperator` that calls a runtime function from the split modules under `src/vina_bim_shop/orchestration/`. No business logic, no `subprocess.run`, no `requests.get`, no file I/O, and no browser/screenshot helpers live in the DAG files.

## DAG Reference

### `kafka_topic_bootstrap` (manual)

| Field | Documentation |
| --- | --- |
| Purpose | Apply the Kafka ingestion contract: topics, Avro/JSON schemas, and the Bronze S3 sink connector. |
| Why it exists | Producers, Flink, and Spark need a stable topic and schema surface before any streaming or batch data is published. |
| Runtime signature | `run_kafka_topic_bootstrap(*, run_id: str) -> dict[str, Any]` |
| Inputs (file) | `infra/kafka/topics.yaml`, `infra/kafka/schemas/`, `infra/kafka/connect/source-events-s3-sink.template.json` |
| Inputs (runtime) | `context["run_id"]` only. |
| Services / endpoints | Kafka broker `kafka:29092`, Schema Registry `http://schema-registry:8081`, Kafka Connect `http://kafka-connect:8083`, MinIO `http://minio:9000`. |
| Outputs | `evidence/08_airflow_gx/runs/kafka_topic_bootstrap/<run_id>/{bootstrap_health.json, connector_response.json, topic_list.txt, run_manifest.json}`. |
| Boundary | Does not generate business data, does not run Spark/Flink, does not run GX. |
| Tests | `tests/unit/test_orchestration_runtime.py::test_required_airflow_dags_are_declared_with_manual_or_demo_schedules`, `tests/unit/test_orchestration_adr_boundaries.py::test_orchestration_evidence_artifacts_exist`. |

### `hourly_batch_lakehouse` (`@hourly`, paused)

| Field | Documentation |
| --- | --- |
| Purpose | Run one logical hourly batch window: Bronze landing inventory, Spark Bronze→Silver→Gold transformation, Trino/Iceberg Gold contract check, GX Data Docs render. |
| Why it exists | Streaming is provisional; Spark batch against MinIO/Iceberg is the canonical truth and the source of Gold tables for Trino. |
| Runtime signature | `run_hourly_batch_lakehouse(*, run_id: str, start_ts: str, end_ts: str) -> dict[str, Any]` |
| Inputs (runtime) | `context["data_interval_start"]` and `context["data_interval_end"]`, wrapped by `BatchWindow.from_args(start_ts, end_ts, mode="hourly")`. |
| Services / endpoints | `spark-master:8080`, `spark-history-server:18080`, MinIO via `mc ls --recursive ALIAS/bronze`, Hive Metastore via Spark, Trino `http://trino:8080` (`show tables from iceberg.gold`, `select count(*) from iceberg.gold.fact_order`), GX docs container `/usr/share/nginx/html`. |
| Outputs | `evidence/08_airflow_gx/runs/hourly_batch_lakehouse/<run_id>/{run_manifest.json, quality/bronze_raw_minio.json, quality/gold_trino_contract.json, spark_batch/{run_batch_summary.json, spark_master_status.json, spark_history_applications.json, dbt_parity_report.{json,md}, pyspark_validation_report.json, spark_job_manifest.json, spark_table_row_counts.json, trino_gold_smoke_results.json, version_matrix.json, gx/validation_results.json}}`. Manifest's `artifacts[]` is explicit: `quality/bronze_raw_minio.json`, `quality/gold_trino_contract.json`, `spark_batch/run_batch_summary.json`. No `screenshots/` paths. |
| Boundary | Does not create Kafka topics, does not apply Pinot assets, does not run DataHub ingestion, does not capture UI screenshots. Raises `RuntimeError("Gold Trino validation failed.")` when `gold_report.blocks_dag and not gold_report.success` (per `gate_outcome_for_layer("gold_trino", success=False)`). |
| Tests | `test_required_airflow_dags_are_declared_with_manual_or_demo_schedules`, `test_required_airflow_dags_preserve_adr06_boundaries`, `test_quality_gate_policy_matches_adr06_failure_contract`, `test_prepare_spark_evidence_root_uses_root_exec_for_shared_workspace`, `test_prepare_gx_docs_root_uses_root_exec_for_static_site_mount`, `test_airflow_webserver_allows_slow_local_plugin_startup`. |

### `pinot_bootstrap` (manual, paused)

| Field | Documentation |
| --- | --- |
| Purpose | Apply Pinot schema/table assets and run sample serving queries against Pinot broker + Trino. |
| Why it exists | Reviewers need to confirm the realtime serving surface and Gold reconciliation queries are reachable before they look at hourly metrics. |
| Runtime signature | `run_pinot_bootstrap(*, run_id: str) -> dict[str, Any]` |
| Inputs (file) | Pinot asset templates under `infra/pinot/` (resolved inside `vina_bim_shop.pinot.bootstrap.apply_assets`). |
| Inputs (runtime) | `context["run_id"]` only. |
| Services / endpoints | Pinot controller `http://pinot-controller:9000`, Pinot broker `http://pinot-broker:8000`, Trino `http://trino:8080` (user `vina_analyst`). |
| Outputs | `evidence/08_airflow_gx/runs/pinot_bootstrap/<run_id>/{run_manifest.json, pinot_bootstrap_manifest.json, query_outputs/*, query_examples_manifest.json}`. |
| Boundary | Does not own Flink job lifecycle. Consumes the realtime topics and Gold tables that Flink/Spark make available. Does not capture UI screenshots. |
| Tests | `test_required_airflow_dags_are_declared_with_manual_or_demo_schedules`, `test_required_airflow_dags_preserve_adr06_boundaries`, `test_pinot_bootstrap_dag_exists_without_flink_control_logic` (forbids `vina_bim_shop.flink`, `scripts/flink/run_`, `restart` inside the DAG file). |

### `reconciliation_report` (`@hourly`, paused)

| Field | Documentation |
| --- | --- |
| Purpose | Compare provisional realtime Pinot metrics against canonical Trino/Iceberg Gold for the same logical hourly window. |
| Why it exists | Late-arriving events and duplicates mean realtime Pinot numbers can shift; the batch truth is what the report exposes. |
| Runtime signature | `run_reconciliation_report(*, run_id: str, start_ts: str, end_ts: str) -> dict[str, Any]` |
| Inputs (runtime) | `context["data_interval_start"]`, `context["data_interval_end"]`. |
| Services / endpoints | Pinot broker `http://pinot-broker:8000`, Trino `http://trino:8080`, MinIO/Iceberg via Trino. |
| Outputs | `evidence/08_airflow_gx/runs/reconciliation_report/<run_id>/{run_manifest.json, quality/pinot_query_contract.json, query_outputs/pinot_dashboard_results.json, query_outputs/reconciliation_report.md, query_examples_manifest.json}`. |
| Boundary | Does not re-run data generation, Kafka bootstrap, or Spark batch. The only Pinot path that escalates to a blocking failure, via `should_fail_reconciliation(pinot_success, reconciliation_success)`. |
| Tests | `test_required_airflow_dags_are_declared_with_manual_or_demo_schedules`, `test_required_airflow_dags_preserve_adr06_boundaries`, `test_reconciliation_is_the_only_pinot_path_that_escalates_to_failure`. |

### `datahub_ingestion` (manual, **unpaused**)

| Field | Documentation |
| --- | --- |
| Purpose | Publish metadata, tags, ownership, lineage, and GX assertions into DataHub GMS. |
| Why it exists | Governance and lineage are an official platform concern, not a UI decoration. |
| Runtime signature | `run_datahub_ingestion(*, run_id: str) -> dict[str, Any]` |
| Inputs (file) | Recipes resolved at `/opt/airflow/recipes/` first, falling back to `infra/governance/recipes/`. The four recipes iterated: `kafka_topics.yml`, `minio_storage.yml`, `trino_tables.yml`, `dbt_legacy.yml`. |
| Inputs (runtime) | `context["run_id"]` only. |
| Services / endpoints | `datahub` CLI on PATH, DataHub GMS `http://datahub-gms:8080`, plus emitters in `src/vina_bim_shop/datahub_lineage/` (`spark_lineage.emit_spark_batch_lineage`, `flink_lineage.emit_flink_streaming_lineage`, `emitter.DataHubLineageEmitter`, `gx_assertions.emit_gx_assertions_to_datahub`). |
| Outputs | `evidence/08_airflow_gx/runs/datahub_ingestion/<run_id>/run_manifest.json` with `status` and `ingestion_results{recipe_name: {status, output|reason}}` plus `custom_lineage.{spark,flink,vocabulary,gx_assertions}`. Tags emitted: `bronze`, `silver`, `gold`, `official`, `provisional`, `pii_safe`, `regression_oracle`, `quality_gate`. Owners emitted: `data_engineer`, `airflow` (both `TECHNICAL_OWNER`). GX docs are merged into `evidence/08_airflow_gx/gx_data_docs/`. |
| Boundary | Does not validate business data quality; it publishes metadata/lineage. Per-layer quality policy: `datahub` is `WARNING` and `blocks_dag=False` unless `critical=True`. |
| Tests | `test_run_datahub_ingestion_includes_all_repo_recipes` (asserts all 4 recipes invoked in order), `test_run_datahub_ingestion_preserves_existing_quality_reports`, `test_quality_gate_policy_matches_adr06_failure_contract`, plus `tests/unit/test_datahub_adr_boundaries.py`. |

### `local_evidence_build` (manual, paused)

| Field | Documentation |
| --- | --- |
| Purpose | Collect official machine evidence: Airflow health, the latest `run_manifest.json` per DAG, and a pointer to the GX Data Docs index. |
| Why it exists | Reviewers need a single runnable DAG that produces a portable, machine-verifiable evidence summary. |
| Runtime signature | `run_local_evidence_build(*, run_id: str) -> dict[str, Any]` |
| Inputs (runtime) | `context["run_id"]`, Airflow webserver `http://airflow-webserver:8080/health`, and `evidence/08_airflow_gx/runs/`. |
| Services / endpoints | Airflow webserver `/health`, `gx-docs` static site (`evidence/08_airflow_gx/gx_data_docs/index.html`). |
| Outputs | `evidence/08_airflow_gx/runs/local_evidence_build/<run_id>/run_manifest.json` (per-run), plus `evidence/08_airflow_gx/airflow_health.json` and `evidence/08_airflow_gx/run_manifest.json` (top-level). Manifest contains `airflow_health`, `docs_index_exists`, `latest_run_manifests[]`, `artifacts=["gx_data_docs/index.html"]`. |
| Boundary | Does not perform screenshot capture. Does not write any `screenshots/` directory. Per `tests/unit/test_script_surface_documentation.py::test_official_machine_evidence_has_no_browser_or_screenshot_helpers`, the official runtime path must not contain `playwright`, `npx`, `ScreenshotCapturer`, `screenshot_capturer`, `screenshots/README.md`, `_PLACEHOLDER_PNG`, or `--*-ui-url` flags. |
| Tests | `test_required_airflow_dags_are_declared_with_manual_or_demo_schedules`, `test_orchestration_evidence_artifacts_exist`, `test_official_machine_evidence_has_no_browser_or_screenshot_helpers`. |

## Quality Policy

Great Expectations checks are interpreted through the project quality policy in `src/vina_bim_shop/quality/policies.py`.

| Layer | Failure policy | Rationale |
| --- | --- | --- |
| Bronze raw | Bronze warns and quarantines | Raw landing should preserve source-fidelity and isolate bad records without hiding ingestion drift. |
| Silver | Silver/Gold failures block the DAG | Typed curated data should not continue if core cleansing rules fail. |
| Gold Trino | Silver/Gold failures block the DAG | Canonical reporting tables must satisfy stronger expectations. |
| Pinot queries | Warning unless reconciliation fails | Pinot is fresh and provisional, so query issues are surfaced but the canonical truth remains Spark Gold through Trino. |
| DataHub | Warning unless marked critical | Governance capture is evidence-supporting; critical metadata failures can still block if configured. |

This policy is intentionally asymmetric. Bronze is allowed to expose source problems; Silver and Gold are expected to protect downstream trust.

## Service Interactions

| Service | Relationship |
| --- | --- |
| Kafka | Airflow can bootstrap Kafka topics before ingestion and streaming runs. |
| MinIO/Iceberg/Hive | Airflow-triggered batch workflows read and write lakehouse data through Spark and Trino-facing tables. |
| Spark | Batch DAGs invoke Spark processing for Bronze-to-Silver-to-Gold transformation evidence. |
| Trino | GX and reconciliation tasks query canonical Gold tables through Trino. |
| Pinot | Airflow can apply Pinot configs and run Pinot query checks, but Pinot remains a provisional realtime surface. |
| Flink | Flink is intentionally outside Airflow supervision. Airflow does not restart, monitor, or own Flink stream jobs. |
| DataHub | Airflow can trigger metadata ingestion recipes after platform assets have been created. |
| GX Data Docs | Validation output is rendered into static documentation served locally for audit. |

## Runtime Profile

Start orchestration after its dependency profiles are available:

```powershell
docker compose --profile ingestion up -d
docker compose --profile lakehouse up -d
docker compose --profile batch up -d
docker compose --profile ingestion --profile lakehouse --profile streaming --profile serving up -d
docker compose --profile orchestration up -d
```

The `orchestration` profile starts:

| Service | Responsibility |
| --- | --- |
| `airflow-init` | Initializes Airflow metadata and local admin configuration. |
| `airflow-webserver` | Hosts the Airflow UI. |
| `airflow-scheduler` | Schedules and runs DAG tasks. |
| `gx-docs` | Serves static GX Data Docs from committed evidence artifacts. |

Airflow reuses the shared lakehouse Postgres service and the pre-created `airflow` database.

Local URLs:

| Service | URL |
| --- | --- |
| Airflow UI | `http://localhost:8082` |
| GX Data Docs | `http://localhost:8088` |
| Trino UI | `http://localhost:8080` |
| Pinot controller UI | `http://localhost:9003` |

## Typical Trigger Flow

List installed DAGs:

```powershell
docker compose exec airflow-webserver airflow dags list
```

Trigger Kafka bootstrap:

```powershell
docker compose exec airflow-webserver airflow dags trigger kafka_topic_bootstrap
```

Trigger an hourly batch run for one closed UTC hour:

```powershell
docker compose exec airflow-webserver airflow dags trigger hourly_batch_lakehouse --logical-date 2026-06-01T01:00:00+00:00
```

Trigger reconciliation for the same logical hour:

```powershell
docker compose exec airflow-webserver airflow dags trigger reconciliation_report --logical-date 2026-06-01T01:00:00+00:00
```

Trigger local evidence packaging:

```powershell
docker compose exec airflow-webserver airflow dags trigger local_evidence_build
```

## GX Data Docs

GX Data Docs are generated under `evidence/08_airflow_gx/gx_data_docs/` and served by the `gx-docs` container. They provide a static local audit view of validation runs and are useful when the runtime UI is not available.

The docs are supported by:

| Artifact | Role |
| --- | --- |
| Validation JSON | Machine-readable result for each check. |
| GX static HTML | Human-readable local inspection page. |
| Run manifests | Links each validation batch to DAG and logical-window context. |

UI screenshots are manual reviewer evidence, not orchestration-runtime output and not asserted by `local_evidence_build`. The Topic 07 captures under `evidence/08_airflow_gx/screenshots/` supplement the machine-readable manifests and GX reports without adding browser code to the runtime path.

## Evidence

Committed evidence is stored under `evidence/08_airflow_gx/`.

Important artifacts include:

| Artifact | Purpose |
| --- | --- |
| `airflow_health.json` | Shows local Airflow availability during capture. |
| `run_manifest.json` | Records the evidence capture context. |
| `gx_data_docs/index.html` | Static validation documentation entrypoint. |
| `runs/<dag_id>/<run_id>/run_manifest.json` | Per-run manifest for each captured DAG run. |
| `runs/hourly_batch_lakehouse/<run_id>/quality/*.json` | Quality evidence for the batch lakehouse path. |
| `runs/reconciliation_report/<run_id>/query_outputs/reconciliation_report.md` | Realtime-vs-canonical reconciliation summary. |

### Historical Review Artifacts

A small set of screenshot/UI-capture placeholders previously lived under `evidence/08_airflow_gx/screenshots/` and `evidence/08_airflow_gx/runs/hourly_batch_lakehouse/<run_id>/spark_batch/screenshots/`. They were static capture notes, not generated by the orchestration runtime, and were relocated to the `develop` branch under `develop/evidence/08_airflow_gx/...`.

Official machine evidence is JSON + GX Data Docs. The current Topic 07 graph and grid PNGs are manually captured reviewer evidence after a successful run; they are deliberately outside the orchestration runtime and `local_evidence_build` artifact contract. The no-browser / no-screenshot-helpers contract in `src/vina_bim_shop/orchestration/` and `src/vina_bim_shop/lakehouse/spark/evidence.py` remains enforced by `tests/unit/test_script_surface_documentation.py::test_official_machine_evidence_has_no_browser_or_screenshot_helpers`.

## Runtime Boundary Follow-ups

`src/vina_bim_shop/orchestration/runtime.py` has been split into one module per DAG plus shared helpers. The split is complete; this section now describes the resulting layout.

### Resulting Module Layout

| Module | Responsibility |
| --- | --- |
| `specs.py` | Required-DAG inventory and ADR06 schedule/window boundaries (`supports_hourly_logical_window`, `monitors_flink`). Unchanged. |
| `paths.py` | `REPO_ROOT`, `ADR06_EVIDENCE_ROOT`, `RUNS_ROOT`, `DOCS_ROOT`, `build_run_root`, `_utc_now`, `_write_json`. |
| `subprocess_helpers.py` | `_run_command`, `_working_directory`, `_get_json`. |
| `quality_helpers.py` | `_validate_pandas_dataframe`, `_render_docs`, `_window_payload` — shared by `hourly_batch`, `reconciliation`, and `datahub_ingestion`. |
| `kafka_bootstrap.py` | `run_kafka_topic_bootstrap`. |
| `hourly_batch.py` | `run_hourly_batch_lakehouse` plus lakehouse-specific helpers (`_list_bronze_objects`, `_count_quarantine_records`, `_prepare_spark_evidence_root`, `_prepare_gx_docs_root`, `_capture_airflow_batch_evidence`). |
| `pinot_bootstrap.py` | `run_pinot_bootstrap`. |
| `reconciliation.py` | `run_reconciliation_report`. |
| `datahub_ingestion.py` | `run_datahub_ingestion` plus lineage helpers (`_run_custom_lineage_emission`, `_bootstrap_governance_vocabulary`, `_latest_quality_reports`, `_docs_reports_with`). |
| `local_evidence.py` | `run_local_evidence_build`. |

`runtime.py` is **deleted**; the six DAG adapters import directly from the per-DAG modules above. No shim remains.

### Boundary Rules Preserved By The Split

- `dag_specs_by_id()` in `specs.py` is the single source of truth for the required DAG inventory and ADR06 schedule/window boundaries. None of the new modules redeclare this.
- The two hourly DAGs (`hourly_batch_lakehouse`, `reconciliation_report`) both call `BatchWindow.from_args(start_ts, end_ts, mode="hourly")` and serialize via the shared `_window_payload` helper. Their DAG adapters forward `data_interval_start` and `data_interval_end` into `start_ts` / `end_ts` as ISO-8601 UTC strings; the new `test_orchestration_dag_adapters.py` asserts that contract for both adapters.
- `pinot_bootstrap` and `pinot_bootstrap`'s DAG file do not import `vina_bim_shop.flink` or `scripts/flink/run_*`; this is asserted by `test_pinot_bootstrap_dag_exists_without_flink_control_logic` and the new adapter test.
- `datahub_ingestion` does not validate business data quality; it publishes metadata and lineage. The medallion tag vocabulary is asserted against `datahub_ingestion.py` (not a deleted `runtime.py`) by `test_governance_vocabulary_covers_medallion_layers`.
- The no-browser / no-screenshot-helpers contract applies to the new module set: `tests/unit/test_script_surface_documentation.py::OFFICIAL_MACHINE_EVIDENCE_FILES` lists all eight new orchestration source files.

### Behavior

The split is byte-equivalent in behavior. No algorithm was changed; only file boundaries and import lines moved. `dag_id`, `description`, `start_date`, `schedule`, `catchup`, `is_paused_upon_creation`, `tags`, and the `_run(**context)` callable bodies are unchanged.

## Tests

The DAG, runtime, and governance contracts are protected by these unit tests:

| Test file | What it protects |
| --- | --- |
| `tests/unit/test_orchestration_runtime.py` | Required DAG inventory, ADR06 failure policies, Spark/GX evidence-root exec calls, DataHub recipe order, DataHub docs preservation. After the per-DAG split, the monkeypatches resolve against the per-DAG module names (e.g. `hourly_batch._run_command`, `datahub_ingestion._render_docs`). |
| `tests/unit/test_orchestration_dag_adapters.py` | Adapter-level contract: `dag_id`, `schedule`, `tags`, single `PythonOperator` with `python_callable=_run`, `catchup=False`, `is_paused_upon_creation=...`, no import of the deleted `runtime` module, and (for hourly DAGs) `start_ts=context["data_interval_start"].isoformat()` / `end_ts=context["data_interval_end"].isoformat()` plumbing. |
| `tests/unit/test_orchestration_adr_boundaries.py` | This deliverable's policy phrases, `gx_data_docs/index.html` exists. |
| `tests/unit/test_datahub_adr_boundaries.py` | Governance truth-policy references and recipe coverage. |
| `tests/unit/test_script_surface_documentation.py` | No browser/screenshot helpers in the official machine-evidence files; `develop-only` candidate scripts are not referenced from `README.md`, `deliverables/`, `tests/`, `infra/`, or `src/`. |

Run them with:

```powershell
uv run pytest tests/unit/test_orchestration_runtime.py
uv run pytest tests/unit/test_orchestration_adr_boundaries.py
uv run pytest tests/unit/test_datahub_adr_boundaries.py
uv run pytest tests/unit/test_script_surface_documentation.py
uv run pytest tests/unit -k airflow
```

## Limitations

- Airflow is not a streaming process supervisor. Flink runtime health is verified separately through Flink evidence and clean-room checks.
- The orchestration profile is designed for staged local startup, not a single high-resource full-stack boot.
- GX validation proves selected quality policies and evidence boundaries; it is not a complete production observability system.
- `local_evidence_build` does not include screenshot capture in its `artifacts[]`; Topic 07's manually captured UI proof is stored separately at the documented screenshot paths. Historical placeholders remain on the `develop` branch.

## Convenience Make Targets

The root [Makefile](../Makefile) wraps the most common commands for this deliverable under a `make + verb` convention. It delegates to the official scripts in [scripts/](../scripts/) and to `scripts/ctl.py` for Docker Compose lifecycle; compose targets expand documented profile bundles and prebuild shared images where needed.

| Target | What it does |
| --- | --- |
| `make up-orchestration` | Start the orchestration profile (Airflow webserver, scheduler, init, GX Data Docs). |
| `make down-orchestration` | Stop the orchestration profile and remove its volumes. |
| `make test` | Run `pytest` to verify the orchestration runtime contracts. |

## Mini Coursework Pipeline (DP1–DP3)

`mini_coursework_pipeline` is the rubric-facing hourly DAG. It contains exactly three expanded TaskGroups and six real tasks in a linear dependency chain:

| Rubric stage | Airflow task ID | Output artifact | Validation/evidence |
| --- | --- | --- | --- |
| DP1 ingest | `dp1_raw_to_bronze.ingest_raw_to_bronze` | `evidence/08_airflow_gx/coursework_pipeline/<run-id>/dp1_ingest.json` | Raw snapshot and event files copied into the deterministic Bronze prefixes. |
| DP1 validate | `dp1_raw_to_bronze.validate_bronze` | `evidence/08_airflow_gx/coursework_pipeline/<run-id>/dp1_validate.json` | Bronze GX contract; Bronze warnings preserve quarantine evidence without blocking. |
| DP2 transform | `dp2_bronze_to_silver_gold.transform_bronze_to_silver_gold` | `evidence/08_airflow_gx/coursework_pipeline/<run-id>/dp2_transform.json` | Silver plus every non-feature Gold table and the Spark application ID. |
| DP2 validate | `dp2_bronze_to_silver_gold.validate_silver_gold` | `evidence/08_airflow_gx/coursework_pipeline/<run-id>/dp2_validate.json` | Trino inventory/count contract and GX result; a failed Gold contract blocks. |
| DP3 compute | `dp3_offline_features.compute_offline_features` | `evidence/08_airflow_gx/coursework_pipeline/<run-id>/dp3_compute.json` | `feat_customer_90d`, `feat_stream_60m`, and `feat_customer_unified` plus the Spark application ID. |
| DP3 validate | `dp3_offline_features.validate_offline_features` | `evidence/08_airflow_gx/coursework_pipeline/<run-id>/dp3_validate.json` | Each feature table has rows and exposes `event_timestamp` and `created`, not `created_ts`. |

The per-run `run_manifest.json` maps all six task IDs to their stage artifacts. The reviewer proof is captured manually after a successful run at `evidence/08_airflow_gx/screenshots/mini_coursework_pipeline_graph.png` (expanded graph) and `evidence/08_airflow_gx/screenshots/mini_coursework_pipeline_grid.png` (six green task instances). The Airflow UI is `http://localhost:8082`.

Airflow metadata is seeded idempotently by `airflow-init`: Connections `vbs_minio`, `vbs_trino`, `vbs_kafka`, and `datahub_rest_default`; Variables `vbs_raw_root`, `vbs_bronze_bucket`, `vbs_spark_evidence_root`, and `vbs_feature_tables`. The seed logs only identifiers and non-secret endpoints. Flink remains continuously managed outside Airflow.

### Captured Topic 07 Runtime Proof

The successful manual run was `topic07_20260711T124521Z` for the closed UTC window `2026-04-26T03:00:00Z` to `2026-04-26T04:00:00Z`. It completed all six tasks successfully in the stated DP1 → DP2 → DP3 order. The core Spark application was `app-20260711124824-0000`; the feature Spark application was `app-20260711125042-0001`.

The stage artifacts and run manifest are under `evidence/08_airflow_gx/coursework_pipeline/topic07_20260711T124521Z/`. Bronze GX passed 2/2 expectations, core Gold GX passed 2/2, and feature GX passed 4/4. The DP3 compute artifact records `event_timestamp` and `created` for all three feature tables, with no `created_ts` column. The manually captured reviewer proof is `evidence/08_airflow_gx/screenshots/mini_coursework_pipeline_graph.png` and `evidence/08_airflow_gx/screenshots/mini_coursework_pipeline_grid.png`.
