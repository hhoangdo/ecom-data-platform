# Script Inventory

This directory contains command-line entry points for the local coursework platform.
Official commands stay on `main` because README, deliverables, tests, Airflow/runtime
launchers, or reproducible evidence flows still reference them.

Do not move or delete a script from `main` unless its README, deliverable, test, DAG,
and runtime references have been removed or replaced in the same change.

## Convenience Make Targets

The repository also ships a root [Makefile](../Makefile) that provides a `make + verb` shortcut for the most common commands. It delegates to the official scripts below and to `scripts/ctl.py` for the Docker Compose lifecycle. The compose wrapper expands documented profile bundles and prebuilds shared images where needed, while the classification tables in this README remain the source of truth for which scripts are official.

| Make target | Underlying script or command |
| --- | --- |
| `make install` | `uv sync` |
| `make generate` | `scripts/generate/run_generator.py` (with `SCALE`, `MODE`, `SEED` variables) |
| `make build-dbt` | `uv run dbt build --project-dir infra/analytics/dbt --profiles-dir infra/analytics/dbt` |
| `make test` | `uv run pytest` |
| `make finalize` | `scripts/qa/finalize_sections_01_02.py` |
| `make reset` | `scripts/qa/reset_all.py` (forwards `RESET_FLAGS=...`) |
| `make up-<profile>` / `make down-<profile>` | `scripts/ctl.py compose up <profile>` / `scripts/ctl.py compose down <profile>` for `ingestion`, `lakehouse`, `batch`, `streaming`, `serving`, `orchestration`, `governance`, and `all`; dependent profiles expand to the documented staged bundles. |

Run `make help` at any time to print the full catalog.

## Classification Policy

| Class | Meaning |
| --- | --- |
| `official-main` | Supported command or runtime entry point required by official docs, tests, DAG/runtime launchers, or evidence generation. |
| `develop-only candidate` | Debug, manual, duplicate, screenshot, or destructive helper with no current official references; queued for a later `develop` move. |
| `needs-review` | Documented or tested today, but risky/destructive enough that a human should decide before it moves or becomes fully official. |

## Official Main Scripts

| Script | Why it stays on `main` |
| --- | --- |
| `scripts/analytics/benchmark_duckdb_index.py` | Runs the isolated DuckDB ART-index benchmark without mutating the canonical dbt database. |
| `scripts/datahub/capture_evidence.py` | Referenced by the DataHub governance deliverable for official governance evidence. |
| `scripts/datahub/restore_search_indices.py` | Restores DataHub search indices and verifies representative datasets are indexed after recovery. |
| `scripts/datahub/restore_search_indices.py` | Replays persisted DataHub metadata through GMS to restore indexed-search evidence after a governed runtime recovery. |
| `scripts/flink/capture_evidence.py` | Referenced by the Flink deliverable for streaming evidence capture. |
| `scripts/flink/publish_smoke.py` | Referenced by the Flink deliverable for deterministic streaming smoke events. |
| `scripts/flink/run_baseline_comparison.py` | Runs the isolated baseline or optimized Flink comparison variant and captures its evidence contract. |
| `scripts/flink/run_commerce_metrics_job.py` | Flink job entry point referenced by deliverables, tests, and `infra/flink/bin/submit-jobs.sh`. |
| `scripts/flink/run_ops_alerts_job.py` | Flink job entry point referenced by deliverables, tests, and `infra/flink/bin/submit-jobs.sh`. |
| `scripts/generate/run_generator.py` | Official Section 01 generator entry point referenced by README, deliverables, and integration tests. |
| `scripts/kafka/bootstrap_topics.py` | Kafka topic bootstrap command referenced by the Kafka ingestion deliverable. |
| `scripts/kafka/capture_connect_image_optimization.py` | Captures the Kafka Connect image-size optimization evidence package. |
| `scripts/kafka/capture_evidence.py` | Kafka evidence command referenced by the Kafka ingestion deliverable. |
| `scripts/kafka/consumer_smoke.py` | Kafka smoke consumer referenced by the Kafka ingestion deliverable. |
| `scripts/kafka/producer_smoke.py` | Kafka smoke producer referenced by the Kafka ingestion deliverable. |
| `scripts/kafka/register_bronze_sink.py` | Bronze Kafka Connect sink command referenced by deliverables and unit tests. |
| `scripts/kafka/register_schemas.py` | Schema Registry command referenced by the Kafka ingestion deliverable. |
| `scripts/lakehouse/capture_bronze_evidence.py` | Bronze landing evidence command referenced by deliverables and unit tests. |
| `scripts/lakehouse/capture_evidence.py` | Lakehouse evidence command referenced by the lakehouse deliverable. |
| `scripts/lakehouse/land_bronze_batch.py` | Bronze batch landing command referenced by deliverables and unit tests. |
| `scripts/lakehouse/optimize_iceberg.py` | Captures approved Iceberg compaction and Trino benchmark evidence. |
| `scripts/lakehouse/smoke_sql.py` | Lakehouse SQL smoke command referenced by the lakehouse deliverable. |
| `scripts/pinot/bootstrap.py` | Pinot serving bootstrap command referenced by deliverables and unit tests. |
| `scripts/pinot/query_examples.py` | Pinot query example command referenced by the Pinot serving deliverable. |
| `scripts/pinot/refresh_evidence.py` | Official Pinot evidence refresh command referenced by README-adjacent deliverables, tests, and Flink verification notes. |
| `scripts/qa/finalize_sections_01_02.py` | Final Section 01/02 package command referenced by README and tests. |
| `scripts/qa/generate_section02_evidence.py` | Section 02 evidence command referenced by the schema deliverable and tests. |
| `scripts/qa/capture_novel_ideas.py` | Validates the two ordered novel-idea gates and writes their machine-readable evidence package. |
| `scripts/qa/audit_public_documentation.py` | AST-audits the declared deployable public API and writes hash-bound coverage evidence. |
| `scripts/qa/build_mini_coursework_rubric_manifest.py` | Builds and verifies the fail-closed, row-ordered Mini-Coursework rubric manifest. |
| `scripts/qa/summarize_runlog.py` | Produces compact markdown summaries from long platform run logs for post-run review. |
| `scripts/spark/export_executive_mart.py` | Executive mart export command referenced by README, deliverables, and tests. |
| `scripts/spark/job.py` | Spark submit compatibility entry point referenced by `src/vina_bim_shop/lakehouse/spark/runner.py` and tests. |
| `scripts/spark/run_batch.py` | Official Spark batch command referenced by deliverables and tests. |
| `scripts/spark/run_optimization_experiments.py` | Runs one controlled Spark skew or high-cardinality experiment variant for the Section 05 evidence package without invoking the canonical batch path. |

## Develop-Only Candidates

These files are not currently referenced by official docs, tests, DAGs, or runtime launchers.
Keep them in this branch for now, but treat them as queued for a later `develop` move.

| Script | Why it is a `develop-only candidate` |
| --- | --- |
| `scripts/kafka/cleanup_kafka.py` | Destructive Kafka reset helper; superseded by the documented project-level reset flow for official use. |
| `scripts/lakehouse/cleanup_lakehouse.py` | Destructive lakehouse service/evidence cleanup helper with no official references. |
| `scripts/pinot/capture_evidence.py` | Duplicate/manual Pinot evidence wrapper; official Pinot evidence refresh uses `scripts/pinot/refresh_evidence.py`. |
| `scripts/qa/upload_bronze.sh` | Manual shell helper for copying local raw files into MinIO; official landing uses `scripts/lakehouse/land_bronze_batch.py`. |
| `scripts/spark/capture_evidence.py` | Duplicate/manual Spark evidence wrapper; official batch evidence is produced through `scripts/spark/run_batch.py`. |

## Needs Review

These scripts remain in place because they are referenced, but they are not ordinary
official run commands.

| Script | Risk to resolve before moving or normalizing |
| --- | --- |
| `scripts/flink/cleanroom_verify.py` | Referenced by Flink and Pinot deliverables and tests, but performs clean-room reset, Docker service removal, volume removal, and image/build-cache pruning. |
| `scripts/qa/reset_all.py` | Referenced by README as the local reset command, but intentionally stops services, removes Docker volumes, and can clean gitignored local data. |

## Follow-Up

Official evidence helpers in `src/vina_bim_shop/*/evidence.py` and orchestration
runtime helpers now generate machine-verifiable artifacts only. Committed
`evidence/**/screenshots/` files remain historical review artifacts, but UI
screenshot capture/manual browser helpers should live on `develop` rather than in
the official `main` runtime path.
