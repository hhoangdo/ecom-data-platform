# Flink Baseline and Streaming Proof Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Produce a controlled Flink baseline-versus-optimized comparison and package direct proof for burst, late-arrival, duplicate, and event-time window handling.

**Architecture:** Add an experiment-only profile layer around the existing commerce and operations jobs. Baseline and optimized variants consume the same deterministic replay, write isolated derived topics, and emit comparable metrics without changing the canonical streaming defaults.

**Tech Stack:** Python 3.12, PyFlink 1.19.2, Kafka, MinIO, Docker Compose, pytest, JSON, Markdown, and Flink Web UI.

## Global Constraints

- Preserve `configs/pipelines/flink_streaming.yaml` as the canonical optimized runtime contract.
- Baseline and optimized runs must consume byte-identical input events and use separate consumer groups, topic suffixes, checkpoint prefixes, and job names.
- The baseline disables checkpointing, uses parallelism `1`, sets out-of-orderness and allowed lateness to `0`, and records the resulting limitations rather than presenting it as production-safe.
- The optimized run uses the existing values: out-of-orderness `5` seconds and allowed lateness `300`, `600`, `900`, and `120` seconds for commerce, catalog, fulfillment, and operations topics respectively.
- Output equality applies to on-time windows. Late-event correction output is expected only from the optimized path and must be explained separately.
- Do not stage or commit unless the user explicitly requests it.
- Preserve unrelated changes and use `rtk` for every shell command.

---

## Rubric Coverage

| Row | Requirement | Points | Current status | Effort | Value added |
|---:|---|---:|---|---|---|
| 21 | Flink baseline without optimization and explain optimization steps with UI proof. | 2 | Partial | M | High |
| 22 | Handle streaming burst with explanation and proof. | 2 | Satisfied, proof dispersed | XS | Medium |
| 23 | Handle late arrivals with explanation and proof. | 2 | Satisfied, proof dispersed | XS | Medium |
| 24 | Handle another streaming problem, using duplicates. | 2 | Satisfied, proof dispersed | XS | Medium |
| 25 | Demonstrate Flink window processing. | 2 | Satisfied, proof dispersed | XS | Medium |

## Current Implementation and Evidence

- `configs/pipelines/flink_streaming.yaml` defines topic mappings, one-minute windows, a five-second watermark, per-topic lateness, and checkpoint/curated-output locations.
- `src/vina_bim_shop/flink/runtime.py` owns timestamp assignment and watermark creation.
- `src/vina_bim_shop/flink/commerce_job.py` uses event-time windows and allowed lateness.
- `src/vina_bim_shop/flink/ops_job.py`, `alerts.py`, `metrics.py`, and `corrections.py` implement burst, duplicate, and late-correction outputs.
- `scripts/flink/publish_smoke.py`, `cleanroom_verify.py`, and `capture_evidence.py` already provide deterministic publication, verification, and runtime capture surfaces.
- `evidence/06_flink_streaming/` contains job, checkpoint, output, and UI artifacts, but no explicit baseline comparison.

## Gap, Scope, and Non-Goals

**Gap:** The optimized design is documented, but there is no controlled unoptimized run, no one-to-one comparison, and no single row-21-to-25 evidence narrative.

**Scope:** Add isolated experiment configuration, a comparison runner, focused unit tests, two completed runtime submissions, machine-readable comparison evidence, two UI screenshots, and a consolidated deliverable section.

**Non-goals:** Do not replace the production jobs, change canonical topic names, supervise Flink from Airflow, claim that zero lateness is correct, or benchmark cluster-scale throughput.

## Dependencies

- Topic 02 must have produced the generator scenarios and measured burst/late/duplicate rates.
- Docker profiles `ingestion`, `lakehouse`, and `streaming` must be healthy.
- Existing Flink clean-room verification must pass before experiment work begins.

## Exact File Map

| Action | Path | Responsibility |
|---|---|---|
| Create | `configs/pipelines/flink_baseline_experiment.yaml` | Baseline-only overrides, isolated groups/topics/checkpoints, and explicit disabled optimizations. |
| Create | `src/vina_bim_shop/flink/baseline_experiment.py` | Resolve variants, validate isolation, summarize job metrics, and compare outputs. |
| Create | `scripts/flink/run_baseline_comparison.py` | Submit exactly one baseline or optimized variant and write its manifest. |
| Create | `tests/unit/test_flink_baseline_experiment.py` | Test profile values, isolation, comparison rules, and canonical-config preservation. |
| Modify | `deliverables/06_flink_streaming.md` | Add ordered row-21-to-25 baseline and challenge proof. |
| Modify | `deliverables/11_solving_data_challenges.md` | Link the measured Flink experiment and direct artifacts. |
| Create | `evidence/06_flink_streaming/optimization/baseline_metrics.json` | Baseline job ID, duration, records, checkpoints, watermark/lateness settings, and output counts. |
| Create | `evidence/06_flink_streaming/optimization/optimized_metrics.json` | Optimized job ID and the same comparable fields. |
| Create | `evidence/06_flink_streaming/optimization/comparison.json` | Comparable on-time results, correction delta, and explicit optimization settings. |
| Create | `evidence/06_flink_streaming/optimization/challenge_samples.json` | One input/output proof set for burst, late arrival, duplicate, and window result. |
| Create | `evidence/06_flink_streaming/optimization/report.md` | Method, findings, limitations, and rubric-row links. |
| Create | `evidence/06_flink_streaming/optimization/run_manifest.json` | Artifact inventory and two distinct Flink job IDs. |
| Create | `evidence/06_flink_streaming/screenshots/flink_baseline_job.png` | Completed baseline job overview. |
| Create | `evidence/06_flink_streaming/screenshots/flink_optimized_job.png` | Completed optimized job overview and checkpoint state. |
| Test | `tests/unit/test_flink_baseline_experiment.py` | Experiment contract. |
| Test | `tests/unit/test_flink_config.py` | Canonical configuration regression. |
| Test | `tests/unit/test_flink_challenge_handling.py` | Burst, lateness, duplicate, and window behavior. |
| Test | `tests/unit/test_flink_cleanroom_verification.py` | Output verification regression. |
| Test | `tests/unit/test_flink_runtime_helpers.py` | Runtime helper regression. |
| Regenerate | `evidence/06_flink_streaming/optimization/` | Re-run both variants and rebuild all comparison artifacts. |
| Regenerate | `evidence/06_flink_streaming/screenshots/flink_baseline_job.png` | Re-capture the baseline UI. |
| Regenerate | `evidence/06_flink_streaming/screenshots/flink_optimized_job.png` | Re-capture the optimized UI. |
| Regenerate | `evidence/06_flink_streaming/run_manifest.json` | Include the optimization evidence in the existing inventory. |

## Interfaces and Evidence Contract

- `load_experiment_profile(variant: str) -> FlinkExperimentProfile` accepts only `baseline` or `optimized`.
- `FlinkExperimentProfile` records job name, consumer-group suffix, derived-topic suffix, checkpoint prefix, parallelism, checkpointing flag, watermark seconds, and per-topic allowed-lateness seconds.
- `compare_runs(baseline: dict, optimized: dict) -> dict` requires equal input hashes and equal on-time aggregate keys/counts; it reports correction counts separately.
- Exact job names are `vina-bim-shop-flink-baseline` and `vina-bim-shop-flink-optimized`.
- `challenge_samples.json` contains four named objects in row order: `burst`, `late_arrival`, `duplicate`, and `window_processing`, each with source event IDs and output fields.

## Ordered Tasks

### Task 1: Lock the experiment contract in tests

**Files:**
- Create: `tests/unit/test_flink_baseline_experiment.py`

- [ ] Assert baseline values are parallelism `1`, checkpointing false, watermark `0`, and all allowed-lateness values `0`.
- [ ] Assert optimized values are loaded from `configs/pipelines/flink_streaming.yaml`, not duplicated in Python.
- [ ] Assert variant consumer groups, output topics, checkpoint prefixes, and job names are distinct.
- [ ] Assert comparison rejects different input hashes and mismatched on-time aggregates while allowing optimized-only late corrections.
- [ ] Run `rtk uv run pytest tests/unit/test_flink_baseline_experiment.py -q`.

Expected: tests fail because the experiment module and baseline configuration do not exist.

### Task 2: Implement isolated baseline and optimized variants

**Files:**
- Create: `configs/pipelines/flink_baseline_experiment.yaml`
- Create: `src/vina_bim_shop/flink/baseline_experiment.py`
- Create: `scripts/flink/run_baseline_comparison.py`

- [ ] Implement immutable profile loading and reject unsupported variants or any collision with canonical consumer groups, topics, and checkpoints.
- [ ] Route baseline settings through existing runtime/job builders without changing their default arguments.
- [ ] Make one process submit one named variant and write only that variant's metrics.
- [ ] Record a SHA-256 hash of the replay input manifest in both metric files.
- [ ] Run `rtk uv run pytest tests/unit/test_flink_baseline_experiment.py tests/unit/test_flink_config.py tests/unit/test_flink_runtime_helpers.py -q`.

Expected: all focused tests pass and canonical config tests remain unchanged.

### Task 3: Execute both runs and capture comparison evidence

**Files:**
- Regenerate: `evidence/06_flink_streaming/optimization/`
- Create: `evidence/06_flink_streaming/screenshots/flink_baseline_job.png`
- Create: `evidence/06_flink_streaming/screenshots/flink_optimized_job.png`

- [ ] Start dependencies with `rtk docker compose --profile ingestion --profile lakehouse --profile streaming up -d --build`.
- [ ] Publish one deterministic replay and save its manifest using `rtk uv run python scripts/flink/publish_smoke.py`.
- [ ] Run the baseline submission with `rtk uv run python scripts/flink/run_baseline_comparison.py --variant baseline --input-manifest evidence/06_flink_streaming/flink_smoke_publish_summary.json --evidence-root evidence/06_flink_streaming/optimization`.
- [ ] Run the optimized submission with the same input manifest and `--variant optimized`.
- [ ] Run clean-room verification with `rtk uv run python scripts/flink/cleanroom_verify.py` and then capture APIs with `rtk uv run python scripts/flink/capture_evidence.py`.
- [ ] Verify two distinct job IDs, matching input hashes, equal on-time aggregate outputs, and optimized late-correction records.
- [ ] Save completed job pages from `http://localhost:8082` to the two named screenshot paths.

Expected: `comparison.json` reports successful comparable-output checks, documents baseline losses, and links two real UI captures.

### Task 4: Consolidate row-21-to-25 proof

**Files:**
- Modify: `deliverables/06_flink_streaming.md`
- Modify: `deliverables/11_solving_data_challenges.md`
- Create: `evidence/06_flink_streaming/optimization/report.md`

- [ ] Add five ordered sections labeled Row 21 through Row 25.
- [ ] For Row 21, explain each changed setting and cite both metrics files and UI images.
- [ ] For Rows 22-25, embed the corresponding `challenge_samples.json` object and direct code/evidence paths.
- [ ] State that the optimized path is the canonical runtime and the baseline exists only for comparison.

Expected: a reviewer can move from each rubric row to code, runtime result, and screenshot without inference.

### Task 5: Run focused and regression verification

- [ ] Run `rtk uv run pytest tests/unit/test_flink_baseline_experiment.py tests/unit/test_flink_config.py tests/unit/test_flink_challenge_handling.py tests/unit/test_flink_cleanroom_verification.py tests/unit/test_flink_runtime_helpers.py -q`.
- [ ] Run `rtk uv run pytest -q`.
- [ ] Run `rtk git status --short` and confirm only authorized implementation/evidence/doc files plus pre-existing user changes appear.

Expected: focused and full suites pass with no canonical streaming regression.

## Required Evidence

- Two metrics JSON files with distinct job IDs and identical replay hashes.
- One comparison JSON with explicit setting differences and correctness checks.
- One four-scenario sample JSON in rubric order.
- One report and one run manifest.
- Two readable Flink UI screenshots showing completed jobs and optimized checkpoint behavior.

## Definition of Done

- Rows 21-25 appear in order with direct code, output, and UI proof.
- Baseline and optimized jobs are isolated and reproducible from the same input.
- The comparison distinguishes comparable on-time results from expected late-correction differences.
- Canonical Flink configuration and existing runtime tests remain passing.
- No rubric status is upgraded until all named evidence exists.

## Completion Record

The implementing session records the date, exact commands and exit codes, two job IDs, replay hash, artifact paths, measured comparison, tests, and residual limitations here. Until then, unchecked tasks define the outstanding work.
