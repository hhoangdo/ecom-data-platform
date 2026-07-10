# Kafka Connect Docker Image Optimization Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Produce a measured, working multi-stage Kafka Connect image that reduces the current image size and supplies rubric-ready evidence for rows 3 and 4.

**Architecture:** Capture the current single-stage image before changing it. Build the S3 connector in a builder stage, copy only the installed plugin into a fresh pinned Kafka Connect runtime stage, then run the existing Bronze connector registration and plugin discovery smoke checks against the optimized image. The image experiment is isolated from application behavior: connector class, topic list, credentials, and image tag remain unchanged.

**Tech Stack:** Docker BuildKit, Docker Compose, Confluent Platform 7.8.3, Confluent Hub S3 connector 10.6.4, Python, pytest.

## Global Constraints

- This is an implementation plan only; do not treat its proposed code, commands, or evidence as already executed.
- Preserve user work. Do not modify, delete, revert, stage, or commit unrelated files.
- Do not stage or commit automatically. Staging and commits require an explicit user request.
- Use ASCII text and `apply_patch` for every repository write.
- Keep `vina-bim-shop/kafka-connect:7.8.3-s3`, `confluentinc/cp-kafka-connect:7.8.3`, and `confluentinc/kafka-connect-s3:10.6.4` unchanged.
- Success requires a positive measured image-size reduction and a working connector. No minimum percentage reduction is asserted.

## Rubric Coverage

| Row | Requirement | Points | Current status | Effort | Value added |
|---:|---|---:|---|---|---|
| 3 | Use Docker and Docker Compose; document image reduction from X to Y and the optimization method. | 1 | Partial: Compose and image build exist, but there is no before/after size evidence. | S | High |
| 4 | Optimize a Dockerfile, for example with a multistage build. | 1 | Missing: `infra/kafka/connect/Dockerfile` is single-stage. | M | High |

## Current Implementation and Evidence

- Existing `infra/kafka/connect/Dockerfile` starts from `confluentinc/cp-kafka-connect:7.8.3`, installs CA certificates, and installs the pinned S3 connector in the runtime image.
- Existing `compose/*.yml` exposes the deterministic `vina-bim-shop/kafka-connect:7.8.3-s3` image through the `kafka-connect` service.
- Existing `tests/unit/test_kafka_bronze_sink_runtime.py` confirms that the custom image build and pinned connector reference remain present, and it validates the Bronze sink registration payload.
- Existing `scripts/kafka/register_bronze_sink.py` and `src/vina_bim_shop/kafka/bronze_sink.py` provide the connector registration smoke surface.

## Gap, Scope, Non-Goals, and Dependencies

**Gap:** The repository cannot show the old and new image sizes, layer histories, percentage reduction, or that the smaller image still discovers and runs the S3 sink connector.

**Scope:** Optimize only Kafka Connect, add a narrowly scoped capture script and test assertions, create measured evidence under `evidence/00_engineering_fundamentals/`, and document the result in the engineering-fundamentals report.

**Non-goals:** Do not change connector JSON, topic names, credentials, compose topology, Spark/Flink images, or Kafka Connect runtime semantics. Do not claim a reduction before the two images are measured on the same Docker engine.

**Dependencies:** Docker Engine with BuildKit, Docker Compose, network access to retrieve the pinned base image and plugin, and the ingestion profile for the runtime smoke test.

## Exact File Map

| Action | Path | Responsibility |
|---|---|---|
| Modify | `infra/kafka/connect/Dockerfile` | Replace the single runtime-stage plugin install with a builder stage and a lean runtime stage. |
| Modify | `tests/unit/test_kafka_bronze_sink_runtime.py` | Assert the multi-stage boundary and retain the pinned plugin and custom-image contracts. |
| Create | `scripts/kafka/capture_connect_image_optimization.py` | Inspect image bytes, format MiB, collect `docker history`, calculate reduction, and write comparison artifacts. |
| Create | `tests/unit/test_kafka_connect_image_optimization.py` | Unit-test size conversion, output schema, and mandatory positive-reduction enforcement in the capture script. |
| Create | `evidence/00_engineering_fundamentals/kafka_connect_image_baseline.json` | Baseline image bytes and MiB, image ID, tag, and collection timestamp. |
| Create | `evidence/00_engineering_fundamentals/kafka_connect_image_optimized.json` | Optimized image bytes and MiB, image ID, tag, and collection timestamp. |
| Create | `evidence/00_engineering_fundamentals/kafka_connect_image_history_baseline.txt` | Baseline `docker history --no-trunc` output. |
| Create | `evidence/00_engineering_fundamentals/kafka_connect_image_history_optimized.txt` | Optimized `docker history --no-trunc` output. |
| Create | `evidence/00_engineering_fundamentals/kafka_connect_image_comparison.json` | Machine-readable byte, MiB, and percentage reduction fields. |
| Create | `evidence/00_engineering_fundamentals/kafka_connect_image_comparison.md` | Method, X-to-Y size table, byte delta, MiB delta, positive percentage reduction, and smoke result links. |
| Create | `evidence/00_engineering_fundamentals/kafka_connect_plugin_smoke.json` | Connector plugin discovery and Bronze sink registration response. |
| Create | `evidence/00_engineering_fundamentals/kafka_connect_bronze_sink_response.json` | Existing registration CLI output produced against the optimized image. |
| Create | `evidence/00_engineering_fundamentals/run_manifest.json` | Inventory of generated engineering-fundamentals evidence. |
| Test | `tests/unit/test_kafka_connect_image_optimization.py` | Focused capture-script behavior and schema checks. |
| Test | `tests/unit/test_kafka_bronze_sink_runtime.py` | Dockerfile, image, and connector registration contracts. |
| Test | `tests/unit/test_kafka_compose_profile.py` | Ingestion profile regression coverage. |
| Test | `tests/unit/test_kafka_smoke_scripts.py` | Kafka smoke-command regression coverage. |
| Regenerate | `evidence/00_engineering_fundamentals/kafka_connect_image_baseline.json` | Re-capture baseline inspection metadata on the comparison Docker engine. |
| Regenerate | `evidence/00_engineering_fundamentals/kafka_connect_image_optimized.json` | Re-capture optimized inspection metadata on the same Docker engine. |
| Regenerate | `evidence/00_engineering_fundamentals/kafka_connect_image_history_baseline.txt` | Re-capture baseline layer history. |
| Regenerate | `evidence/00_engineering_fundamentals/kafka_connect_image_history_optimized.txt` | Re-capture optimized layer history. |
| Regenerate | `evidence/00_engineering_fundamentals/kafka_connect_image_comparison.json` | Recalculate the machine-readable comparison. |
| Regenerate | `evidence/00_engineering_fundamentals/kafka_connect_image_comparison.md` | Recalculate the measured comparison. |
| Regenerate | `evidence/00_engineering_fundamentals/kafka_connect_plugin_smoke.json` | Re-capture plugin discovery and connector status. |
| Regenerate | `evidence/00_engineering_fundamentals/kafka_connect_bronze_sink_response.json` | Re-capture connector registration output. |
| Regenerate | `evidence/00_engineering_fundamentals/run_manifest.json` | Rebuild the complete artifact inventory. |

## Interfaces and Outputs

- `scripts/kafka/capture_connect_image_optimization.py` accepts `--baseline-image`, `--optimized-image`, and `--evidence-root`; it exits nonzero unless `optimized_size_bytes < baseline_size_bytes`.
- `evidence/00_engineering_fundamentals/kafka_connect_image_comparison.json` contains exactly `baseline_size_bytes`, `optimized_size_bytes`, `reduction_bytes`, `reduction_mib`, and `reduction_percent`; MiB uses `bytes / 1048576` rounded to two decimals.
- Each image JSON has exactly `captured_at`, `image_id`, `image_tag`, `size_bytes`, and `size_mib`. The capture-script unit test asserts these keys and the five comparison fields rather than accepting undeclared alternatives.
- The optimized Dockerfile has exactly two named stages: `plugin-builder` and the final unnamed runtime stage. The builder installs the S3 plugin under `/opt/connect-plugins`; the runtime copies that directory to `/usr/share/confluent-hub-components`.
- Plugin smoke output records the `io.confluent.connect.s3.S3SinkConnector` class from `GET /connector-plugins` and the registered `bronze-events-s3-sink` connector status from Kafka Connect.

## Ordered Tasks

### Task 1: Capture the unoptimized baseline

**Files:**
- Create: `evidence/00_engineering_fundamentals/kafka_connect_image_baseline.json`
- Create: `evidence/00_engineering_fundamentals/kafka_connect_image_history_baseline.txt`

- [ ] Build the current Dockerfile before editing it: `rtk docker build --no-cache --tag vina-bim-shop/kafka-connect:7.8.3-s3-baseline -f infra/kafka/connect/Dockerfile infra/kafka/connect`.
- [ ] Inspect `vina-bim-shop/kafka-connect:7.8.3-s3-baseline` with `rtk docker image inspect --format '{{.Id}} {{.Size}}'` and store its byte count and MiB value in the baseline JSON.
- [ ] Run `rtk docker history --no-trunc vina-bim-shop/kafka-connect:7.8.3-s3-baseline` and store the unmodified output in the baseline history artifact.
- [ ] Verify the baseline JSON has a positive integer `size_bytes` and a `size_mib` value equal to `round(size_bytes / 1048576, 2)`.

Expected result: the baseline tag, byte count, MiB value, and history are persisted before the Dockerfile changes.

### Task 2: Add the multi-stage Dockerfile and unit contract

**Files:**
- Modify: `infra/kafka/connect/Dockerfile`
- Modify: `tests/unit/test_kafka_bronze_sink_runtime.py`

- [ ] Add a failing test that reads the Dockerfile and asserts `AS plugin-builder`, `COPY --from=plugin-builder /opt/connect-plugins /usr/share/confluent-hub-components`, and the existing pinned S3 connector expression.
- [ ] Run `rtk uv run pytest tests/unit/test_kafka_bronze_sink_runtime.py -q`.

Expected result: FAIL because the current Dockerfile has no builder stage.

- [ ] Change the Dockerfile to install CA certificates and `confluentinc/kafka-connect-s3:10.6.4` only in `plugin-builder`, using `confluent-hub install --no-prompt --component-dir /opt/connect-plugins`.
- [ ] Start a fresh final stage from `confluentinc/cp-kafka-connect:7.8.3`, install only runtime CA certificates, clean `yum` metadata, and copy `/opt/connect-plugins` into `/usr/share/confluent-hub-components`.
- [ ] Re-run `rtk uv run pytest tests/unit/test_kafka_bronze_sink_runtime.py -q`.

Expected result: PASS; the pinned connector and Compose image contracts still pass while the builder/runtime boundary is asserted.

### Task 3: Build, measure, and compare the optimized image

**Files:**
- Create: `scripts/kafka/capture_connect_image_optimization.py`
- Create: `tests/unit/test_kafka_connect_image_optimization.py`
- Create: `evidence/00_engineering_fundamentals/kafka_connect_image_optimized.json`
- Create: `evidence/00_engineering_fundamentals/kafka_connect_image_history_optimized.txt`
- Create: `evidence/00_engineering_fundamentals/kafka_connect_image_comparison.json`
- Create: `evidence/00_engineering_fundamentals/kafka_connect_image_comparison.md`
- Create: `evidence/00_engineering_fundamentals/run_manifest.json`

- [ ] In `test_write_image_optimization_evidence_enforces_reduction_and_schema`, inject fixed `docker image inspect` and `docker history` outputs; assert byte-to-MiB conversion, `reduction_bytes = baseline - optimized`, the exact image/comparison output keys defined above, success for a positive reduction, and failure when optimized bytes are greater than or equal to baseline bytes.
- [ ] Run `rtk uv run pytest tests/unit/test_kafka_connect_image_optimization.py::test_write_image_optimization_evidence_enforces_reduction_and_schema -q`.

Expected result: FAIL because the capture script does not yet exist.

- [ ] Implement a command runner that calls `subprocess.run` with the command list plus `check=True`, `text=True`, and `capture_output=True`; invoke `docker image inspect` and `docker history --no-trunc`, write deterministic JSON keys sorted alphabetically, and preserve raw history text.
- [ ] Re-run `rtk uv run pytest tests/unit/test_kafka_connect_image_optimization.py::test_write_image_optimization_evidence_enforces_reduction_and_schema -q`.

Expected result: PASS; size calculations, positive-reduction enforcement, and both output schemas match the contract.

- [ ] Build the final image: `rtk docker build --no-cache --tag vina-bim-shop/kafka-connect:7.8.3-s3 -f infra/kafka/connect/Dockerfile infra/kafka/connect`.
- [ ] Run `rtk uv run python scripts/kafka/capture_connect_image_optimization.py --baseline-image vina-bim-shop/kafka-connect:7.8.3-s3-baseline --optimized-image vina-bim-shop/kafka-connect:7.8.3-s3 --evidence-root evidence/00_engineering_fundamentals`.

Expected result: exit code 0; comparison reports a positive `reduction_bytes`, a positive `reduction_percent`, both image sizes in bytes and MiB, and both history artifacts.

### Task 4: Prove plugin and connector runtime behavior

**Files:**
- Create: `evidence/00_engineering_fundamentals/kafka_connect_plugin_smoke.json`
- Create: `evidence/00_engineering_fundamentals/kafka_connect_bronze_sink_response.json`
- Regenerate: `evidence/00_engineering_fundamentals/run_manifest.json`

- [ ] Start the ingestion dependencies with `rtk docker compose --profile ingestion up -d --build`.
- [ ] Poll `http://localhost:8083/connector-plugins` until the response contains `io.confluent.connect.s3.S3SinkConnector`; store the matching plugin object in the smoke JSON.
- [ ] Run `rtk uv run python scripts/kafka/register_bronze_sink.py --connect-url http://localhost:8083 --template-path infra/kafka/connect/source-events-s3-sink.template.json --connector-name bronze-events-s3-sink --bronze-bucket bronze --minio-endpoint http://minio:9000 --minio-region us-east-1 --minio-access-key vina_minio --minio-secret-key vina_minio_password --evidence-root evidence/00_engineering_fundamentals`.
- [ ] Query `http://localhost:8083/connectors/bronze-events-s3-sink/status`, store the response in `kafka_connect_plugin_smoke.json`, and require a connector state of `RUNNING` or `UNASSIGNED` only when no task has received source records.
- [ ] Update the run manifest to list every engineering-fundamentals artifact and link the connector response written by the existing registration script.

Expected result: the S3 sink plugin is discoverable and the Bronze sink registration succeeds with the optimized image.

### Task 5: Run focused and regression checks

**Files:**
- Test: `tests/unit/test_kafka_connect_image_optimization.py`
- Test: `tests/unit/test_kafka_bronze_sink_runtime.py`
- Test: `tests/unit/test_kafka_compose_profile.py`
- Test: `tests/unit/test_kafka_smoke_scripts.py`

- [ ] Run the focused capture-script test: `rtk uv run pytest tests/unit/test_kafka_connect_image_optimization.py::test_write_image_optimization_evidence_enforces_reduction_and_schema -q`.
- [ ] Run `rtk uv run pytest tests/unit/test_kafka_bronze_sink_runtime.py -q`.
- [ ] Run the capture-script and Kafka regression set: `rtk uv run pytest tests/unit/test_kafka_connect_image_optimization.py tests/unit/test_kafka_bronze_sink_runtime.py tests/unit/test_kafka_compose_profile.py tests/unit/test_kafka_smoke_scripts.py -q`.
- [ ] Run `rtk docker compose --profile ingestion config --quiet`.

Expected result: all focused tests pass and Compose validates without changing the Kafka Connect image tag or service definition.

## Required Evidence Artifacts

- Create: `evidence/00_engineering_fundamentals/kafka_connect_image_baseline.json`
- Create: `evidence/00_engineering_fundamentals/kafka_connect_image_optimized.json`
- Create: `evidence/00_engineering_fundamentals/kafka_connect_image_history_baseline.txt`
- Create: `evidence/00_engineering_fundamentals/kafka_connect_image_history_optimized.txt`
- Create: `evidence/00_engineering_fundamentals/kafka_connect_image_comparison.json`
- Create: `evidence/00_engineering_fundamentals/kafka_connect_image_comparison.md`
- Create: `evidence/00_engineering_fundamentals/kafka_connect_plugin_smoke.json`
- Create: `evidence/00_engineering_fundamentals/kafka_connect_bronze_sink_response.json`
- Create: `evidence/00_engineering_fundamentals/run_manifest.json`

## Definition of Done

- The Kafka Connect Dockerfile is a meaningful two-stage builder/runtime image and retains the pinned base image and S3 connector version.
- Baseline and optimized image sizes are recorded in bytes and MiB from the same Docker engine.
- The comparison proves a positive measured reduction and preserves raw `docker history` output for both images.
- The S3 connector is discoverable and `bronze-events-s3-sink` registers successfully using the optimized image.
- Focused Kafka Connect tests, Kafka regression tests, and Compose config validation pass.
- No staging or commit occurs unless the user explicitly requests it.

## Completion Record

- Executed on 2026-07-10 in the current `feature/finalize-edai1` checkout; no worktree, staging, or commit was created.
- Baseline image: `vina-bim-shop/kafka-connect:7.8.3-s3-baseline`, image ID `sha256:d7a9cba83abf27f35020eff70c01a941b6e8bb454262ae1fd56685359247c878`, `1549476393` bytes (`1477.70` MiB).
- Optimized image: `vina-bim-shop/kafka-connect:7.8.3-s3`, image ID `sha256:613434b8058c6a13134176fafef01c281fb62c10568b0c82348737b703f90d32`, `1540697383` bytes (`1469.32` MiB).
- Measured reduction: `8779010` bytes (`8.37` MiB, `0.57%`). Both images were built with `rtk docker build --no-cache` on the same Docker engine; the baseline was frozen before the Dockerfile change.
- Image evidence command: `rtk uv run python scripts/kafka/capture_connect_image_optimization.py --baseline-image vina-bim-shop/kafka-connect:7.8.3-s3-baseline --optimized-image vina-bim-shop/kafka-connect:7.8.3-s3 --evidence-root evidence/00_engineering_fundamentals`.
- Runtime proof used `kafka`, `schema-registry`, `kafka-connect`, and `minio`, initialized the Bronze bucket, bootstrapped topics, and registered `bronze-events-s3-sink`. `GET /connector-plugins` returned `io.confluent.connect.s3.S3SinkConnector` version `10.6.4`; the connector and task `0` both reported `RUNNING`.
- Verification: focused capture test passed (`1`), Bronze sink runtime tests passed (`5`), the Kafka regression set passed (`14`), and `rtk docker compose --profile ingestion config --quiet` exited `0`.
- Evidence: `evidence/00_engineering_fundamentals/kafka_connect_image_baseline.json`, `kafka_connect_image_optimized.json`, both image-history files, `kafka_connect_image_comparison.json`, `kafka_connect_image_comparison.md`, `kafka_connect_plugin_smoke.json`, `kafka_connect_bronze_sink_response.json`, and `run_manifest.json`.
- Residual limitations: the reduction is positive but modest because the pinned Kafka Connect base and required S3 plugin dominate the final image. This Topic 01 smoke test proves plugin discovery and a running sink task, but does not publish a source record or assert a resulting MinIO object.
- Teardown: `rtk docker compose --profile ingestion --profile lakehouse down` stopped and removed the runtime containers and network without deleting named volumes.
