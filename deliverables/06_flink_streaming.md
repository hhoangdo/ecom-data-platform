# Flink Streaming

## Purpose

Apache Flink is the realtime processing engine for the Vina Bim Shop platform. It reads event streams from Kafka, applies event-time logic, and publishes derived topics that are suitable for live dashboards and operational alerting.

The streaming path is intentionally separate from the batch lakehouse path. Flink optimizes for freshness and operational visibility, while Spark Gold through Trino remains the canonical reconciled source for financial and executive reporting.

## Why Flink Is Needed

Kafka stores events, but it does not calculate rolling metrics, reason about late events, or maintain event-time windows by itself. The project needs a stream processor because the e-commerce platform has questions that cannot wait for the next batch cycle:

| Pain point | How Flink addresses it |
| --- | --- |
| Operators need live conversion and payment signals. | Flink calculates one-minute commerce metrics from Kafka source events. |
| Events can arrive out of order. | Flink uses event time, watermarks, and topic-specific allowed lateness instead of relying only on processing time. |
| Late events can change an already published minute. | Flink emits correction snapshots through `realtime_metric_corrections`. |
| Operational events need normalization before serving. | Flink turns catalog, fulfillment, and ops events into normalized alert rows. |
| Pinot should not consume raw source topics. | Flink creates compact derived topics that match the realtime serving contract. |

This makes the speed layer useful without pretending it is the final source of truth.

## Implemented Jobs

The streaming profile runs two long-running jobs:

| Job | Source topics | Outputs | Responsibility |
| --- | --- | --- | --- |
| `vina-bim-shop-commerce-metrics` | `commerce_events` | `realtime_commerce_metrics_1m`, `realtime_metric_corrections`, selected payment-failure alerts | Calculates minute-level commerce metrics and emits correction snapshots when late events affect an already emitted window. |
| `vina-bim-shop-ops-alerts` | `ops_events`, `catalog_events`, `fulfillment_events` | `realtime_ops_alerts` | Normalizes operational signals into a single alert stream for Pinot and audit evidence. |

The implementation assets live in:

| Asset | Path |
| --- | --- |
| Streaming configuration | `configs/pipelines/flink_streaming.yaml` |
| Commerce job entrypoint | `scripts/flink/run_commerce_metrics_job.py` |
| Ops-alert job entrypoint | `scripts/flink/run_ops_alerts_job.py` |
| Flink package | `src/vina_bim_shop/flink/` |
| Smoke fixture publisher | `scripts/flink/publish_smoke.py` |
| Evidence capture | `scripts/flink/capture_evidence.py` |
| Clean-room verification | `scripts/flink/cleanroom_verify.py` |

## Event-Time And Late Data Handling

Flink is configured around event time rather than container clock time. This matters because a commerce event may be produced late, replayed after a reset, or delivered after another event from the same order.

The runtime configuration is:

| Setting | Value | Meaning |
| --- | --- | --- |
| Window size | `1` minute | Commerce metrics are aggregated by minute. |
| Watermark out-of-orderness | `5` seconds | Events may arrive slightly out of order before a window is considered ready. |
| `commerce_events` allowed lateness | `300` seconds | Late commerce activity can update recent live metrics. |
| `catalog_events` allowed lateness | `600` seconds | Catalog changes are allowed a wider correction window. |
| `fulfillment_events` allowed lateness | `900` seconds | Shipment and delivery updates can arrive well after the original order event. |
| `ops_events` allowed lateness | `120` seconds | Operational alerts are time-sensitive and use a shorter tolerance. |

Late-arriving commerce data is handled with correction snapshots:

1. Flink emits an initial one-minute metric row to `realtime_commerce_metrics_1m`.
2. If a late event changes the same `metric_key`, Flink recomputes the full minute snapshot.
3. The recomputed row is emitted to `realtime_metric_corrections` with correction metadata such as the correction reason.
4. Pinot queries use the latest correction snapshot for a metric key instead of blindly adding the original and corrected rows together.

This design keeps the realtime layer honest: dashboards can be fast, and late-data behavior is explicit.

## Service Interactions

Flink sits between Kafka and Pinot, with MinIO used for operational state and audit evidence.

| Service | Relationship |
| --- | --- |
| Kafka | Kafka source topics provide raw commerce, catalog, fulfillment, and ops events. Flink publishes only derived topics back to Kafka. |
| Schema Registry | Event contracts are managed at ingestion time; Flink consumes the topic payloads according to those source contracts. |
| MinIO | Flink writes checkpoints to the `checkpoints/flink/` prefix and mirrors selected JSONL audit rows to `evidence/streaming_curated/`. |
| Pinot | Pinot consumes the Flink-derived topics for realtime OLAP serving. |
| Airflow | Airflow orchestrates batch and control-plane work only. Airflow does not monitor or restart Flink in v1. Airflow must not monitor or restart Flink in v1. |
| DataHub | Streaming lineage is emitted after the platform assets exist, connecting source Kafka topics, Flink processing, and derived topics. |

## Derived Topic Contracts

| Topic | Grain | Role |
| --- | --- | --- |
| `realtime_commerce_metrics_1m` | One row per minute and commerce metric key | Fresh dashboard metric stream. |
| `realtime_metric_corrections` | One correction snapshot per affected metric key and correction event | Late-data correction stream used by reconciliation and Pinot query logic. |
| `realtime_ops_alerts` | One alert candidate per normalized operational signal | Operational alert stream for live review. |

The derived topics are intentionally narrower than the raw topics. They are serving contracts, not general-purpose source-of-record tables.

## Runtime Profile

Start the streaming services after ingestion and lakehouse dependencies are healthy:

```powershell
docker compose --profile ingestion --profile lakehouse --profile streaming up -d
```

The `streaming` profile starts:

| Service | Responsibility |
| --- | --- |
| `flink-jobmanager` | Coordinates Flink jobs and exposes the Flink REST/UI endpoint. |
| `flink-taskmanager` | Executes stream processing tasks. |
| `flink-job-submit` | Waits for the JobManager REST API and submits the commerce and ops jobs when needed. |

Local URL:

| Service | URL |
| --- | --- |
| Flink UI | `http://localhost:8086` |

The local containers use a runtime guard so Flink does not run indefinitely on a constrained machine:

| Variable | Default role |
| --- | --- |
| `VBS_FLINK_MAX_RUNTIME_MINUTES` | Caps local runtime duration. |
| `VBS_FLINK_MAX_RUNTIME_GRACE_SECONDS` | Adds a short grace period before shutdown. |
| `VBS_FLINK_DISABLE_AUTO_STOP` | Allows the guard to be disabled for manual experiments. |

## Verification And Evidence

The smoke publisher creates deterministic events that cover:

| Scenario | Expected behavior |
| --- | --- |
| Normal commerce activity | A one-minute metric row is produced. |
| Duplicate commerce event | Duplicate handling prevents inflated metric interpretation. |
| Late commerce event | A correction snapshot is emitted. |
| Ops burst | Alert rows are produced. |
| Late and duplicate ops signals | Alerts remain normalized and auditable. |
| Payment-failure spike | Payment alert conditions are represented in the output stream. |

Useful commands:

```powershell
uv run python scripts/flink/publish_smoke.py
uv run python scripts/flink/capture_evidence.py
uv run python scripts/flink/cleanroom_verify.py --phase all
```

Committed evidence is stored under `evidence/06_flink_streaming/`. Runtime-only clean-room evidence is written under `evidence/runtime/cleanroom/`.

Evidence includes:

| Artifact type | Purpose |
| --- | --- |
| Flink REST JSON | Shows JobManager, job, and taskmanager health. |
| Derived topic samples | Proves the jobs emitted expected rows. |
| Checkpoint listings | Shows stateful streaming wrote operational state. |
| Curated JSONL audit outputs | Preserves correction and alert examples for inspection. |
| Run manifest | Records the capture context and artifact inventory. |

## Limitations

- The realtime layer is fresh and provisional; it does not replace reconciled Spark Gold tables.
- Flink jobs are long-running runtime processes, while Airflow is only the control plane for local evidence workflows.
- The clean-room verifier resets only selected streaming and serving state so committed evidence and lakehouse data remain intact.
