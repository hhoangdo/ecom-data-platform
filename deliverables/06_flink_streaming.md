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

The full set of streaming behaviors is implemented in:

| Behavior | Code |
| --- | --- |
| Bounded out-of-orderness watermark | `src/vina_bim_shop/flink/runtime.py:event_timestamp_assigner` |
| Event-time timestamp extraction | `src/vina_bim_shop/flink/runtime.py:event_timestamp_millis` |
| One-minute tumbling event-time window | `src/vina_bim_shop/flink/commerce_job.py` `.window(TumblingEventTimeWindows.of(Time.minutes(...)))` |
| Per-topic allowed lateness | `src/vina_bim_shop/flink/commerce_job.py` `.allowed_lateness(...)` |
| Dedupe by `event_id` | `src/vina_bim_shop/flink/metrics.py:dedupe_events` |
| `duplicate_event_count` and `late_event_count` in the snapshot | `src/vina_bim_shop/flink/metrics.py:build_metric_snapshot` |
| Correction version + reason | `src/vina_bim_shop/flink/corrections.py:build_correction_record` |
| 30-second `EXACTLY_ONCE` checkpointing to MinIO | `src/vina_bim_shop/flink/runtime.py:configure_checkpointing` |
| Three derived Kafka topics | `src/vina_bim_shop/flink/commerce_job.py` (sinks) and `configs/pipelines/flink_streaming.yaml` |
| JSONL curated file sinks | `src/vina_bim_shop/flink/runtime.py:jsonl_file_sink` |
| Ops signal -> alert mapping | `src/vina_bim_shop/flink/alerts.py:OPS_ALERT_TYPE_MAP` and `src/vina_bim_shop/flink/ops_job.py` filter |

## Service Interactions

Flink sits between Kafka and Pinot, with MinIO used for operational state and audit evidence.

| Service | Relationship |
| --- | --- |
| Kafka | Kafka source topics provide raw commerce, catalog, fulfillment, and ops events. Flink publishes only derived topics back to Kafka. |
| Schema Registry | Event contracts are managed at ingestion time; Flink consumes the topic payloads according to those source contracts. |
| MinIO | Flink writes checkpoints to the `checkpoints/flink/` prefix (30-second `EXACTLY_ONCE` interval) and mirrors selected JSONL audit rows to `evidence/streaming_curated/`. |
| Pinot | Pinot consumes the Flink-derived topics for realtime OLAP serving; `realtime_metric_corrections` lets Pinot apply late-event corrections without double-counting. |
| Airflow | Airflow orchestrates batch and control-plane work only. Airflow does not monitor or restart Flink in v1. Airflow must not monitor or restart Flink in v1. |
| DataHub | Streaming lineage is emitted after the platform assets exist, connecting source Kafka topics, Flink processing, and derived topics. |

## Data Challenge Handling

The map from generator challenges to Flink code paths is documented in
[11 Solving Data Challenges](11_solving_data_challenges.md). Highlights:

- **Event time and watermarks**: `WatermarkStrategy.for_bounded_out_of_orderness` in
  `runtime.py:event_timestamp_assigner`; the `TimestampAssigner` reads `event_timestamp`
  in `runtime.py:event_timestamp_millis`.
- **One-minute tumbling event-time windows**:
  `TumblingEventTimeWindows.of(Time.minutes(config.window_minutes))` in `commerce_job.py`.
- **Per-topic allowed lateness**: `.allowed_lateness(config.allowed_lateness_seconds[topic] * 1000)`.
- **Dedupe**: `metrics.py:dedupe_events` keeps one event per `event_id` and the snapshot
  carries `duplicate_event_count`.
- **Corrections**: `commerce_job.py:CommerceWindowProcessor` emits to
  `realtime_metric_corrections` on re-sightings, with `correction_version` and
  `correction_reason` set by `corrections.py:build_correction_record`.
- **Checkpointing**: 30-second `EXACTLY_ONCE` checkpoints persisted to MinIO via
  `runtime.py:configure_checkpointing`.
- **Derived Kafka topics**: `realtime_commerce_metrics_1m`,
  `realtime_metric_corrections`, and `realtime_ops_alerts` are the only topics Flink
  publishes.
- **Operational signals**: `ops_job.py` filters `traffic_burst_detected`,
  `late_arrival_observed`, `duplicate_event_observed`, `inventory_low_stock`,
  `shipment_delayed`, `shipment_blocked_payment_failed`; `alerts.py` maps them to a
  stable `alert_type` and emits a uniform alert contract.

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

## Topic 04 Baseline And Streaming Proof

The canonical streaming configuration remains unchanged. The experiment uses one composed job
per variant so both commerce and ops outputs share the same isolated replay boundary. Each job is
captured while running and then intentionally canceled because Kafka sources are unbounded.

### Row 21 - Baseline and Optimized Comparison

The baseline job [`vina-bim-shop-flink-baseline`](../evidence/06_flink_streaming/optimization/baseline_metrics.json)
uses parallelism `1`, no checkpointing, zero watermark out-of-orderness, and zero allowed
lateness. The optimized job
[`vina-bim-shop-flink-optimized`](../evidence/06_flink_streaming/optimization/optimized_metrics.json)
inherits the canonical five-second watermark and `300/600/900/120`-second lateness contract.
Both consume replay SHA-256 `43ff67a48371d20ca6793f62cb94a320632b90386a6a1febf92301bea588bbd7`.
[`comparison.json`](../evidence/06_flink_streaming/optimization/comparison.json) proves their
on-time aggregates are equal; [baseline UI](../evidence/06_flink_streaming/screenshots/flink_baseline_job.png)
and [optimized UI](../evidence/06_flink_streaming/screenshots/flink_optimized_job.png) show the
two job IDs, with one completed optimized checkpoint.

### Row 22 - Burst Proof

The deterministic `ops-1` input produces `traffic_burst` with `burst_event_count: 120` in
[`challenge_samples.json`](../evidence/06_flink_streaming/optimization/challenge_samples.json).

### Row 23 - Late Arrival Proof

The replay advances the event-time watermark before publishing `evt-8`. The baseline records zero
corrections; the optimized path emits one `late_event` correction to its isolated metric topic.
The source event and correction snapshot are retained in
[`challenge_samples.json`](../evidence/06_flink_streaming/optimization/challenge_samples.json).

### Row 24 - Duplicate Proof

The two `evt-3` inputs yield one paid-order metric with `duplicate_event_count: 1`, proving that
the duplicate is observed without inflating the aggregate. See
[`challenge_samples.json`](../evidence/06_flink_streaming/optimization/challenge_samples.json).

### Row 25 - Event-Time Window Proof

The sample records the successful one-minute `TumblingEventTimeWindows` output for
`2026-05-01T10:00:00+00:00` through `2026-05-01T10:01:00+00:00`; the implementation remains in
`commerce_job.py` and the runtime result is in
[`challenge_samples.json`](../evidence/06_flink_streaming/optimization/challenge_samples.json).

The full method, IDs, settings, output counts, and limitations are recorded in
[`optimization/report.md`](../evidence/06_flink_streaming/optimization/report.md).

## Limitations

- The realtime layer is fresh and provisional; it does not replace reconciled Spark Gold tables.
- Flink jobs are long-running runtime processes, while Airflow is only the control plane for local evidence workflows.
- The clean-room verifier resets only selected streaming and serving state so committed evidence and lakehouse data remain intact.
- Flink does not consume the `dead_letter_events` topic; bad event payloads are quarantined by the Spark batch path only. See
  [11 Solving Data Challenges](11_solving_data_challenges.md#honest-gaps-and-future-work) for the documented future action.

## Convenience Make Targets

The root [Makefile](../Makefile) wraps the most common commands for this deliverable under a `make + verb` convention. It delegates to the official scripts in [scripts/](../scripts/) and to `scripts/ctl.py` for Docker Compose lifecycle; compose targets expand documented profile bundles and prebuild shared images where needed.

| Target | What it does |
| --- | --- |
| `make up-streaming` | Start the streaming profile (Flink JobManager, TaskManager, job submitter). |
| `make down-streaming` | Stop the streaming profile and remove its volumes. |
| `make up-ingestion` | Start the ingestion profile (Flink reads from Kafka source topics). |
| `make down-ingestion` | Stop the ingestion profile and remove its volumes. |
| `make up-lakehouse` | Start the lakehouse profile (Fink checkpoints land in MinIO). |
| `make down-lakehouse` | Stop the lakehouse profile and remove its volumes. |
