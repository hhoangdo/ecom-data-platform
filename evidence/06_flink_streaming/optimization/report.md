# Flink Baseline and Streaming Proof

## Method

- Replay artifact: [`replay.ndjson`](replay.ndjson), SHA-256 `43ff67a48371d20ca6793f62cb94a320632b90386a6a1febf92301bea588bbd7`.
- The replay publishes 16 byte-preserved records in `initial_business_events`, `watermark_control_event`, and `late_correction_event` phases. The control event closes the first event-time window before `evt-8` arrives.
- Baseline job: `vina-bim-shop-flink-baseline` (`78e3cd83407fce7360cdaf38e70a2509`); optimized job: `vina-bim-shop-flink-optimized` (`88cc24c1eec249d6f00a0571f51c7ee8`). Each was captured, then deliberately canceled after evidence collection because the Kafka sources are unbounded.

## Results

| Check | Baseline | Optimized |
| --- | ---: | ---: |
| Observed duration | 39,159 ms | 53,439 ms |
| On-time commerce metrics | 3 | 3 |
| Ops alerts | 7 | 7 |
| Late correction records | 0 | 1 |
| Checkpoints | Disabled | 1 completed |
| On-time aggregate equality | — | Pass |

The baseline uses parallelism `1`, zero watermark out-of-orderness, zero allowed lateness, and no checkpointing. The optimized profile inherits the canonical one-minute window, five-second watermark, `300/600/900/120`-second allowed-lateness values, and exactly-once checkpoint configuration. Both profiles use isolated consumer groups, derived topics, checkpoint prefixes, and curated-output prefixes.

## Rubric Proof

### Row 21 - Baseline and Optimized Comparison

[`comparison.json`](comparison.json) records matching replay hashes and identical on-time aggregates while separating the optimized late correction. [`flink_baseline_job.png`](../screenshots/flink_baseline_job.png) and [`flink_optimized_job.png`](../screenshots/flink_optimized_job.png) show the distinct Flink job IDs; the optimized image shows its completed checkpoint.

### Row 22 - Burst

[`challenge_samples.json`](challenge_samples.json) maps `ops-1` to the `traffic_burst` alert with `burst_event_count: 120`.

### Row 23 - Late Arrival

The `evt-8` replay produces no baseline correction and one optimized `late_event` correction targeting `realtime_commerce_metrics_1m_optimized`.

### Row 24 - Duplicate

The two `evt-3` inputs produce one on-time optimized metric with `duplicate_event_count: 1`, without double-counting the paid order.

### Row 25 - Event-Time Window

The control event advances the watermark and yields the successful `2026-05-01T10:00:00+00:00` to `2026-05-01T10:01:00+00:00` tumbling event-time window captured in the challenge sample.

## Limitations

- This is a correctness and evidence experiment on one local TaskManager, not a cluster-throughput benchmark.
- The baseline is intentionally unsafe for production; its zero lateness and disabled checkpointing are comparison controls only.
- Jobs are canceled after capture because their Kafka sources are continuous. The manifests retain both observed `RUNNING` capture state and final `CANCELED` state.
