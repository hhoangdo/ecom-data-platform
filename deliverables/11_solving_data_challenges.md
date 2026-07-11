# 11 Solving Data Challenges

## Purpose

The generator described in [01 Data Generator](01_data_generator.md) injects a fixed set of
realistic data challenges. This deliverable is the map that explains how Spark (batch) and
Flink (streaming) handle each one, with code references and honest gaps.

If a reader only has time to read one section of the platform documentation, this is it:
the link between "the generator produces imperfect data" and "the engine decisions and code
that make the platform trustworthy".

## Why The Map Exists

The platform should never look like a "perfect demo" with hand-cleaned inputs. Each challenge
is a designed test case for the lakehouse and streaming path:

- **Batch** is the reconciled source of truth. Spark SQL produces Silver and Gold tables that
  are queried by Trino and exported to DuckDB. If Spark swallows dirty data, all BI is
  untrustworthy.
- **Streaming** is the freshness layer. Flink emits one-minute commerce metrics, ops alerts,
  and correction snapshots that Pinot consumes. If Flink drops or miscounts, dashboards lie
  before the next batch cycle reconciles them.

The map documents the **current** behavior of both engines. It is not a wish list; gaps are
called out as gaps and parked under [Honest Gaps And Future Work](#honest-gaps-and-future-work).

## Spark (Batch) Engine

### Engine Profile

- Implementation language: Python with `pyspark.sql` (`SparkSession`, `DataFrame`,
  `Window`, `functions as F`, `types as T`).
- Engine entry point: `src/vina_bim_shop/lakehouse/spark/job.py:run_job` builds a
  `SparkSession` with the Iceberg Hive catalog and the MinIO `s3a` warehouse.
- Distributed readers: `spark.read.parquet` for batch snapshots and `spark.read.json` for
  Kafka event landings (`src/vina_bim_shop/lakehouse/spark/job.py:81-100`).
- Silver builders: distributed `DataFrame` operations with `Window` and `row_number`
  dedupe, JSON payload extraction, and timestamp casting.
- Gold builders: Spark SQL `CREATE OR REPLACE TABLE` against `iceberg.gold` from the SQL
  templates in `src/vina_bim_shop/lakehouse/spark/sql.py:ordered_gold_queries()`.
- Idempotent writes: `MERGE INTO` for Silver, `CREATE OR REPLACE TABLE` for Gold
  (`src/vina_bim_shop/lakehouse/spark/job.py:368-411`).
- Validation: `src/vina_bim_shop/lakehouse/spark/validation.py:run_pyspark_validations`
  asserts not-null/uniqueness, accepted values, dimensional relationships, and
  reconciliation deltas.

### Skew Policy (Controlled)

- The canonical Spark batch path does not use salting. `job.py` retains its Bronze-to-Silver-
  to-Gold behavior, Iceberg writes, and Airflow integration unchanged.
- A standalone optimization experiment derives a deterministic 150,000-row coursework input
  and compares a city repartition baseline with a salted/repartitioned variant. It applies
  `pmod(xxhash64(customer_id), 16)` only to `Ho Chi Minh City` and `Ha Noi`; all other cities
  use bucket `0`.
- AQE is disabled for both runs. The experiment persists exact sorted city aggregates,
  pre-aggregate partition distributions, elapsed times, and History Server application pages.
  [Skew equivalence](../evidence/05_spark_batch/optimization/skew_equivalence.json) is the
  correctness gate; elapsed time is reported as observed rather than guaranteed improvement.

### Challenge Handling Table

| Generator challenge | Why it matters | Spark handling | Code reference |
| --- | --- | --- | --- |
| Geographic skew (HCMC/Ha Noi ~45%) | BI must reflect urban concentration, not smooth it away. | The standalone optimization experiment proves deterministic targeted salting/repartitioning and exact city-result equality. The canonical batch path remains unsalted and reconciles hourly. | `optimization_experiments.py`, `sql.py:agg_hourly_reconciled_kpi`, `sql.py:dim_customer` |
| Category skew (FMCG/ELHA) | Imbalance creates realistic demand and margin pressure. | Per-category cost rates, distributed joins, per-category dimension rows. | `sql.py:category_cost_rate_sql`, `sql.py:dim_category` |
| Offline duplicate payloads | `order_items` carries exact duplicates; double-counting breaks revenue. | Window dedupe by stable business key, latest `created_ts` wins. | `job.py:_dedupe_latest`, `job.py:_build_silver_tables_for_window` |
| Streaming duplicate events | Same envelope may be re-delivered by Kafka or replayed after a reset. | `stg_commerce_events` dedupes by `event_id` order by `created_ts desc`. | `job.py:_build_silver_tables_for_window` (commerce block) |
| Late arrivals | Events with `event_timestamp` far from `created_ts`. | Silver event tables filter by `[start_ts, end_ts)` so windows stay stable. Late rows are still visible to Flink via Kafka and reconciled by batch. | `job.py:_build_silver_tables_for_window` (event `.where(...)`) |
| Missing shipping method | `orders.shipping_method` is null in some rows. | `coalesce(shipping_method, 'unknown')` in both the dimension and the fact. | `sql.py:dim_shipping_method`, `sql.py:fact_shipment` |
| Missing brand (post-change) | `products.brand` is null in some rows. | `dim_product` normalizes `coalesce(p.brand, 'unknown') as brand` so the dimension is never null. `stg_products` keeps the raw null for fidelity. | `sql.py:dim_product` |
| Missing device metadata | `commerce_events.payload.device_type` can be missing. | Streaming metric snapshots preserve nullable dimensions; `feat_stream_60m` aggregates tolerate nulls. | `sql.py:feat_stream_60m` |
| Schema evolution | Older slices miss newer fields (e.g. `category_attributes`). | Every Silver event table preserves `schema_version` as a typed column; JSON fields are extracted with `get_json_object` so missing paths produce null, not errors. | `job.py:_build_silver_tables_for_window` (event blocks) |
| Bad event payloads | `dead_letter_events` has `missing_required_key`, `invalid_json`, `invalid_timestamp`, `unknown_schema_version`. | `_read_optional_dead_letter_events` reads them, projects a stable schema, and registers `raw_bad_events` for quarantine. | `job.py:_read_optional_dead_letter_events` |
| Bad snapshot payloads | `bad_snapshots/bad_snapshots.jsonl` carries malformed batch extracts. | `_read_optional_bad_snapshots` reads the JSONL from `data/raw/bad_snapshots/`, registers `raw_bad_snapshots`, and `stg_bad_snapshots` is a real Silver table. | `job.py:_read_optional_bad_snapshots`, `job.py:_build_silver_tables_for_window` (bad_snapshots block) |
| Operational signals | `ops_events` carries `traffic_burst_detected`, `late_arrival_observed`, `duplicate_event_observed`, `schema_version_changed`. | `stg_ops_events` keeps them as a Silver fact so audit and governance can replay the same view that Flink alerted on. | `job.py:_build_silver_tables_for_window` (ops block), `sql.py:fact_payment_attempt` joins, `validation.py:dim_product.brand.not_null` |

### End-To-End Flow

```text
MinIO Bronze (s3a://bronze/batch, s3a://bronze/events)
  -> raw_* and raw_kafka_* temp views
  -> raw_bad_events, raw_bad_snapshots (quarantine views)
  -> stg_* Silver tables (Window/row_number dedupe, typed casts)
  -> stg_bad_snapshots, stg_commerce_events, stg_ops_events (challenge handling)
  -> Iceberg Gold: dim_*, fact_*, obt_*, agg_*, feat_*
  -> Trino smoke, DuckDB Executive Mart export, PySpark + Great Expectations validations
```

## Flink (Streaming) Engine

### Engine Profile

- Implementation language: Python with `pyflink.datastream`
  (`StreamExecutionEnvironment`, `TumblingEventTimeWindows`, `ProcessWindowFunction`).
- Entry points: `src/vina_bim_shop/flink/commerce_job.py:run` and
  `src/vina_bim_shop/flink/ops_job.py:run`.
- Kafka source: `runtime.py:kafka_source` builds a `KafkaSource` with the topic and
  `KafkaOffsetsInitializer.earliest()`.
- Watermarks: `runtime.py:event_timestamp_assigner` configures
  `WatermarkStrategy.for_bounded_out_of_orderness(Duration.of_seconds(N))` and a
  `TimestampAssigner` that parses `event_timestamp` from each record.
- Windows: `commerce_job.py` uses `TumblingEventTimeWindows.of(Time.minutes(1))` and
  `.allowed_lateness(seconds * 1000)` driven by per-topic config.
- Dedupe: `metrics.py:dedupe_events` keeps one event per `event_id` and exposes
  `duplicate_event_count`.
- Corrections: `commerce_job.py:CommerceWindowProcessor` emits to
  `realtime_commerce_metrics_1m` on the first sighting and to
  `realtime_metric_corrections` (with `correction_version` and `correction_reason`) on
  re-sightings.
- Checkpointing: `runtime.py:configure_checkpointing` enables a 30-second interval,
  `CheckpointingMode.EXACTLY_ONCE`, `RETAIN_ON_CANCELLATION`, and MinIO-backed
  `FileSystemCheckpointStorage`.
- Derived topics: `realtime_commerce_metrics_1m`, `realtime_metric_corrections`,
  `realtime_ops_alerts` (see `configs/pipelines/flink_streaming.yaml`).
- Curated file sinks: `runtime.py:jsonl_file_sink` writes JSONL evidence under
  `evidence/streaming_curated/`.
- Config: `configs/pipelines/flink_streaming.yaml` sets `window_minutes: 1`,
  `out_of_orderness_seconds: 5`, and per-topic `allowed_lateness_seconds`.

### Challenge Handling Table

| Generator challenge | Why it matters | Flink handling | Code reference |
| --- | --- | --- | --- |
| Geographic skew | Skewed city traffic must not break minute-level metrics. | Key selector includes `primary_category`, `source`, `device_type`, `payment_method`, `order_status`, `schema_version`; skew shows up in metric keys, not in crashes. | `commerce_job.py` `.key_by(...)` |
| Category skew | Dominant categories must not starve minority ones. | Same key strategy; per-window processor counts and dedupes uniformly. | `commerce_job.py`, `metrics.py:build_metric_snapshot` |
| Streaming duplicate events | Kafka re-delivery or replay must not double-count. | `dedupe_events` by `event_id`; `duplicate_event_count` exposed in the snapshot. | `metrics.py:dedupe_events`, `metrics.py:build_metric_snapshot` |
| Late arrivals | Events created minutes after `event_timestamp`. | Bounded out-of-orderness watermark (5s) + per-topic allowed lateness (300/600/900/120s). A re-sighting emits a `realtime_metric_corrections` row with `correction_version` incremented and `correction_reason = late_event`. | `runtime.py:event_timestamp_assigner`, `commerce_job.py:.allowed_lateness`, `commerce_job.py:CommerceWindowProcessor`, `corrections.py:build_correction_record` |
| Missing device metadata | Some `device_type` values are null. | The dimension is included in the metric key as null and the snapshot preserves it; downstream consumers can use `coalesce` in Pinot. | `metrics.py:_dimension_values`, `metrics.py:DIMENSION_FIELDS` |
| Schema evolution | `schema_version` changes when payload shape changes. | `schema_version` is part of the key selector and the snapshot, so old/new payloads do not collide. | `commerce_job.py` `.key_by(...)`, `metrics.py:DIMENSION_FIELDS` |
| Bad event payloads | DLQ examples should not corrupt the live metrics stream. | The DLQ topic is not consumed by the commerce or ops jobs. Bronze-side handling in Spark is the official quarantine. | (not consumed by Flink) |
| Operational signals | `ops_events` carries alerts and observability signals. | `ops_job.py` filters `traffic_burst_detected`, `late_arrival_observed`, `duplicate_event_observed`, `inventory_low_stock`, `shipment_delayed`, `shipment_blocked_payment_failed`; `alerts.py:normalize_source_alert_event` maps each `event_type` to a stable `alert_type`; results go to `realtime_ops_alerts`. | `ops_job.py`, `alerts.py:OPS_ALERT_TYPE_MAP`, `alerts.py:normalize_source_alert_event` |

### End-To-End Flow

```text
Kafka source topics
  -> event-time timestamp assigner (bounded out-of-orderness watermark)
  -> 1-minute tumbling event-time window
  -> .allowed_lateness(per-topic)
  -> CommerceWindowProcessor (dedupe, count, derive payment-failure alerts)
  -> realtime_commerce_metrics_1m (first sighting) OR
     realtime_metric_corrections  (re-sighting, with correction_version)
  -> realtime_ops_alerts (when threshold crossed)
  -> MinIO checkpoints every 30s (EXACTLY_ONCE)
  -> evidence/streaming_curated/ JSONL file sink
```

### Topic 04 Measured Flink Proof

The controlled Flink experiment replays the same byte-preserved 16-event fixture through isolated
baseline and optimized jobs. [`comparison.json`](../evidence/06_flink_streaming/optimization/comparison.json)
records equal on-time aggregates and the optimized-only late correction. The four direct input to
output examples for burst, late arrival, duplicates, and the one-minute event-time window are in
[`challenge_samples.json`](../evidence/06_flink_streaming/optimization/challenge_samples.json).
The job IDs, configuration deltas, UI proof, and limitations are consolidated in
[`optimization/report.md`](../evidence/06_flink_streaming/optimization/report.md).

## Honest Gaps And Future Work

| Gap | Why it matters | Future action |
| --- | --- | --- |
| No automatic salting in the canonical batch path | The controlled experiment proves one targeted mitigation, but production still requires a measured hotspot before its semantics change. | Keep the standalone evidence as the decision record; add production shuffle-metric alerting only when a production change is approved. |
| `stg_bad_snapshots` reads from the local raw root only (via `VBS_RAW_ROOT`) | Bad snapshots are not uploaded to MinIO by the standard batch landing; they live on the local filesystem. | Extend `src/vina_bim_shop/lakehouse/bronze.py` to optionally upload `bad_snapshots` to a dedicated MinIO prefix, or keep the local path but make the resolution explicit. |
| `dim_product.brand` is normalized to `'unknown'` (post-change) | A reviewer expecting strict null preservation will see a different value. | If a future use case requires strict null preservation, replace the `coalesce` with `p.brand` and remove the `dim_product.brand.not_null` validation. |
| Spark batch does not run the Flink cleanroom verifier | Spark and Flink are validated independently. | Add a `post-batch` step that asserts `agg_hourly_reconciled_kpi` matches a streaming `realtime_commerce_metrics_1m` aggregate for the same window. |
| DLQ (`dead_letter_events`) is consumed only by Spark, not by Flink | Flink cannot add a per-event quarantine path without losing minute-level freshness. | If realtime DLQ alerting is required, add a third Flink job that consumes the DLQ topic and emits a `dead_letter_alert` to `realtime_ops_alerts`. |

## Cross-References

- [01 Data Generator](01_data_generator.md) - the source of every challenge listed above.
- [05 Spark Batch](05_spark_batch.md) - the engine entry point, services, and evidence.
- [06 Flink Streaming](06_flink_streaming.md) - the streaming engine, jobs, and evidence.
- [02 Schema Design](02_schema_design.md) - schema contracts for raw, Silver, and Gold.
- [evidence/01_data_generator/issue_manifest.csv](../evidence/01_data_generator/issue_manifest.csv) -
  observed rates for each challenge in the medium run.
- [evidence/01_data_generator/quality_report.md](../evidence/01_data_generator/quality_report.md) -
  quality report summarising the injected issues.
- [evidence/01_data_generator/rubric_evidence_summary.md](../evidence/01_data_generator/rubric_evidence_summary.md) -
  consolidated source-side skew, schema, duplicate, burst, lateness, and streaming-duplicate facts used by this Spark and Flink challenge map.

## Convenience Make Targets

The root [Makefile](../Makefile) wraps the most common commands for this deliverable under a `make + verb` convention. It delegates to the official scripts in [scripts/](../scripts/) and to `scripts/ctl.py` for Docker Compose lifecycle; compose targets expand documented profile bundles and prebuild shared images where needed.

| Target | What it does |
| --- | --- |
| `make install` | `uv sync` to install Python dependencies. |
| `make generate` | Run the data generator that injects the challenges documented here. |
| `make build-dbt` | Run `dbt build` (the dbt path is the canonical local-parity quarantine for `stg_bad_snapshots`). |
| `make up-lakehouse` | Start the lakehouse profile (Spark reads Bronze from MinIO for the batch-side challenge handling). |
| `make up-streaming` | Start the streaming profile (Flink applies the streaming-side challenge handling). |
