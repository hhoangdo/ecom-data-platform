# ADR 04 Flink Streaming Runbook

This runbook implements `architecture/decisions/2026-06-01-04-flink-streaming-plan.md` without changing Pinot or Airflow ownership boundaries.

## Services

Start the streaming profile alongside ingestion and lakehouse dependencies:

```powershell
docker compose --profile ingestion --profile lakehouse --profile streaming up -d
```

Local service URLs:

| Service | URL |
| --- | --- |
| Flink UI | `http://localhost:8086` |
| Kafka broker | `localhost:9092` |
| MinIO console | `http://localhost:9001` |

The `streaming` profile starts:

- `flink-jobmanager`
- `flink-taskmanager`
- `flink-job-submit`

`flink-job-submit` is a one-shot service that waits for the JobManager REST API and submits the long-running jobs only if they are not already present.

## Job Ownership

The two long-running jobs are:

- `vina-bim-shop-commerce-metrics`
- `vina-bim-shop-ops-alerts`

Ownership boundaries:

- Flink consumes raw Kafka source topics directly.
- Flink writes derived Kafka topics only.
- Flink writes checkpoints to the `checkpoints` bucket.
- Flink mirrors `realtime_metric_corrections` and `realtime_ops_alerts` JSONL audit outputs into the `evidence` bucket under `streaming_curated/`.
- Pinot consumes the derived topics later.
- Airflow does not monitor or restart Flink in v1.
- Airflow must not monitor or restart Flink in v1.

## Smoke Publish

Publish deterministic ADR 04 source fixtures:

```powershell
uv run python scripts/flink/publish_smoke.py
```

The fixture covers:

- a normal commerce window
- a duplicate commerce event
- a late commerce event
- ops burst, late-arrival, and duplicate alerts
- payment-failure spike conditions

Expected derived topics:

- `realtime_commerce_metrics_1m`
- `realtime_ops_alerts`
- `realtime_metric_corrections`

## Evidence

Capture ADR 04 evidence:

```powershell
uv run python scripts/flink/capture_evidence.py
```

Evidence is written under `evidence/06_flink_streaming/`.

Required artifacts:

- Flink overview, jobs, and taskmanager REST JSON
- derived topic sample payloads
- checkpoint and curated-output MinIO listings
- version matrix
- screenshot placeholders and run manifest

## Reset

Stop Flink services:

```powershell
docker compose --profile streaming down
```

If you need a clean end-to-end retry, stop all involved profiles and remove volumes:

```powershell
docker compose --profile ingestion --profile lakehouse --profile streaming down -v
```

Flink checkpoints are operational state, not committed coursework evidence.
