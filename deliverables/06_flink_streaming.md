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

Default runtime safety guard:

- `VBS_FLINK_MAX_RUNTIME_MINUTES=45`
- `VBS_FLINK_MAX_RUNTIME_GRACE_SECONDS=30`
- `VBS_FLINK_DISABLE_AUTO_STOP=false`

Unless you explicitly disable it, each Flink container is wrapped with a hard runtime limit so the local stack does not run indefinitely and exhaust workstation memory or disk.

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

## Clean-Room Verification

Run the selective ADR 04 clean-room flow into ignored runtime evidence:

```powershell
uv run python scripts/flink/cleanroom_verify.py --phase all
```

Runtime-only scratch artifacts are written under `evidence/runtime/cleanroom/adr04/<timestamp>/`.

Useful phase-level commands:

```powershell
uv run python scripts/flink/cleanroom_verify.py --phase preflight
uv run python scripts/flink/cleanroom_verify.py --phase reset
uv run python scripts/flink/cleanroom_verify.py --phase verify-adr04
uv run python scripts/flink/cleanroom_verify.py --phase verify-pinot --include-pinot
uv run python scripts/flink/cleanroom_verify.py --phase cleanup
```

The clean-room flow is selective by default:

- reset Kafka topics by delete + recreate
- delete only `checkpoints/flink/`
- delete only `evidence/streaming_curated/realtime_metric_corrections/`
- delete only `evidence/streaming_curated/realtime_ops_alerts/`
- keep Bronze, Silver, Gold, committed evidence, and the final dataset zip untouched

The ADR 04 gate is explicit:

- `realtime_commerce_metrics_1m` must contain exactly `3` rows
- `realtime_metric_corrections` must contain exactly `1` row
- `realtime_ops_alerts` must contain exactly `7` rows
- the single correction row must be a full-snapshot `late_event` correction
- checkpoint and curated JSONL prefixes must both exist after replay

Pinot verification is downstream and optional in the same script. Only run it after the ADR 04 gate passes.

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

## Safe Cleanup

When local Docker storage gets tight, use the clean-room cleanup phase instead of a volume-wide prune:

```powershell
uv run python scripts/flink/cleanroom_verify.py --phase cleanup
```

That phase:

- stops only the project services used by ADR 04 and ADR 05
- removes only those stopped project containers
- runs `docker image prune -f`
- runs `docker builder prune -f --filter "until=168h"`

It does not default to `docker volume prune`, `docker system prune --volumes`, or deleting committed evidence.
