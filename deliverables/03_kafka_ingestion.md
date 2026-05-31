# ADR 01 Kafka KRaft Ingestion Runbook

This runbook implements `architecture/decisions/2026-06-01-01-kafka-kraft-ingestion-plan.md` without changing later-session ownership.

## Services

Start the ingestion profile:

```powershell
docker compose --profile ingestion up -d
```

Local service URLs:

| Service | URL |
| --- | --- |
| Kafka broker | `localhost:9092` |
| Schema Registry | `http://localhost:8081` |
| Kafka Connect | `http://localhost:8083` |
| Kafka UI | `http://localhost:8084` |

The broker is a single-node Kafka KRaft service. There is no ZooKeeper and no multi-broker local cluster.

## Bootstrap

Create all source topics and derived-placeholder topics idempotently:

```powershell
uv run python scripts/kafka/bootstrap_topics.py
```

Source topics:

- `commerce_events`
- `catalog_events`
- `fulfillment_events`
- `ops_events`
- `dead_letter_events`

Derived-placeholder topics created for later Flink/Pinot sessions:

- `realtime_commerce_metrics_1m`
- `realtime_ops_alerts`
- `realtime_metric_corrections`

## Schemas

Register JSON Schema subjects in Schema Registry:

```powershell
uv run python scripts/kafka/register_schemas.py
```

Subjects:

- `commerce_events-value`
- `catalog_events-value`
- `fulfillment_events-value`
- `ops_events-value`
- `dead_letter_events-value`

The four normal source topics use the common JSON envelope fields. `dead_letter_events` uses its current DLQ shape with `dlq_id`, `source_topic`, `error_reason`, `raw_payload`, `event_topic`, `schema_version`, and `ingest_ts`.

## Smoke Tests

Publish deterministic smoke events while still writing local JSONL evidence:

```powershell
uv run python scripts/kafka/producer_smoke.py
```

Read at least one message per source topic and validate the JSON Schema contracts:

```powershell
uv run python scripts/kafka/consumer_smoke.py
```

The consumer smoke default timeout is 120 seconds to allow local Docker Kafka group assignment and readback to settle on slower machines.

Capture service evidence:

```powershell
uv run python scripts/kafka/capture_evidence.py
```

Evidence is written under `evidence/03_kafka_ingestion/`.

## Kafka Connect

Kafka Connect runs in ADR 01 so its REST endpoint and UI integration are visible. The future S3 sink template is `infra/kafka/connect/source-events-s3-sink.template.json`, but it is not posted until ADR 02 provides MinIO.

## Reset

Reset topics and recreate them:

```powershell
uv run python scripts/kafka/cleanup_kafka.py
```

Reset topics and remove ADR 01 evidence:

```powershell
uv run python scripts/kafka/cleanup_kafka.py --clean-evidence
```

Fully drop the local Kafka data volume if needed:

```powershell
docker compose --profile ingestion down -v
```

Kafka data is operational state, not committed coursework evidence.
