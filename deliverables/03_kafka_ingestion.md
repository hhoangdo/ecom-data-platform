# 03 Kafka Ingestion

## Purpose

Kafka is the event ingestion layer for `vina-bim-shop`. In this project it acts as the durable event log between the synthetic source system and the realtime/lakehouse platform.

Kafka owns three responsibilities:

| Responsibility | Project implementation |
| --- | --- |
| Event transport | Source events are published to Kafka topics such as `commerce_events`, `catalog_events`, `fulfillment_events`, and `ops_events`. |
| Contract enforcement | Schema Registry stores JSON Schema subjects for source and DLQ topics. |
| Replayable Bronze landing | Kafka Connect writes source-topic events into MinIO Bronze so Spark can process historical event logs. |

The local broker runs as single-node Kafka KRaft. ZooKeeper is not used for Kafka because KRaft provides the broker/controller metadata path in modern Kafka.

## Why Kafka Is Needed

Without Kafka, the project would only have batch files. That would miss the main realtime problems an e-commerce platform faces:

| Pain point | How Kafka helps |
| --- | --- |
| Live actions happen continuously | Kafka receives events as an ordered stream instead of waiting for batch snapshots. |
| Multiple services need the same events | Flink, Kafka Connect, DataHub, and smoke consumers can read from topics independently. |
| Realtime data must be replayable | Topics and Bronze event logs allow consumers to recover or reprocess windows. |
| Event contracts must be inspectable | Schema Registry records the expected JSON shape for each topic. |
| Bad records must be visible | `dead_letter_events` provides a deliberate DLQ contract instead of hiding malformed payloads. |

Kafka therefore bridges the generator and the rest of the platform. It is not the canonical historical source; Spark Gold through Trino remains canonical for reconciled reporting.

## Services

Start the ingestion profile:

```powershell
docker compose --profile ingestion up -d
```

| Service | Local URL | Role |
| --- | --- | --- |
| Kafka broker | `localhost:9092` | Single-node KRaft event broker. |
| Schema Registry | `http://localhost:8081` | Stores JSON Schema subjects for topic values. |
| Kafka Connect | `http://localhost:8083` | Runs the S3 sink that lands events into MinIO Bronze. |
| Kafka UI | `http://localhost:8084` | Lets reviewers inspect topics, partitions, and messages. |

The root Compose entry point is [docker-compose.yml](../docker-compose.yml), which includes the ingestion services from [compose/ingestion.kafka.yml](../compose/ingestion.kafka.yml). Topic config lives in [infra/kafka/topics.yaml](../infra/kafka/topics.yaml).

## Topic Design

| Topic | Role | Producer | Consumers |
| --- | --- | --- | --- |
| `commerce_events` | Customer session, browse, cart, checkout, order, and payment events. | Generator or Kafka smoke publisher. | Flink commerce metrics, Kafka Connect, Spark event Silver, DataHub. |
| `catalog_events` | Product, price, inventory, and promotion source changes. | Generator or Kafka smoke publisher. | Flink ops alerts, Kafka Connect, Spark event Silver, DataHub. |
| `fulfillment_events` | Shipment lifecycle events. | Generator or Kafka smoke publisher. | Flink ops alerts, Kafka Connect, Spark event Silver, DataHub. |
| `ops_events` | Source observability events for bursts, lateness, duplicates, and schema changes. | Generator or Kafka smoke publisher. | Flink alert path, Kafka Connect, Spark event Silver, DataHub. |
| `dead_letter_events` | Wrappers around malformed event examples. | Generator and validation paths. | Bronze quarantine, dbt/Spark inspection, DataHub. |
| `realtime_commerce_metrics_1m` | Derived one-minute metrics. | Flink. | Pinot realtime table. |
| `realtime_ops_alerts` | Derived operational alerts. | Flink. | Pinot realtime table and audit output. |
| `realtime_metric_corrections` | Derived correction snapshots for late events. | Flink. | Pinot support table and audit output. |

All topics are local coursework topics with one partition and replication factor one. That keeps the project reproducible on one machine while preserving the conceptual boundaries of a production Kafka platform.

Bootstrap topics:

```powershell
uv run python scripts/kafka/bootstrap_topics.py
```

## Schema Registry

Schema Registry is used so event payloads are not just arbitrary JSON files. It stores the value schemas for each source topic:

| Subject | Topic |
| --- | --- |
| `commerce_events-value` | `commerce_events` |
| `catalog_events-value` | `catalog_events` |
| `fulfillment_events-value` | `fulfillment_events` |
| `ops_events-value` | `ops_events` |
| `dead_letter_events-value` | `dead_letter_events` |

Register schemas:

```powershell
uv run python scripts/kafka/register_schemas.py
```

The four normal topics share the common event envelope: `event_id`, `event_type`, `event_topic`, `schema_version`, `event_timestamp`, `created_ts`, `producer`, `correlation_ids`, and `payload`. The DLQ topic uses `dlq_id`, `source_topic`, `error_reason`, `raw_payload`, `event_topic`, `schema_version`, and `ingest_ts`.

## Kafka Connect To MinIO Bronze

Kafka Connect turns the live event log into replayable lakehouse storage. The project uses a custom pinned Kafka Connect image so the Confluent S3 sink plugin is available deterministically:

- image: `vina-bim-shop/kafka-connect:7.8.3-s3`
- Dockerfile: [infra/kafka/connect/Dockerfile](../infra/kafka/connect/Dockerfile)
- connector template: [infra/kafka/connect/source-events-s3-sink.template.json](../infra/kafka/connect/source-events-s3-sink.template.json)

Register the Bronze event sink after the lakehouse profile is running:

```powershell
uv run python scripts/kafka/register_bronze_sink.py --connector-name bronze-events-s3-sink --bronze-bucket bronze --minio-endpoint http://minio:9000 --minio-region us-east-1 --minio-access-key vina_minio --minio-secret-key vina_minio_password
```

Supported event object layout:

```text
bronze/events/<topic>/ingest_date=<date>/*
```

This direct topic-segment layout is the live contract. The older `bronze/events/topic=<topic>/...` shape is not supported.

## End-To-End Data Flow

```text
Generator JSONL
  -> Kafka publisher / smoke publisher
  -> Kafka source topics
  -> Schema Registry validation and Kafka UI inspection
  -> Kafka Connect S3 sink
  -> MinIO Bronze event logs
  -> Spark Silver event tables

Kafka source topics
  -> Flink event-time jobs
  -> derived Kafka topics
  -> Pinot realtime serving

Kafka topic metadata
  -> DataHub ingestion
```

## Evidence And Verification

Smoke publish:

```powershell
uv run python scripts/kafka/producer_smoke.py
```

Smoke consume:

```powershell
uv run python scripts/kafka/consumer_smoke.py
```

Capture evidence:

```powershell
uv run python scripts/kafka/capture_evidence.py
```

Key evidence files:

- [topic_list.txt](../evidence/03_kafka_ingestion/topic_list.txt)
- [topic_descriptions.txt](../evidence/03_kafka_ingestion/topic_descriptions.txt)
- [schema_registry_subjects.json](../evidence/03_kafka_ingestion/schema_registry_subjects.json)
- [producer_smoke_summary.json](../evidence/03_kafka_ingestion/producer_smoke_summary.json)
- [consumer_smoke_summary.json](../evidence/03_kafka_ingestion/consumer_smoke_summary.json)
- [kafka_connect_status.json](../evidence/03_kafka_ingestion/kafka_connect_status.json)
- [kafka_connect_bronze_sink_response.json](../evidence/03_kafka_ingestion/kafka_connect_bronze_sink_response.json)
- [run_manifest.json](../evidence/03_kafka_ingestion/run_manifest.json)

## Service Interactions

| Service | Relationship |
| --- | --- |
| Generator | Produces Kafka-shaped event payloads and DLQ examples. |
| Schema Registry | Stores topic value schemas used by smoke validation and governance evidence. |
| Kafka Connect | Reads source topics and lands Bronze event logs into MinIO. |
| MinIO lakehouse | Receives replayable event logs under the Bronze bucket. |
| Flink | Consumes source topics and writes derived realtime topics. |
| Pinot | Consumes derived topics, not raw source topics. |
| Spark | Reads Bronze event logs after Kafka Connect lands them. |
| Airflow | Can trigger topic/bootstrap tasks and downstream evidence workflows. |
| DataHub | Ingests Kafka topic metadata and schema information. |

## Limitations

- The local broker is single-node and not designed for production durability.
- Topic partitioning is intentionally simple for coursework reproducibility.
- Kafka is not the official historical reporting surface; it is the event log and ingestion backbone.
- Kafka Connect writes Bronze event objects, while Spark owns Silver/Gold transformation.

## Convenience Make Targets

The root [Makefile](../Makefile) wraps the most common commands for this deliverable under a `make + verb` convention. It delegates to the official scripts in [scripts/](../scripts/) and to `scripts/ctl.py` for Docker Compose lifecycle; compose targets expand documented profile bundles and prebuild shared images where needed.

| Target | What it does |
| --- | --- |
| `make up-ingestion` | Start the ingestion profile (Kafka broker, Schema Registry, Kafka Connect, Kafka UI). |
| `make down-ingestion` | Stop the ingestion profile and remove its volumes. |
| `make up-lakehouse` | Start the lakehouse profile (needed by the Bronze Kafka Connect sink). |
| `make down-lakehouse` | Stop the lakehouse profile and remove its volumes. |
