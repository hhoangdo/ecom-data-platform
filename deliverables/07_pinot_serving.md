# Pinot Serving

## Purpose

Apache Pinot is the realtime OLAP serving layer for the Vina Bim Shop platform. It ingests Flink-derived Kafka topics and exposes low-latency query results for live dashboards, alert review, and streaming reconciliation checks.

Pinot is fresh and provisional. Spark Gold through Trino is canonical. This boundary is central to the serving design: Pinot helps operators see what is happening now, while Trino over Spark-written Gold tables remains the official source for reconciled KPIs.

## Why Pinot Is Needed

The lakehouse already stores the durable truth, but it is not the best surface for every question. Operators often need fast answers from a small realtime slice of the platform, such as:

| Question | Best serving path |
| --- | --- |
| What happened in the last few minutes? | Pinot over Flink-derived realtime topics. |
| Which operational alerts are active? | Pinot over `realtime_ops_alerts`. |
| Did a late event change a live metric? | Pinot over `realtime_metric_corrections`. |
| What is the official revenue value for a reporting period? | Trino over Spark Gold. |
| What should be packaged for offline evidence review? | DuckDB Executive Mart exported from Trino Gold. |

Using only the lakehouse would make the live dashboard dependent on batch refresh cadence and heavier SQL scans. Pinot gives the speed layer a dedicated store with realtime ingestion and fast aggregation.

## Why Pinot Instead Of ClickHouse For This Slice

ClickHouse is also a strong analytical database, but Pinot matches the constraints of this coursework slice especially well:

| Need | Pinot fit |
| --- | --- |
| Native Kafka-derived realtime ingestion | Pinot realtime tables are configured directly around Kafka topics produced by Flink. |
| Clear local serving architecture | Controller, broker, server, and Zookeeper services are easy to map to the serving profile. |
| Dashboard-style aggregations | Pinot is designed for low-latency OLAP over append-heavy event streams. |
| Separate provisional serving truth | Pinot can sit clearly beside Trino without becoming a second canonical lakehouse. |
| Evidence-oriented contracts | Schemas, table configs, health checks, row counts, and query outputs are all committed as inspectable assets. |

The choice is not a claim that ClickHouse would be unsuitable in general. It is a scoped design decision: for this local Kafka-to-Flink-to-serving path, Pinot provides a clean realtime OLAP demonstration without changing the canonical lakehouse design.

## Implemented Serving Tables

Pinot consumes only derived topics. Do not ingest raw source topics directly into Pinot for v1.

| Kafka topic | Pinot table | Grain | Role |
| --- | --- | --- | --- |
| `realtime_commerce_metrics_1m` | `pinot_realtime_commerce_metrics_1m` | One row per minute and commerce metric key | Live commerce dashboard metrics. |
| `realtime_ops_alerts` | `pinot_realtime_ops_alerts` | One alert row per normalized operational signal | Operational alert review. |
| `realtime_metric_corrections` | `pinot_realtime_metric_corrections` | One correction snapshot per affected metric key | Late-data correction support for dashboard and reconciliation queries. |

Key implementation assets:

| Asset | Path |
| --- | --- |
| Pinot schemas | `infra/pinot/schemas/` |
| Pinot realtime table configs | `infra/pinot/tables/` |
| Dashboard query contract | `infra/pinot/sql/dashboard_pinot.sql` |
| Pinot reconciliation query contract | `infra/pinot/sql/reconciliation_pinot.sql` |
| Trino reconciliation query contract | `infra/pinot/sql/reconciliation_trino.sql` |
| Bootstrap script | `scripts/pinot/bootstrap.py` |
| Query examples | `scripts/pinot/query_examples.py` |
| Evidence refresh | `scripts/pinot/refresh_evidence.py` |

## Correction Handling

Flink may emit an initial one-minute metric and then emit a correction snapshot if a late event changes that same metric key. Pinot keeps these streams queryable, but the query contract must avoid double-counting.

The intended logic is:

1. Read the base rows from `pinot_realtime_commerce_metrics_1m`.
2. Read correction snapshots from `pinot_realtime_metric_corrections`.
3. For a metric key with a correction, use the latest correction snapshot as the serving value.
4. Compare the realtime result to Trino reconciliation output when evidence is captured.

This is why the correction table is an internal support table rather than a separate business KPI surface.

## Service Interactions

| Service | Relationship |
| --- | --- |
| Kafka | Pinot reads the derived topics emitted by Flink. |
| Flink | Flink owns event-time processing, late-arrival correction logic, and alert normalization before Pinot ingestion. |
| Trino | Trino provides the canonical comparison point for reconciliation SQL. |
| Spark/Iceberg | Spark writes the Gold tables that Trino queries as the canonical batch truth. |
| Airflow | Airflow can bootstrap Pinot and run reconciliation/reporting tasks, but it does not own the long-running stream processing jobs. |
| DataHub | Pinot table metadata and lineage are represented in governance evidence through the upstream derived-topic and serving relationships. |

## Runtime Profile

Start Pinot after ingestion, lakehouse, and streaming services are available:

```powershell
docker compose --profile ingestion up -d
docker compose --profile lakehouse up -d
docker compose --profile ingestion --profile lakehouse --profile streaming up -d
docker compose --profile ingestion --profile lakehouse --profile streaming --profile serving up -d
```

The `serving` profile starts:

| Service | Responsibility |
| --- | --- |
| `pinot-zookeeper` | Coordinates Pinot cluster metadata. |
| `pinot-controller` | Manages schemas, tables, segments, and the controller UI. |
| `pinot-broker` | Serves SQL queries. |
| `pinot-server` | Stores and scans realtime segments. |

Local URLs:

| Service | URL |
| --- | --- |
| Pinot controller UI | `http://localhost:9003` |
| Pinot broker query API | `http://localhost:8000` |

Apply the committed schemas and tables:

```powershell
uv run python scripts/pinot/bootstrap.py
```

Run query examples:

```powershell
uv run python scripts/pinot/query_examples.py
```

Refresh the full serving evidence package:

```powershell
uv run python scripts/pinot/refresh_evidence.py
```

## Evidence

Committed evidence is stored under `evidence/07_pinot_serving/`.

| Artifact | Purpose |
| --- | --- |
| `controller_health.json` and `broker_health.json` | Prove the serving services were reachable. |
| `table_inventory.json` and `table_status.json` | Record configured Pinot tables and ingestion state. |
| `consuming_segments.json` | Shows realtime segment activity. |
| `row_counts.json` | Documents ingested row counts by table. |
| `query_outputs/pinot_dashboard_results.json` | Captures dashboard query output. |
| `query_outputs/pinot_reconciliation_results.json` | Captures Pinot-side reconciliation output. |
| `query_outputs/trino_reconciliation_results.json` | Captures canonical comparison output from Trino. |
| `query_outputs/reconciliation_report.md` | Summarizes the realtime-vs-canonical comparison. |
| `refresh_evidence_manifest.json` and `run_manifest.json` | Record evidence generation context. |
| `screenshots/` | Historical UI inspection evidence; official refresh commands generate machine-verifiable JSON/query artifacts. |

The clean-room verification path can be run with:

```powershell
uv run python scripts/flink/cleanroom_verify.py --phase all --include-pinot
```

That flow proves the Kafka to Flink to derived-topic to Pinot path using runtime scratch evidence, while keeping committed lakehouse and final dataset artifacts intact.

## Limitations

- Pinot is not the official financial reporting surface.
- Pinot query logic must respect correction snapshots to avoid double-counting late-event updates.
- Pinot cluster state is operational runtime state, not committed coursework data.
- The serving profile is intended for staged startup after ingestion and streaming dependencies are healthy.

## Convenience Make Targets

The root [Makefile](../Makefile) wraps the most common commands for this deliverable under a `make + verb` convention. It delegates to the official scripts in [scripts/](../scripts/) and to `scripts/ctl.py` for Docker Compose lifecycle; compose targets expand documented profile bundles and prebuild shared images where needed.

| Target | What it does |
| --- | --- |
| `make up-serving` | Start the serving profile (Pinot Zookeeper, controller, broker, server). |
| `make down-serving` | Stop the serving profile and remove its volumes. |
| `make up-streaming` | Start the streaming profile (Flink produces the derived topics Pinot consumes). |
| `make down-streaming` | Stop the streaming profile and remove its volumes. |
| `make up-ingestion` | Start the ingestion profile (Kafka source for the Flink-to-Pinot path). |
| `make down-ingestion` | Stop the ingestion profile and remove its volumes. |
