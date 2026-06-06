# Decision: Consumption Serving Planes

## Status

Accepted.

## Date

2026-05-30

## Decision

The consumption layer has two serving planes. The real-time plane uses Flink to compute event-time-correct operational metrics and publishes them to Apache Pinot for low-latency dashboard queries over recent streaming data. The reconciled analytical plane uses Spark to build hourly Gold tables in the MinIO lakehouse, served through Trino as the canonical SQL interface. For coursework portability, a DuckDB Executive Mart is exported from Trino Gold so executive KPI dashboards can run locally without a full multi-user warehouse.

## Serving Planes

| Plane | Primary consumers | Compute path | Serving interface | Freshness target | Truth role |
| --- | --- | --- | --- | --- | --- |
| Real-time operational serving | BI/livestreaming teams | Kafka source events -> Flink event-time windows and alerts | Apache Pinot realtime OLAP serving sink | under 30 seconds in the local design | Fresh operational view; may be corrected by later reconciliation |
| Reconciled analytical serving | Executive teams and historical BI | MinIO Bronze -> Spark Silver/Gold tables | Trino canonical SQL over Gold tables, plus a DuckDB Executive Mart exported from Trino Gold | within 1 hour | Reconciled business truth for KPI reporting |

## Rationale

- Apache Pinot is a good fit for recent streaming analytics because the live consumers need low-latency filtering and dashboard queries over operational metrics, alerts, and anomaly signals.
- Trino remains the canonical SQL interface because it queries the curated lakehouse tables registered through Hive Metastore.
- DuckDB is included for coursework portability: dbt-DuckDB gives a local parity oracle, while the DuckDB Executive Mart gives a file-based OLAP snapshot exported from the canonical Trino Gold path without requiring a shared warehouse service.
- The design keeps raw Section `01` source topics stable. Any `realtime_metrics` topic or Pinot table is a future derived Section `02` serving output, not a new Section `01` source contract.

## Tradeoffs

| Choice | Benefit | Cost or limitation |
| --- | --- | --- |
| Apache Pinot for live serving | Strong low-latency OLAP story for streaming dashboards | More local infrastructure than a simple metrics file or direct dashboard output |
| Trino as canonical Gold SQL | Preserves the lakehouse serving model and reconciled historical query path | Not intended for sub-second live operational dashboards |
| DuckDB executive mart | Easy local demo and coursework inspection path; exported from Trino Gold | Not the canonical multi-user executive warehouse and stale until regenerated |
| Two serving planes | Makes freshness and reconciliation responsibilities explicit | Requires clear documentation so users do not treat Pinot and DuckDB as competing sources of truth |

## Section 02 Follow-up Items

- Define the derived Flink serving outputs that feed Pinot, such as live revenue, payment issues, traffic bursts, and anomaly metrics.
- Define Pinot table names, grains, retention, time columns, and query examples for the real-time serving plane.
- Define the hourly DuckDB Executive Mart export flow from Trino Gold tables, including which executive KPI tables are emphasized for local users.
- Define reconciliation rules that explain when Pinot live metrics may differ from Gold/Trino and how Gold becomes the official KPI source.
- Add runnable local orchestration only in Section `02`; Section `01` remains source-contract-first.
