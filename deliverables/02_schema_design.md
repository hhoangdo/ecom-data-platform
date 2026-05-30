# 02 Schema Design

## Status

Draft placeholder for the final coursework submission document.

## Current Source of Truth

- `architecture/masterplan.md`
- `architecture/decisions/2026-05-30-consumption-serving-planes.md`
- `architecture/domain/source-to-target-mapping.md`

## Intended Final Coverage

- Bronze, Silver, and Gold schema design
- data quality checks and SLA targets
- update policy, backfill policy, and point-in-time rules
- serving tables, features, and evidence references
- Apache Pinot realtime serving table design for Flink-derived operational metrics and alerts
- Flink-to-Pinot serving outputs, including their metric grain, freshness target, and late-event behavior
- DuckDB executive mart generation from Gold tables for local hourly KPI consumption
- reconciliation rules that identify Trino-served Gold tables as the canonical KPI source when Pinot live metrics or DuckDB exports differ

Section `02` will define these implementation details later. This placeholder intentionally does not include Pinot schema JSON, DuckDB DDL, container setup, or runnable pipeline scripts.
