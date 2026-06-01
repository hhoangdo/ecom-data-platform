# ADR 05 Pinot Serving Runbook

This runbook implements `architecture/decisions/2026-06-01-05-apache-pinot-serving-plan.md` and preserves the realtime-vs-canonical truth boundary.

## Services

Start the required profiles in dependency order:

```powershell
docker compose --profile ingestion up -d
docker compose --profile lakehouse up -d
docker compose --profile streaming up -d
docker compose --profile serving up -d
```

Local service URLs:

| Service | URL |
| --- | --- |
| Pinot controller UI | `http://localhost:9003` |
| Pinot broker query API | `http://localhost:8000` |
| Flink UI | `http://localhost:8086` |
| Trino UI | `http://localhost:8080` |

The `serving` profile starts:

- `pinot-zookeeper`
- `pinot-controller`
- `pinot-broker`
- `pinot-server`

## Table Ownership

Pinot consumes only Flink-derived Kafka topics:

- `realtime_commerce_metrics_1m` -> `pinot_realtime_commerce_metrics_1m`
- `realtime_ops_alerts` -> `pinot_realtime_ops_alerts`
- `realtime_metric_corrections` -> internal support table `pinot_realtime_metric_corrections`

Important boundaries:

- Pinot is fresh and provisional.
- Spark Gold through Trino is canonical.
- Correction handling uses the latest correction snapshot per `metric_key`.
- Do not ingest raw source topics directly into Pinot for v1.

## Bootstrap And Queries

Apply the committed Pinot schemas and realtime table configs:

```powershell
uv run python scripts/pinot/bootstrap.py
```

Publish deterministic Flink smoke fixtures if needed:

```powershell
uv run python scripts/flink/publish_smoke.py
```

Run the Pinot dashboard and reconciliation examples:

```powershell
uv run python scripts/pinot/query_examples.py
```

Key committed assets:

- Schemas: `infra/pinot/schemas/`
- Tables: `infra/pinot/tables/`
- Dashboard SQL contract: `infra/pinot/sql/dashboard_pinot.sql`
- Pinot reconciliation SQL contract: `infra/pinot/sql/reconciliation_pinot.sql`
- Trino reconciliation SQL contract: `infra/pinot/sql/reconciliation_trino.sql`

## Evidence

Capture ADR 05 evidence:

```powershell
uv run python scripts/pinot/capture_evidence.py
```

Evidence is written under `evidence/07_pinot_serving/`.

Expected artifacts include:

- `controller_health.json`
- `broker_health.json`
- `table_inventory.json`
- `table_status.json`
- `consuming_segments.json`
- `row_counts.json`
- `version_matrix.json`
- `query_outputs/pinot_dashboard_results.json`
- `query_outputs/pinot_reconciliation_results.json`
- `query_outputs/trino_reconciliation_results.json`
- `query_outputs/reconciliation_report.md`
- `run_manifest.json`
- `screenshots/pinot_tables.png`
- `screenshots/pinot_query_console.png`

## Reset

Stop Pinot services:

```powershell
docker compose --profile serving down
```

Stop the full staged runtime when you need a clean retry:

```powershell
docker compose --profile ingestion --profile lakehouse --profile streaming --profile serving down -v
```

Pinot cluster state is operational runtime state, not committed coursework evidence.
