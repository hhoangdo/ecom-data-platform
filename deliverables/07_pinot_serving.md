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

## Proof Modes

There are two proof modes and they serve different purposes:

- Clean-room ingestion proof: proves Kafka -> Flink -> derived topics -> Pinot ingestion works in runtime-only scratch evidence.
- Official ADR 05 evidence refresh: regenerates the committed Pinot evidence package under `evidence/07_pinot_serving/`.

Think of a late event like a corrected receipt at a store:

- Pinot first receives the original minute-level row for a metric key.
- If a late commerce event arrives for that same minute, Flink emits a correction snapshot row.
- Dashboard and reconciliation queries must use the latest correction snapshot for that `metric_key` instead of adding both rows together.

## Bootstrap And Queries

Apply the committed Pinot schemas and realtime table configs:

```powershell
uv run python scripts/pinot/bootstrap.py
```

Publish deterministic Flink smoke fixtures if needed:

```powershell
uv run python scripts/flink/publish_smoke.py
```

For a deterministic downstream correction-path check, prefer the ADR 04 clean-room gate before bootstrapping Pinot:

```powershell
uv run python scripts/flink/cleanroom_verify.py --phase all --include-pinot
```

That sequence proves the correction row exists in Kafka and MinIO first, then resets Pinot serving state and verifies that `pinot_realtime_metric_corrections` ingests live rows.

Refresh the official ADR 05 evidence package in one pass after the clean-room proof is green:

```powershell
docker compose --profile lakehouse up -d
docker compose --profile serving up -d
uv run python scripts/pinot/refresh_evidence.py
```

That command:

- reapplies Pinot schemas and tables
- regenerates the dashboard and reconciliation query outputs
- captures the official ADR 05 health, table, row-count, screenshot, and manifest artifacts together

If you want to run the query step by itself:

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

Evidence is written under `evidence/07_pinot_serving/`. The preferred operator flow is `scripts/pinot/refresh_evidence.py`, because it keeps query outputs and evidence manifests in sync.

If Pinot services fail to restart after interrupted local runs, remove only the Pinot runtime state and retry:

```powershell
docker compose stop pinot-zookeeper pinot-controller pinot-broker pinot-server
docker compose rm -f -s pinot-zookeeper pinot-controller pinot-broker pinot-server
docker volume rm vina-bim-shop_pinot_zookeeper_data
docker compose --profile ingestion --profile lakehouse --profile serving up -d
```

That reset is limited to Pinot runtime state. It does not touch Kafka topics, MinIO lakehouse data, or committed evidence.

Expected artifacts include:

- `controller_health.json`
- `broker_health.json`
- `table_inventory.json`
- `table_status.json`
- `consuming_segments.json`
- `row_counts.json`
- `version_matrix.json`
- `refresh_evidence_manifest.json`
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
