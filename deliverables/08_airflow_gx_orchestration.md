# ADR 06 Airflow + GX Orchestration Runbook

This runbook implements `architecture/decisions/2026-06-01-06-airflow-gx-orchestration-plan.md` and preserves the orchestration boundaries from ADR 00.

## Services

Start the required profiles in dependency order:

```powershell
docker compose --profile ingestion up -d
docker compose --profile lakehouse up -d
docker compose --profile batch up -d
docker compose --profile serving up -d
docker compose --profile orchestration up -d
```

Local service URLs:

| Service | URL |
| --- | --- |
| Airflow UI | `http://localhost:8082` |
| GX Data Docs | `http://localhost:8088` |
| Trino UI | `http://localhost:8080` |
| Pinot controller UI | `http://localhost:9003` |

The `orchestration` profile starts:

- `airflow-webserver`
- `airflow-scheduler`
- `airflow-init`
- `gx-docs`

Airflow reuses the shared `lakehouse-postgres` service and the pre-created `airflow` database.

## Required DAGs

- `kafka_topic_bootstrap`
- `pinot_bootstrap`
- `hourly_batch_lakehouse`
- `datahub_ingestion`
- `reconciliation_report`
- `local_evidence_build`

Manual DAGs stay paused by default. The hourly demo DAGs also stay paused by default and should be triggered intentionally for a closed logical hour.

## Boundaries

- Airflow orchestrates batch and control-plane tasks only.
- Airflow must not monitor or restart Flink in v1.
- Pinot is fresh and provisional.
- Spark Gold through Trino is canonical.
- Bronze warns and quarantines.
- Silver/Gold failures block the DAG.
- Pinot query checks warn unless reconciliation fails.

## Trigger Flow

List the installed DAGs:

```powershell
docker compose exec airflow-webserver airflow dags list
```

Trigger Kafka bootstrap manually:

```powershell
docker compose exec airflow-webserver airflow dags trigger kafka_topic_bootstrap
```

Trigger the hourly batch demo for one closed UTC hour:

```powershell
docker compose exec airflow-webserver airflow dags trigger hourly_batch_lakehouse --logical-date 2026-06-01T01:00:00+00:00
```

Trigger the reconciliation demo for the same logical hour:

```powershell
docker compose exec airflow-webserver airflow dags trigger reconciliation_report --logical-date 2026-06-01T01:00:00+00:00
```

Build the local evidence package after one or more runs:

```powershell
docker compose exec airflow-webserver airflow dags trigger local_evidence_build
```

## GX Data Docs

GX Data Docs are written to `evidence/08_airflow_gx/gx_data_docs/` and served by the lightweight `gx-docs` container.

The static page is intended for local auditability:

- validation summaries from ADR 06 runs are rendered into the docs index
- the page is safe to serve from committed local artifacts
- screenshots should be saved under `evidence/08_airflow_gx/screenshots/`

## Evidence

ADR 06 evidence is written under `evidence/08_airflow_gx/`.

Key artifacts include:

- `airflow_health.json`
- `run_manifest.json`
- `gx_data_docs/index.html`
- `screenshots/README.md`
- `runs/<dag_id>/<run_id>/run_manifest.json`
- `runs/hourly_batch_lakehouse/<run_id>/quality/*.json`
- `runs/reconciliation_report/<run_id>/query_outputs/reconciliation_report.md`

## Reset

Stop orchestration services:

```powershell
docker compose --profile orchestration down
```

Stop the full ADR 06 dependency chain when needed:

```powershell
docker compose --profile ingestion --profile lakehouse --profile batch --profile serving --profile orchestration down
```
