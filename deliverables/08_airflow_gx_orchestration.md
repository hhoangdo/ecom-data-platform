# Airflow And Great Expectations Orchestration

## Purpose

Apache Airflow and Great Expectations form the control-plane and quality-evidence layer for the Vina Bim Shop platform.

Airflow coordinates repeatable local workflows such as topic bootstrap, batch lakehouse processing, Pinot bootstrap, reconciliation reporting, DataHub ingestion, and evidence packaging. Great Expectations validates data quality at selected points in the platform and publishes static GX Data Docs for inspection.

Together, they make the project auditable. The goal is not only to run services, but to show what ran, what was checked, which checks block a workflow, and where the resulting evidence is stored.

## Why Airflow And GX Are Needed

A local data platform can quickly become a collection of one-off scripts. Airflow and GX solve that by giving the coursework a clear operational structure:

| Pain point | Airflow/GX response |
| --- | --- |
| Manual commands are hard to repeat in the same order. | Airflow DAGs define the intended control-plane workflows. |
| Evidence runs need logical timestamps. | Hourly demo DAGs support closed logical windows. |
| Quality failures need consistent severity. | GX checks are wrapped in policy logic that decides warning vs blocking behavior. |
| Instructors need inspectable proof. | Manifests, validation JSON, query reports, and GX Data Docs are written to evidence folders; committed screenshots are historical review artifacts. |
| Streaming should remain independent. | Airflow documents the boundary instead of supervising Flink runtime jobs. |

Airflow must not monitor or restart Flink in v1. Flink jobs are long-running streaming runtime processes; Airflow owns batch and control-plane workflow orchestration.

## Implemented DAGs

The required DAG inventory is defined in `src/vina_bim_shop/orchestration/specs.py`.

| DAG | Schedule type | Responsibility |
| --- | --- | --- |
| `kafka_topic_bootstrap` | Manual | Creates or verifies Kafka topics and ingestion prerequisites. |
| `pinot_bootstrap` | Manual | Applies Pinot schemas and realtime table configs. |
| `hourly_batch_lakehouse` | Hourly demo | Runs the staged batch lakehouse flow for a closed logical hour. |
| `reconciliation_report` | Hourly demo | Produces realtime-vs-canonical reconciliation evidence for a logical window. |
| `datahub_ingestion` | Manual | Runs governance metadata ingestion after source assets are available. |
| `local_evidence_build` | Manual | Packages local run evidence into committed evidence structure. |

Manual DAGs stay paused by default and are triggered intentionally. Hourly demo DAGs are also controlled deliberately so evidence can be tied to a known logical date.

## Quality Policy

Great Expectations checks are interpreted through the project quality policy in `src/vina_bim_shop/quality/policies.py`.

| Layer | Failure policy | Rationale |
| --- | --- | --- |
| Bronze raw | Bronze warns and quarantines | Raw landing should preserve source-fidelity and isolate bad records without hiding ingestion drift. |
| Silver | Silver/Gold failures block the DAG | Typed curated data should not continue if core cleansing rules fail. |
| Gold Trino | Silver/Gold failures block the DAG | Canonical reporting tables must satisfy stronger expectations. |
| Pinot queries | Warning unless reconciliation fails | Pinot is fresh and provisional, so query issues are surfaced but the canonical truth remains Spark Gold through Trino. |
| DataHub | Warning unless marked critical | Governance capture is evidence-supporting; critical metadata failures can still block if configured. |

This policy is intentionally asymmetric. Bronze is allowed to expose source problems; Silver and Gold are expected to protect downstream trust.

## Service Interactions

| Service | Relationship |
| --- | --- |
| Kafka | Airflow can bootstrap Kafka topics before ingestion and streaming runs. |
| MinIO/Iceberg/Hive | Airflow-triggered batch workflows read and write lakehouse data through Spark and Trino-facing tables. |
| Spark | Batch DAGs invoke Spark processing for Bronze-to-Silver-to-Gold transformation evidence. |
| Trino | GX and reconciliation tasks query canonical Gold tables through Trino. |
| Pinot | Airflow can apply Pinot configs and run Pinot query checks, but Pinot remains a provisional realtime surface. |
| Flink | Flink is intentionally outside Airflow supervision. Airflow does not restart, monitor, or own Flink stream jobs. |
| DataHub | Airflow can trigger metadata ingestion recipes after platform assets have been created. |
| GX Data Docs | Validation output is rendered into static documentation served locally for audit. |

## Runtime Profile

Start orchestration after its dependency profiles are available:

```powershell
docker compose --profile ingestion up -d
docker compose --profile lakehouse up -d
docker compose --profile batch up -d
docker compose --profile ingestion --profile lakehouse --profile streaming --profile serving up -d
docker compose --profile orchestration up -d
```

The `orchestration` profile starts:

| Service | Responsibility |
| --- | --- |
| `airflow-init` | Initializes Airflow metadata and local admin configuration. |
| `airflow-webserver` | Hosts the Airflow UI. |
| `airflow-scheduler` | Schedules and runs DAG tasks. |
| `gx-docs` | Serves static GX Data Docs from committed evidence artifacts. |

Airflow reuses the shared lakehouse Postgres service and the pre-created `airflow` database.

Local URLs:

| Service | URL |
| --- | --- |
| Airflow UI | `http://localhost:8082` |
| GX Data Docs | `http://localhost:8088` |
| Trino UI | `http://localhost:8080` |
| Pinot controller UI | `http://localhost:9003` |

## Typical Trigger Flow

List installed DAGs:

```powershell
docker compose exec airflow-webserver airflow dags list
```

Trigger Kafka bootstrap:

```powershell
docker compose exec airflow-webserver airflow dags trigger kafka_topic_bootstrap
```

Trigger an hourly batch run for one closed UTC hour:

```powershell
docker compose exec airflow-webserver airflow dags trigger hourly_batch_lakehouse --logical-date 2026-06-01T01:00:00+00:00
```

Trigger reconciliation for the same logical hour:

```powershell
docker compose exec airflow-webserver airflow dags trigger reconciliation_report --logical-date 2026-06-01T01:00:00+00:00
```

Trigger local evidence packaging:

```powershell
docker compose exec airflow-webserver airflow dags trigger local_evidence_build
```

## GX Data Docs

GX Data Docs are generated under `evidence/08_airflow_gx/gx_data_docs/` and served by the `gx-docs` container. They provide a static local audit view of validation runs and are useful when the runtime UI is not available.

The docs are supported by:

| Artifact | Role |
| --- | --- |
| Validation JSON | Machine-readable result for each check. |
| GX static HTML | Human-readable local inspection page. |
| Run manifests | Links each validation batch to DAG and logical-window context. |
| Screenshots | Optional UI evidence for coursework submission. |

## Evidence

Committed evidence is stored under `evidence/08_airflow_gx/`.

Important artifacts include:

| Artifact | Purpose |
| --- | --- |
| `airflow_health.json` | Shows local Airflow availability during capture. |
| `run_manifest.json` | Records the evidence capture context. |
| `gx_data_docs/index.html` | Static validation documentation entrypoint. |
| `runs/<dag_id>/<run_id>/run_manifest.json` | Per-run manifest for each captured DAG run. |
| `runs/hourly_batch_lakehouse/<run_id>/quality/*.json` | Quality evidence for the batch lakehouse path. |
| `runs/reconciliation_report/<run_id>/query_outputs/reconciliation_report.md` | Realtime-vs-canonical reconciliation summary. |
| `screenshots/README.md` | Historical screenshot capture notes retained with committed evidence artifacts. |

## Limitations

- Airflow is not a streaming process supervisor. Flink runtime health is verified separately through Flink evidence and clean-room checks.
- The orchestration profile is designed for staged local startup, not a single high-resource full-stack boot.
- GX validation proves selected quality policies and evidence boundaries; it is not a complete production observability system.
