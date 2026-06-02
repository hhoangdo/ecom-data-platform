# ADR 08: Final Integration Evidence — Handoff Document

> For: Next OpenCode session (fresh context window)  
> Date: 2026-06-02  
> Git HEAD: 916471d (all changes committed, working tree clean)

---

## 1. WHAT WAS ACCOMPLISHED (committed)

### ADR 08 Document Updates (Session 1)
- `README.md` — contract-only narrative replaced with staged runnable platform guide
- `architecture/diagrams/lambda_architecture.puml` — added Airflow, GX, DataHub, Evidence components
- `architecture/diagrams/schema_design.puml` — contracts → runnable; profile references added
- `architecture/diagrams/physical_gold_model.puml` — dual-runnability note (dbt-DuckDB + Spark/Iceberg/Trino)
- `architecture/masterplan.md` — Phase 2 (ADRs 01-08) + new §8.5 Distributed Platform section
- `deliverables/02_schema_design.md` — dbt role → "compatibility/parity harness"; Pinot → "runnable"
- `architecture/decisions/2026-06-01-08-final-integration-evidence-plan.md` — status → Implemented

### New Scripts
- `scripts/qa/reset_all.py` — safe project reset wrapper
  - `--dry-run` — print what would be destroyed, exit
  - `--force` — skip confirmation prompt
  - `--profile ingestion --profile lakehouse` — selective reset
  - `--clean-local-data` — also remove git-ignored paths
- `scripts/qa/upload_bronze.sh` — MinIO upload with correct date-partitioned paths

### Spark Resource Limits (Session 2)
- `infra/spark/bin/entrypoint.sh` — worker block reads SPARK_WORKER_CORES and SPARK_WORKER_MEMORY env vars, passes them as `--cores` and `--memory` flags to Spark Worker. Backward-compatible: defaults to auto-detect if vars not set.
- `docker-compose.yml` — spark-worker service gets `SPARK_WORKER_CORES: "2"` and `SPARK_WORKER_MEMORY: "2g"`
- **VERIFIED in Spark Master UI:** "Cores: 2 Total, Memory: 2.0 GiB Total" (was 16 cores before the fix)

### Evidence Files Written
- `evidence/final_integration/service_health.json` — 24 services healthy across 7 profiles
- `evidence/final_integration/generator_run_summary.json` — medium scale generator stats
- `evidence/final_integration/reconciliation_report.md` — Pinot vs Gold truth policy (Pinot provisional, Gold canonical)
- `evidence/final_integration/datahub_lineage.md` — DataHub governance setup, ingestion recipes, lineage path
- `evidence/final_integration/row_counts.csv` — Bronze→Silver→Gold→dbt parity row counts (47 rows)
- `evidence/final_integration/final_manifest.json` — master artifact inventory

### What Was Proven Working
- All 7 Docker profiles start successfully (24 services)
- Kafka: 5 JSON Schemas registered (commerce_events-value, catalog_events-value, fulfillment_events-value, ops_events-value, dead_letter_events-value), 8 topics created
- Generator: medium scale — 639K events, 45K orders, 168K order items, 12K customers
- MinIO: 343 MB uploaded with correct date partitioning (batch/{dataset}/snapshot_date={date}/ and events/{topic}/ingest_date={date}/)
- DataHub: GMS + frontend + actions + OpenSearch all healthy (profile `governance`)
- dbt-DuckDB parity: 22 tables match Spark Gold row counts (from June 1 run, committed at `evidence/05_spark_batch/dbt_parity_report.md`)
- Spark worker: limited to 2 cores, 2 GB RAM — confirmed in Spark Master UI snapshot

---

## 2. ARCHITECTURE INVARIANTS (DO NOT CHANGE)

These are locked decisions from ADR 00. Every action must respect them.

| # | Area | Decision |
|---|------|----------|
| 1 | Compose strategy | Root compose with profiles. Staged profiles = normal workflow. |
| 2 | `all` profile | Best-effort, high-resource. NOT the only accepted path. |
| 3 | dbt-DuckDB | Preserved as compatibility/parity oracle. Do not remove. |
| 4 | Bronze storage | Parquet snapshots + JSONL Kafka replay logs on MinIO. |
| 5 | Silver/Gold storage | Apache Iceberg on MinIO via Hive Metastore catalog. |
| 6 | Table ownership | Spark owns Silver/Gold writes. Trino reads/serves SQL. |
| 7 | Truth policy | Spark Gold via Trino = canonical. Pinot = fresh & provisional. |
| 8 | Stream serving | Flink → derived Kafka topics → Pinot ingestion. |
| 9 | Orchestration | Airflow orchestrates batch. Does NOT monitor Flink in v1. |
| 10 | Quality gates | Bronze warns. Silver/Gold failures block DAGs. Pinot/DataHub warn. |
| 11 | Governance timing | DataHub after Kafka, lakehouse, Spark, Flink, Pinot, Airflow/GX exist. |
| 12 | Diagrams | Update in place. No duplicate competing diagrams. |
| 13 | No local-only helpers | Don't document helper scripts as required project deps. |

---

## 3. THE CENTRAL PROBLEM: DOCKER DESKTOP DEGRADATION

### Symptoms (observed across both sessions)

After ~30 minutes with 20+ containers running, Docker API degrades:

| Command | Result |
|---------|--------|
| `docker ps` | ✅ Works |
| `docker compose up -d` | ✅ Works |
| `docker exec` | ❌ 500 Internal Server Error |
| `docker logs` | ❌ 500 Internal Server Error |
| `docker stop` | ❌ 500 Internal Server Error |
| `docker compose ps` | ❌ Times out (no output) |
| `docker compose run` | ❌ Times out or EOF |
| Trino REST queries | ❌ Time out |
| Pinot/Airflow REST APIs | ❌ Time out |
| Playwright browser navigation | ❌ Time out (Session 1, after reset) |

### Root Cause

Docker Desktop on Windows (WSL2 backend) allocates a fixed-size VM. When 20+ containers run simultaneously, the VM's internal API server exhausts resources (file descriptors/memory), causing 500 errors on container interaction APIs. Simple list operations continue working because they're cached or use lighter API paths.

It is NOT an API version mismatch — both Docker client and server are v29.5.2.

### The Fix: Staged Pipeline Execution

Instead of starting all 7 profiles simultaneously (24 containers → Docker dies after 30 min), start ONLY the services needed for each pipeline stage, run that stage, then STOP before starting the next.

```
┌──────┬─────────────────────────────────┬───────────┬─────────────┐
│Stage │ Services started                │Containers │ Duration    │
├──────┼─────────────────────────────────┼───────────┼─────────────┤
│  1   │ ingestion + lakehouse           │    10     │  ~10 min    │
│      │ (Kafka, MinIO, HMS, Trino, PG)  │           │             │
│  2   │ lakehouse + batch               │     8     │  ~15 min    │
│      │ (+ Spark master, worker, hist)  │           │             │
│  3   │ ingestion + streaming           │     7     │  ~8 min     │
│      │ (+ Flink JM, TM)                │           │             │
│  4   │ ingestion + serving             │     8     │  ~10 min    │
│      │ (+ Pinot ZK, ctrl, broker, srv) │           │             │
│  5   │ lakehouse + orchestration       │     7     │  ~8 min     │
│      │ (+ Airflow, GX docs)            │           │             │
│  6   │ ingestion + lakehouse + gov     │    12     │  ~10 min    │
│      │ (+ DataHub, OpenSearch)         │           │             │
│  7   │ Final evidence (no Docker)      │     0     │  ~5 min     │
├──────┼─────────────────────────────────┼───────────┼─────────────┤
│      │ TOTAL (max 12 containers)       │           │  ~66 min    │
└──────┴─────────────────────────────────┴───────────┴─────────────┘
```

Stop command between stages: `docker compose --profile <a> --profile <b> stop`

---

## 4. SCREENSHOT STATUS

### Current State
11 screenshots were captured via Playwright MCP (Docker-based) during Session 2. They are trapped inside the Playwright MCP Docker container at `/home/node/*.png`. They are NOT on the host filesystem at `evidence/final_integration/ui_screenshots/`.

### Fix
You installed Playwright MCP natively (not via Docker). The next session should use the native server. This means:
- `browser_navigate` works directly to `localhost` (not `host.docker.internal`)
- `browser_take_screenshot` saves directly to the host filesystem
- No Docker container networking issues

### Screenshot Verification Contract

For EVERY screenshot:
1. `browser_navigate` to URL
2. `browser_wait_for` specific text (confirms page fully loaded, not loading spinner)
3. `browser_snapshot` → read the text output, confirm real content (not empty/error)
4. `browser_take_screenshot` → save to `evidence/final_integration/ui_screenshots/`
5. Report: "Screenshot X verified — shows [specific content]"

### Required Screenshots (12 total)

| # | Service | URL | Wait-for text | When |
|---|---------|-----|---------------|------|
| 1 | Kafka UI | :8084 | "vina-bim-shop-local" | Stage 1 |
| 2 | Schema Registry | :8081/subjects | JSON array of subjects | Stage 1 |
| 3 | Kafka Connect | :8083/connectors | "[]" | Stage 1 |
| 4 | MinIO Console | :9001 | "Buckets" | Stage 1 |
| 5 | Trino UI | :8080 | "Cluster Overview" | Stage 1 |
| 6 | Spark Master | :8085 | "Workers" + "2 Total" (verify 2 cores!) | Stage 2 |
| 7 | Spark History | :18080 | "Completed Applications" | Stage 2 (after Spark job) |
| 8 | Flink UI | :8086 | "Running Jobs" | Stage 3 |
| 9 | Pinot UI | :9003 | "Cluster" or table list | Stage 4 |
| 10 | Airflow UI | :8082 | Login airflow/airflow → "DAGs" | Stage 5 |
| 11 | GX Data Docs | :8088 | "Validation" or content | Stage 5 |
| 12 | DataHub UI | :9002 | "Search" or login page | Stage 6 |

---

## 5. RESOURCE CONFIGURATION (committed at HEAD)

### Spark Worker (docker-compose.yml:283-305)
```yaml
spark-worker:
  image: vina-bim-shop/spark:4.0.0-iceberg-1.10.1
  profiles: ["batch", "all"]
  depends_on:
    spark-master:
      condition: service_healthy
  command: ["worker"]
  environment:
    SPARK_WORKER_CORES: "2"
    SPARK_WORKER_MEMORY: "2g"
    # ... other existing env vars follow
```

### Spark Entrypoint (infra/spark/bin/entrypoint.sh:61-71)
```bash
  worker)
    CORES="${SPARK_WORKER_CORES:-}"
    MEMORY="${SPARK_WORKER_MEMORY:-}"
    CORES_ARG=""
    MEMORY_ARG=""
    [ -n "${CORES}" ] && CORES_ARG="--cores ${CORES}"
    [ -n "${MEMORY}" ] && MEMORY_ARG="--memory ${MEMORY}"
    exec "${SPARK_HOME}/bin/spark-class" org.apache.spark.deploy.worker.Worker \
      --webui-port 8081 \
      ${CORES_ARG} ${MEMORY_ARG} \
      spark://spark-master:7077
    ;;
```

### WSL2 Memory Limit (recommended)
Create/edit `%USERPROFILE%\.wslconfig`:
```
[wsl2]
memory=10GB
```
Restart WSL after: `wsl --shutdown` then restart Docker Desktop.

---

## 6. DOCKER PROFILE DEPENDENCY MAP

```
ingestion:     kafka, schema-registry, kafka-connect, kafka-ui
               ↓ (needed by)
               streaming (Flink needs Kafka)
               serving (Pinot needs Kafka)

lakehouse:     minio, minio-init, lakehouse-postgres,
               hive-metastore-init, hive-metastore, trino
               ↓ (needed by)
               batch (Spark needs HMS + MinIO)
               orchestration (Airflow needs Postgres)
               governance (DataHub needs Postgres + Kafka)

batch:         spark-master, spark-worker, spark-history-server
               (also includes minio + lakehouse services)

streaming:     flink-jobmanager, flink-taskmanager, flink-job-submit
               (needs kafka from ingestion + minio from lakehouse)

serving:       pinot-zookeeper, pinot-controller, pinot-broker, pinot-server
               (needs kafka from ingestion)

orchestration: airflow-webserver, airflow-scheduler, airflow-init, gx-docs
               (needs postgres from lakehouse + kafka for DataHub conn)

governance:    datahub-opensearch, datahub-system-update, datahub-gms,
               datahub-frontend, datahub-actions
               (needs postgres from lakehouse + kafka from ingestion)
```

### Staged Startup Commands

```powershell
# Stage 1
docker compose --profile ingestion --profile lakehouse up -d

# Stage 2 (lakehouse already running)
docker compose --profile batch up -d

# Stage 3 (need ingestion running)
docker compose --profile ingestion --profile streaming up -d

# Stage 4 (need ingestion running)
docker compose --profile ingestion --profile serving up -d

# Stage 5 (need lakehouse running)
docker compose --profile lakehouse --profile orchestration up -d

# Stage 6 (need ingestion + lakehouse running)
docker compose --profile ingestion --profile lakehouse --profile governance up -d
```

---

## 7. SERVICE URLs AND CREDENTIALS

| # | Service | Host URL | Internal Docker URL | Auth |
|---|---------|----------|---------------------|------|
| 1 | Kafka | localhost:9092 | kafka:29092 | none |
| 2 | Schema Registry | localhost:8081 | schema-registry:8081 | none |
| 3 | Kafka Connect | localhost:8083 | kafka-connect:8083 | none |
| 4 | Kafka UI | localhost:8084 | kafka-ui:8080 | none |
| 5 | MinIO API | localhost:9000 | minio:9000 | vina_minio / vina_minio_password |
| 6 | MinIO Console | localhost:9001 | minio:9001 | vina_minio / vina_minio_password |
| 7 | Trino | localhost:8080 | trino:8080 | X-Trino-User: vina_analyst |
| 8 | Spark Master | localhost:8085 | spark-master:8080 | none |
| 9 | Spark History | localhost:18080 | spark-history-server:18080 | none |
| 10 | Flink UI | localhost:8086 | flink-jobmanager:8081 | none |
| 11 | Pinot Controller | localhost:9003 | pinot-controller:9000 | none |
| 12 | Pinot Broker | localhost:8000 | pinot-broker:8000 | none |
| 13 | Airflow UI | localhost:8082 | airflow-webserver:8080 | airflow / airflow |
| 14 | GX Data Docs | localhost:8088 | gx-docs:80 | none |
| 15 | DataHub GMS | localhost:8087 | datahub-gms:8080 | none |
| 16 | DataHub Frontend | localhost:9002 | datahub-frontend:9002 | datahub / datahub |
| 17 | Postgres | localhost:5433 | lakehouse-postgres:5432 | vina_platform / vina_platform_password |

---

## 8. KEY COMMANDS (proven to work)

### Reset
```powershell
uv run python scripts/qa/reset_all.py --force --clean-local-data
```

### Kafka Bootstrap
```powershell
uv run python scripts/kafka/register_schemas.py --registry-url http://localhost:8081
uv run python scripts/kafka/bootstrap_topics.py --bootstrap-server localhost:9092
```

### Generator (SMOKE scale — approved for final run)
```powershell
uv run python scripts/generate/run_generator.py --scale smoke --mode full --clean --seed 42
```

### MinIO Upload (with correct date partitioning)
```powershell
docker compose run --no-deps --rm --entrypoint /bin/sh `
  -v "${PWD}/data/raw:/rawdata:ro" `
  -v "${PWD}/scripts/qa/upload_bronze.sh:/upload.sh:ro" `
  minio-init /upload.sh
```

### Spark Batch (backfill, full 1-day window)
```powershell
docker compose run --no-deps --rm --entrypoint bash `
  -e AWS_ACCESS_KEY_ID=vina_minio `
  -e AWS_SECRET_ACCESS_KEY=vina_minio_password `
  spark-master -lc "cd /workspace; export PYTHONPATH=/workspace/src; `
  spark-submit --master spark://spark-master:7077 --deploy-mode client `
  --conf spark.hadoop.fs.s3a.access.key=vina_minio `
  --conf spark.hadoop.fs.s3a.secret.key=vina_minio_password `
  --conf spark.hadoop.fs.s3a.endpoint=http://minio:9000 `
  --conf spark.hadoop.fs.s3a.path.style.access=true `
  --conf spark.hadoop.fs.s3a.connection.ssl.enabled=false `
  scripts/spark/job.py `
  --start-ts 2026-05-01T00:00:00Z `
  --end-ts 2026-05-02T00:00:00Z `
  --mode backfill `
  --evidence-root /workspace/evidence/05_spark_batch"
```

### Trino Query (via REST API)
```powershell
curl.exe -s --max-time 30 -X POST "http://localhost:8080/v1/statement" `
  -H "X-Trino-User: vina_analyst" `
  -d "SELECT count(*) FROM iceberg.gold.fact_order"

# Poll nextUri from response for results
curl.exe -s --max-time 10 "<nextUri_from_first_response>"
```

### dbt Parity
```powershell
uv run dbt build --project-dir dbt --profiles-dir dbt
```

### Stop a Stage
```powershell
docker compose --profile ingestion --profile lakehouse stop
```

---

## 9. PIPELINE DATA FLOW (end-to-end)

```
                         ┌──────────────────┐
                         │   run_generator   │
                         │   --scale smoke   │
                         └────┬────────┬────┘
                              │        │
                    Parquet   │        │  JSONL
                    snapshots │        │  events
                              ▼        ▼
              ┌──────────────────┐  ┌──────────────────┐
              │   data/raw/      │  │ data/raw/        │
              │   {dataset}/     │  │ kafka_topics/    │
              │   part-000.parq  │  │ {topic}/events.  │
              └───────┬──────────┘  │ jsonl            │
                      │             └────────┬─────────┘
                      │                      │
           ┌──────────▼──────────┐  ┌────────▼──────────┐
           │ MinIO Bronze /batch │  │ Kafka topics       │
           │ snapshot_date={d}   │  │ (raw events)       │
           └──────────┬──────────┘  └────────┬───────────┘
                      │                      │
           ┌──────────▼──────────┐  ┌────────▼───────────┐
           │ Spark batch         │  │ Flink streaming     │
           │ (Iceberg Silver/    │  │ (event-time metrics)│
           │  Gold)              │  └────────┬───────────┘
           └──────────┬──────────┘           │
                      │              ┌───────▼───────────┐
           ┌──────────▼──────────┐  │ Derived Kafka      │
           │ Hive Metastore      │  │ topics (realtime_*) │
           │ (table catalog)     │  └───────┬───────────┘
           └──────────┬──────────┘          │
                      │              ┌──────▼───────────┐
           ┌──────────▼──────────┐  │ Apache Pinot      │
           │ Trino SQL           │  │ (provisional)     │
           │ (canonical truth)   │  └──────┬────────────┘
           └──────────┬──────────┘         │
                      │                    │
           ┌──────────▼────────────────────▼──────────┐
           │         Reconciliation Report              │
           │         Pinot vs Trino Gold                │
           └───────────────────────────────────────────┘
                              │
           ┌──────────────────▼──────────────────────┐
           │  Airflow → GX → DataHub → Evidence      │
           │  (orchestration, quality, governance)    │
           └─────────────────────────────────────────┘
```

---

## 10. CRITICAL PATH KNOWLEDGE (lessons learned)

| # | Lesson | Detail |
|---|--------|--------|
| 1 | **MinIO path structure** | Spark expects `s3://bronze/batch/{dataset}/snapshot_date={date}/part-000.parquet` and `s3://bronze/events/{topic}/ingest_date={date}/events.jsonl`. The upload_bronze.sh script handles this. |
| 2 | **Spark reads from MinIO (S3)** | NOT local filesystem. Must upload generator output to MinIO before running Spark. |
| 3 | **Trino catalog** | Iceberg tables: `iceberg.gold.fact_order`, `iceberg.silver.stg_orders`, etc. |
| 4 | **Flink auto-submits** | Flink jobs (commerce-metrics, ops-alerts) auto-start when streaming profile comes up. |
| 5 | **Pinot source** | Pinot ingests from Flink-derived Kafka topics: `realtime_commerce_metrics_1m`, `realtime_ops_alerts`, `realtime_metric_corrections`. |
| 6 | **Airflow DAGs** | Located at `airflow/dags/`. Auto-trigger on schedule. Auth: airflow/airflow. |
| 7 | **DataHub recipes** | Located at `infra/governance/recipes/`. Run from within the DataHub container or via CLI. |
| 8 | **Spark submit limit** | Must use `docker compose run --no-deps --entrypoint bash` (NOT `docker exec` — broken). |
| 9 | **Screenshots FIRST** | Take screenshots before any data pipeline work. Docker networking is most stable right after services start. |
| 10 | **Stop between stages** | `docker compose --profile <a> --profile <b> stop` — never leave services running between stages. |

---

## 11. WHAT REMAINS (prioritized)

### P1 — Must Complete
- [ ] Capture 12 UI screenshots with native Playwright MCP (verify each with snapshot)
- [ ] Run Spark backfill successfully (smoke scale, backfill mode)
- [ ] Verify Trino can query `iceberg.gold.fact_order` and return rows > 0
- [ ] Run `dbt build` + compare row counts to Spark Gold (parity check)
- [ ] Populate `evidence/09_datahub_governance/` with run_manifest.json + version_matrix.json

### P2 — Should Complete
- [ ] Verify Flink jobs produce derived Kafka topics (check running jobs in Flink UI)
- [ ] Pinot bootstrap + dashboard queries (verify consuming segments)
- [ ] Reconciliation: Pinot vs Trino for same time window
- [ ] Airflow DAG trigger + GX validation results
- [ ] DataHub ingestion + lineage evidence
- [ ] Write `query_outputs/trino_canonical_kpi.sql` and `.csv`
- [ ] Write `query_outputs/pinot_dashboard.sql` and `.csv`
- [ ] Update `final_manifest.json` with real timestamps from this run

### P3 — Nice to Have
- [ ] Full `all` profile health verification (optional, high-resource)
- [ ] Medium-scale data verification (smoke is sufficient per user's approval)

---

## 12. EVIDENCE DIRECTORY STRUCTURE

```
evidence/
├── 01_data_generator/          ✅ Committed
├── 02_schema_design/           ✅ Committed (from dbt-DuckDB run)
├── 03_kafka_ingestion/         ✅ Committed (schema subjects, topics, screenshots)
├── 04_lakehouse/               ✅ Committed (MinIO, Trino, HMS)
├── 05_spark_batch/             ✅ Committed (dbt parity, Trino smoke, Spark logs)
├── 06_flink_streaming/         ✅ Committed (Flink jobs, checkpoints)
├── 07_pinot_serving/           ✅ Committed (Pinot tables, reconciliation)
├── 08_airflow_gx/              ✅ Committed (Airflow health, DAGs)
├── 09_datahub_governance/      ⚠️  Only .gitkeep — needs real evidence
├── final_integration/          ✅ Committed (some files, needs update)
│   ├── service_health.json     ✅
│   ├── generator_run_summary.json ✅
│   ├── reconciliation_report.md   ✅
│   ├── datahub_lineage.md      ✅
│   ├── row_counts.csv          ✅
│   ├── final_manifest.json     ⚠️  Needs update with real timestamps
│   ├── ui_screenshots/         ⚠️  Only README.md — needs 12 PNGs
│   │   └── README.md           ✅
│   └── query_outputs/          ⚠️  Empty — needs .sql + .csv files
└── runtime/                    ❌ Git-ignored (reset logs)
```

---

## 13. SCALE DECISION

**FINAL DECISION: SMOKE scale is sufficient for the evidence package.**

The goal is to prove every service works end-to-end and document it with screenshots and query outputs. Smoke scale produces enough data (~800 customers, ~1800 orders, ~6688 order items) to demonstrate functionality without straining Docker Desktop.

Generator command:
```powershell
uv run python scripts/generate/run_generator.py --scale smoke --mode full --clean --seed 42
```

Expected smoke data volume: ~20-50 MB total, well within disk and VM budgets.

---

## 14. PLAN PROMPT

Copy-paste this into your new OpenCode session to start planning:

```
I'm continuing the ADR 08 final integration and evidence pipeline for
the vina-bim-shop coursework project.

Full context is at tmp/ADR08_HANDOFF.md — please read it first.

Key facts:
- All document updates are committed at HEAD (916471d)
- Spark resource limits (2 cores, 2 GiB) are committed
- Docker Desktop degrades after ~30 min with 20+ containers
- Fix: staged pipeline execution (max 12 containers at once)
- SMOKE scale data is approved (--scale smoke)
- Native Playwright MCP is available for screenshots

Please produce a staged execution plan that:
1. Runs in stages (start services → do work → stop)
2. Never exceeds 12 containers at once
3. Captures all 12 UI screenshots with verification
4. Completes one full data path: generator → Kafka → MinIO → Spark →
   Iceberg Gold → Trino → Flink → Pinot → Airflow/GX → DataHub
5. Writes query outputs, reconciliation, row counts
6. Runs dbt-DuckDB parity check
7. Updates final_manifest.json
8. Populates evidence/09_datahub_governance/

Start by reading tmp/ADR08_HANDOFF.md, then present the plan.
Do not edit files until I approve the plan.
```

---

## 15. EXECUTE PROMPT

Copy-paste this when ready to execute:

```
Execute the approved staged pipeline plan.

Rules:
- Use SMOKE scale (--scale smoke) for all generated data
- Start/stop Docker profiles between stages
- Max 12 containers at any time
- Screenshots: native Playwright MCP, verify every one
  (wait-for-text → snapshot → verify content → screenshot)
- Save screenshots to evidence/final_integration/ui_screenshots/
- Stop between stages: docker compose --profile <a> --profile <b> stop
- Write evidence files as we go, not all at the end
- If any stage fails, document why and continue to next stage
- Report after each stage: what worked, what didn't, evidence produced
```
