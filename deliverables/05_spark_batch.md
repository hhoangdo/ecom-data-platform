# ADR 03 Spark Batch Runbook

This runbook implements `architecture/decisions/2026-06-01-03-spark-batch-iceberg-plan.md` and keeps Spark as the only Silver/Gold writer.

## Services

Start the lakehouse profile first, then the batch profile:

```powershell
docker compose --profile lakehouse up -d
docker compose --profile batch up -d
```

Local service URLs:

| Service | URL |
| --- | --- |
| Spark master UI | `http://localhost:8085` |
| Spark History Server | `http://localhost:18080` |

The worker is internal-only and registers with the master automatically.

## Bronze Inputs

Spark reads already-landed Bronze raw objects from MinIO:

- Batch snapshots: `bronze/batch/<dataset>/snapshot_date=<date>/*`
- Kafka events: `bronze/events/<topic>/ingest_date=<date>/*`

The event layout intentionally uses the direct topic segment form above. `bronze/events/topic=<topic>/...` is not the supported local sink output.

## Batch Interface

Run one logical hourly window:

```powershell
uv run python scripts/spark/run_batch.py --start-ts 2026-06-01T00:00:00Z --end-ts 2026-06-01T01:00:00Z --mode hourly
```

Run an arbitrary backfill window:

```powershell
uv run python scripts/spark/run_batch.py --start-ts 2026-06-01T00:00:00Z --end-ts 2026-06-03T00:00:00Z --mode backfill
```

The interface uses half-open UTC windows: `[start_ts, end_ts)`. `hourly` requires exactly one hour. `backfill` accepts any positive UTC range.

## What The Wrapper Does

- Submits the PySpark job through `spark-master`.
- Builds Silver Iceberg tables with idempotent `MERGE INTO`.
- Rebuilds all current Gold tables as Iceberg.
- Runs PySpark assertions.
- Runs Great Expectations checks.
- Runs Trino Gold smoke queries.
- Runs dbt-DuckDB parity checks.
- Exports the DuckDB Executive Mart from Trino Gold into `data/gold/vina_bim_shop_executive.duckdb`.
- Captures Spark master and History Server evidence.

## Evidence

ADR 03 evidence is written under `evidence/05_spark_batch/`.

Expected artifacts include:

- `spark_master_status.json`
- `spark_history_applications.json`
- `spark_job_manifest.json`
- `spark_table_row_counts.json`
- `pyspark_validation_report.json`
- `gx/validation_results.json`
- `trino_gold_smoke_results.json`
- `dbt_parity_report.json`
- `dbt_parity_report.md`
- `executive_mart_export_manifest.json`
- `executive_mart_export_report.md`
- `run_manifest.json`
- `screenshots/spark_master_ui.png`
- `screenshots/spark_history_server.png`

## DuckDB Outputs

| Artifact | Built by | Purpose |
| --- | --- | --- |
| `data/gold/vina_bim_shop.duckdb` | `dbt build` | dbt-DuckDB parity oracle for data engineering regression checks. |
| `data/gold/vina_bim_shop_executive.duckdb` | `scripts/spark/export_executive_mart.py` through Trino | DuckDB Executive Mart, a Trino Gold snapshot export for local/offline executive analysis. |

The executive mart is not a second source of truth. Spark/Iceberg/Trino remains canonical; the DuckDB file is a regenerated local snapshot.

## Reset

Stop the batch services:

```powershell
docker compose --profile batch down
```

Stop lakehouse services as well when the full stack is no longer needed:

```powershell
docker compose --profile lakehouse down
```
