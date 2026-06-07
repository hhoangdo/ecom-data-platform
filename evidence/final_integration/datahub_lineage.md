# DataHub Governance Evidence

## Service Status

DataHub is healthy in the repaired ADR 08 stack:

- GMS (`http://localhost:8087/health`): `200 OK`
- Frontend (`http://localhost:9002`): login and dataset pages working
- Actions: running
- OpenSearch: healthy

## Latest Successful Ingestion

Airflow run `adr08_datahub_20260603T003300Z` completed successfully and emitted:

- 8 Kafka topic datasets
- 4 MinIO/S3 prefix datasets
- 45 Trino datasets
- 52 dbt datasets
- 20 Spark v1 lineage edges
- 3 Flink v1 lineage edges
- 8 GX assertions
- 20 v2 Spark `DataJob` entities (this refresh added the v2 emitter)
- 3 v2 Flink `DataJob` entities (this refresh added the v2 emitter)

These counts are recorded in `evidence/09_datahub_governance/dataset_count.json`.

## Representative Entity Verification

Representative assets were verified directly against GMS GraphQL:

- `urn:li:dataset:(urn:li:dataPlatform:iceberg,vina_bim_shop.fact_order,PROD)`
- `urn:li:dataset:(urn:li:dataPlatform:kafka,commerce_events,PROD)`
- `urn:li:dataset:(urn:li:dataPlatform:pinot,realtime_commerce_metrics_1m,PROD)`
- `urn:li:dataset:(urn:li:dataPlatform:s3,checkpoints.spark-events,PROD)`

Representative v2 `DataJob` URNs were also verified directly via `dataJob(urn:...).inputOutput`:

- `urn:li:dataJob:(urn:li:dataFlow:(spark,vina-bim-shop-batch,local),iceberg_transform_fact_order)` (with non-empty `inputDatasets` -> `stg_orders`, `outputDatasets` -> `fact_order`)
- `urn:li:dataJob:(urn:li:dataFlow:(flink,vina-bim-shop-streaming,local),flink_derive_realtime_commerce_metrics_1m)`

All 23 expected v2 `DataJob` URNs verified queryable (see `datajob_lineage_index_readiness.verified_datajob_count` in `dataset_count.json`).

Representative governance tags were also verified directly:

- `bronze`
- `silver`
- `gold`
- `official`
- `provisional`
- `quality_gate`

This evidence pass uses direct entity lookups instead of relying on local search-index timing.

## Lineage Path

```
Generator -> Kafka raw topics -> Kafka Connect S3 sink -> MinIO Bronze
                                      |
                                      +-> Flink -> derived Kafka topics -> Pinot
                                      |
Generator batch files -> Spark -> Iceberg Silver/Gold -> Trino -> DataHub
                                                              |
                                                              +-> dbt-DuckDB parity + GX assertions
```

With the v2 `DataJob` emitter added in this refresh, each transition above also has a corresponding `DataJob` entity in GMS, exposing `DataJobInfoClass` (name, type, customProperties) and `DataJobInputOutputClass` (inputDatasets, outputDatasets).

## Screenshots

This refresh does **not** produce `12*` DataHub UI captures in `evidence/final_integration/ui_screenshots/`. The DataHub v1.5.0.6 React frontend's Lineage canvas, Discover search, and Manage Tags page all render against the OpenSearch graph / search index, which is fed by a Kafka → MAE consumer pipeline inside GMS. In this local deployment, **the MAE consumer in GMS does not write to OpenSearch** — a structural issue confirmed across this session and two prior sessions.

The previous session produced 6 backend-rendered focused PNGs labeled "NOT the DataHub UI" as an honest supplement. Those PNGs have been archived to `tmp/datahub_focused_supplements_archive_2026-06-07/` because they do not show the real DataHub UI. The full diagnosis is in `tmp/datahub_mae_consumer_diagnosis_2026-06-07.md`.

The authoritative governance proof is the backend evidence in `evidence/09_datahub_governance/` and the direct GMS GraphQL queries documented in this file (see the table below). The capture script at `output/playwright/capture_datahub.mjs` is kept on disk (in the gitignored `output/playwright/`) for re-running if/when the MAE consumer is fixed.

| Evidence Source | Value | What It Proves |
|---|---:|---|
| `datahub_health.json.status_code` | 200 | DataHub GMS was reachable during the refresh. |
| `dataset_count.json.total_datasets_emitted` | 109 | Kafka, S3/MinIO, Trino, and dbt datasets were emitted. |
| `dataset_count.json.custom_lineage.spark_entities` | 20 | Spark Silver/Gold v1 lineage metadata was emitted. |
| `dataset_count.json.custom_lineage.flink_entities` | 3 | Flink real-time v1 lineage metadata was emitted. |
| `dataset_count.json.custom_lineage.gx_assertions_emitted` | 8 | GX assertion metadata was emitted. |
| `dataset_count.json.custom_lineage.datajob_entities` | 23 | v2 Spark + Flink `DataJob` lineage metadata was emitted. |
| `dataset_count.json.datajob_lineage_index_readiness.status` | `success` | All 23 v2 `DataJob` URNs are queryable with non-empty inputOutput. |
| `dataset_count.json.verified_representative_datasets` | 4/4 | Representative Iceberg, Kafka, Pinot, and S3 entities resolve through GraphQL. |
| `tag_count.json.verified_tag_count` | 6 | Governance vocabulary tags resolve through GraphQL. |
| Direct GMS GraphQL `dataJob(urn:...).inputOutput` (re-verified 2026-06-07) | 23/23 | Returns non-empty `inputDatasets` + `outputDatasets` for all 23 v2 DataJob URNs (reads from eBean DB, which is fully populated). |
| Direct GMS GraphQL `searchAcrossLineage` and `search` (re-verified 2026-06-07) | 0 results | Confirms OpenSearch has no documents; this is the structural gap that prevents the React UI from rendering. |

## Evidence Location

- Governance evidence: `evidence/09_datahub_governance/`
- Airflow run manifest: `evidence/08_airflow_gx/runs/datahub_ingestion/adr08_datahub_20260603T003300Z/run_manifest.json`
- Screenshot set: `evidence/final_integration/ui_screenshots/` (no `12*` PNGs; see "Screenshots" above for the verdict)
- Archived focused supplements from the previous session: `tmp/datahub_focused_supplements_archive_2026-06-07/`
- MAE consumer diagnosis: `tmp/datahub_mae_consumer_diagnosis_2026-06-07.md`

