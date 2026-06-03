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
- 20 Spark lineage entities
- 3 Flink lineage entities
- 8 GX assertions

These counts are recorded in `evidence/09_datahub_governance/dataset_count.json`.

## Representative Entity Verification

Representative assets were verified directly against GMS GraphQL:

- `urn:li:dataset:(urn:li:dataPlatform:iceberg,vina_bim_shop.fact_order,PROD)`
- `urn:li:dataset:(urn:li:dataPlatform:kafka,commerce_events,PROD)`
- `urn:li:dataset:(urn:li:dataPlatform:pinot,realtime_commerce_metrics_1m,PROD)`
- `urn:li:dataset:(urn:li:dataPlatform:s3,checkpoints.spark-events,PROD)`

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

## Screenshot

The refreshed DataHub screenshot is `evidence/final_integration/ui_screenshots/12_datahub_ui.png` and shows the lineage workspace centered on `vina_bim_shop.fact_order` with the summary panel open.

This view was chosen to replace the earlier columns view that showed an empty center panel. In the current cold-start UI state, the lineage canvas centers the dataset correctly but does not surface upstream edges even after the existing lineage metadata is re-emitted, so the summary panel is kept open to show entity context directly in the same screenshot.

## Evidence Location

- Governance evidence: `evidence/09_datahub_governance/`
- Airflow run manifest: `evidence/08_airflow_gx/runs/datahub_ingestion/adr08_datahub_20260603T003300Z/run_manifest.json`
- Screenshot set: `evidence/final_integration/ui_screenshots/`
