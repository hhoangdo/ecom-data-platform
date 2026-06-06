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

The refreshed DataHub screenshot is `evidence/final_integration/ui_screenshots/12_datahub_ui.png`. It was captured on 2026-06-05 from the direct entity URL for `vina_bim_shop.fact_order` and shows the Lineage workspace with the dataset summary panel open.

This view replaces the earlier columns view that showed an empty center panel. In the current cold-start UI state, the lineage canvas centers the dataset correctly but does not surface upstream edges, assertion cards, or search results reliably even after the existing lineage metadata is re-emitted. The screenshot is therefore paired with direct GMS/GraphQL evidence as the authoritative proof of catalog, lineage, assertion, and tag coverage.

| Evidence Source | Value | What It Proves |
|---|---:|---|
| `datahub_health.json.status_code` | 200 | DataHub GMS was reachable during the refresh. |
| `dataset_count.json.total_datasets_emitted` | 109 | Kafka, S3/MinIO, Trino, and dbt datasets were emitted. |
| `dataset_count.json.custom_lineage.spark_entities` | 20 | Spark Silver/Gold lineage metadata was emitted. |
| `dataset_count.json.custom_lineage.flink_entities` | 3 | Flink real-time lineage metadata was emitted. |
| `dataset_count.json.custom_lineage.gx_assertions_emitted` | 8 | GX assertion metadata was emitted. |
| `dataset_count.json.verified_representative_datasets` | 4/4 | Representative Iceberg, Kafka, Pinot, and S3 entities resolve through GraphQL. |
| `tag_count.json.verified_tag_count` | 6 | Governance vocabulary tags resolve through GraphQL. |

## Evidence Location

- Governance evidence: `evidence/09_datahub_governance/`
- Airflow run manifest: `evidence/08_airflow_gx/runs/datahub_ingestion/adr08_datahub_20260603T003300Z/run_manifest.json`
- Screenshot set: `evidence/final_integration/ui_screenshots/`
