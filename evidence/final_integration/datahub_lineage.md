# DataHub Governance Evidence

## Service Status

DataHub was successfully started via the `governance` profile:
- GMS (port 8087): healthy
- Frontend (port 9002): running (login page captured in screenshot)
- Actions: running
- OpenSearch: healthy

## Ingestion Recipes

Available ingestion recipes in `infra/governance/recipes/`:
- `kafka_topics.yml` — registers Kafka topics as DataHub datasets
- `minio_storage.yml` — registers MinIO buckets/containers
- `trino_tables.yml` — registers Iceberg tables via Trino catalog
- `dbt_legacy.yml` — registers dbt-DuckDB models for lineage parity

## Execution Status

DataHub ingestion recipes are available but require explicit execution. To run:

```powershell
# From within the datahub-actions container or via CLI:
datahub ingest -c infra/governance/recipes/kafka_topics.yml
datahub ingest -c infra/governance/recipes/minio_storage.yml
datahub ingest -c infra/governance/recipes/trino_tables.yml
datahub ingest -c infra/governance/recipes/dbt_legacy.yml
```

## Lineage Path

The complete data lineage from source to consumption:

```
Generator → Kafka (raw events) → Kafka Connect S3 → MinIO Bronze
                                  ↓
                       Flink (streaming) → Derived Kafka topics → Pinot (provisional)
                                  ↓
Generator → MinIO Bronze (batch) → Spark → Iceberg Silver/Gold → Trino (canonical)
                                                                  ↓
                                                         DataHub (governance)
                                                                  ↓
                                                    dbt-DuckDB (parity)
```

## Screenshot

DataHub UI screenshot captured at `evidence/final_integration/ui_screenshots/datahub_ui.png` (login page, proves service is running).

## Evidence Location

- DataHub evidence directory: `evidence/09_datahub_governance/`
- DataHub screenshot: `evidence/final_integration/ui_screenshots/datahub_ui.png`
