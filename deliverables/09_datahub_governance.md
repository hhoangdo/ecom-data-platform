# DataHub Governance

## Purpose

DataHub is the governance and metadata catalog layer for the Vina Bim Shop
platform. It records datasets, tags, lineage, quality assertions, and evidence
for the same Kafka, MinIO/S3, Trino/Iceberg, dbt, Spark, Flink, and Great
Expectations assets that the platform produces. It can emit metadata about the same assets that the pipelines produce.

It is not a decorative UI: acceptance requires indexed search and a rendered
entity page, in addition to direct service checks.

Tags such as `bronze`, `silver`, `gold`, `official`, `provisional`, and `quality_gate` classify assets.

## Runtime Contract

The governance profile is aligned to the following tested version set.

| Component | Version or image | Role |
| --- | --- | --- |
| DataHub system update | `acryldata/datahub-upgrade:v1.6.0` | Applies schema, mapping, and system metadata updates before GMS starts. |
| DataHub GMS | `acryldata/datahub-gms:v1.6.0` | Metadata API, GraphQL, and search-index producer. |
| DataHub frontend | `acryldata/datahub-frontend-react:v1.6.0` | Local catalog UI on port 9002. |
| DataHub actions | `acryldata/datahub-actions:v1.6.0-slim` | DataHub action runtime. |
| Elasticsearch | `elasticsearch:7.10.1` | Persistent search and graph indices on `datahub_search_data`. |
| DataHub ingestion CLI and Airflow plugin | `1.6.0` | Recipe and lineage ingestion in the Airflow image. |

`datahub-gms` and the system-update job use Elasticsearch as their search and
graph implementation. They share the existing Kafka broker and PostgreSQL
metadata database, and use the project Schema Registry for new MCL records.
The recovery MCL consumer starts at the current high-water mark; the explicit
restore replay repopulates the index without moving or deleting legacy consumer
group offsets.

## Metadata Sources

| Recipe or emitter | Governance role |
| --- | --- |
| `infra/governance/recipes/kafka_topics.yml` | Kafka topic datasets. |
| `infra/governance/recipes/minio_storage.yml` | Four MinIO/S3 prefix datasets, including `checkpoints.spark-events`. |
| `infra/governance/recipes/trino_tables.yml` | Trino and Iceberg datasets. |
| `infra/governance/recipes/dbt_legacy.yml` | dbt model metadata. |
| `src/vina_bim_shop/datahub_lineage/` | Spark/Flink lineage, tags, ownership, and GX assertions. |

## Recovery Procedure

Perform recovery only while the shared runtime slot is exclusively held. Do not
overlap DataHub, Spark, Flink, Airflow, or Pinot runtime sessions.

1. Start only the required dependencies and verify a PostgreSQL backup before
   changing services. The migration backup is custom-format `pg_dump` output;
   its SHA-256, byte count, database, and source aspect count are recorded in
   `evidence/09_datahub_governance/runtime_recovery/preflight.json`.
2. Start the aligned `ingestion`, `lakehouse`, and `governance` profile bundle
   without removing any volume. Require `datahub-system-update` to exit `0`.
3. Re-ingest current metadata when a source needs a fresh MCL event. The
   recovery run re-ingested the file-backed MinIO/S3 metadata with the pinned
   DataHub 1.6.0 CLI so the checkpoint asset was indexed.
4. Replay persisted PostgreSQL metadata through the documented direct GMS
   endpoint and retain every request/response:

   ```powershell
   uv run python scripts/datahub/restore_search_indices.py `
     --gms-url http://localhost:8087 `
     --elasticsearch-url http://localhost:9200 `
     --urn-like "urn:li:%" `
     --batch-size 1000
   ```

5. Run `uv run python scripts/datahub/capture_evidence.py`. Its manifest is
   failed when Elasticsearch health, positive indices, or any representative
   indexed search result is absent. Direct GraphQL entity resolution alone
   cannot pass this gate.

## Acceptance Evidence

The completed local recovery has the following recorded results.

| Check | Result | Artifact |
| --- | --- | --- |
| Metadata source before migration | 1,150 `metadata_aspect_v2` rows | `runtime_recovery/preflight.json` |
| Restore replay | 963 rows migrated | `runtime_recovery/restore_indices.json` |
| Search backend | Elasticsearch yellow (single-node) with 50 `datasetindex_v2` documents | `runtime_recovery/elasticsearch_indices.json` |
| Indexed representatives | Iceberg `fact_order`, Kafka `commerce_events`, Pinot `realtime_commerce_metrics_1m`, and S3 `checkpoints.spark-events` | `runtime_recovery/search_results.json` |
| Search UI after restart | 18 `fact_order` results | `screenshots/datahub_search_results.png` |
| Entity UI after restart | `vina_bim_shop.fact_order` page and lineage graph | `screenshots/datahub_dataset_entity.png` |

The UI screenshots were captured after restarting the frontend, proving that
the named Elasticsearch volume preserves the recovered search state. The
lineage graph renders an upstream Spark task and `stg_orders` dataset.

## Operational Controls and Rollback

- Preserve `lakehouse_postgres_data`, `kafka_kraft_data`, and
  `datahub_search_data`; a normal recovery never runs `docker compose down -v`.
- Use `docker compose stop` for session-owned services when handing off the
  shared runtime; it leaves volumes and evidence intact.
- The backup is intentionally ignored at
  `evidence/runtime/datahub-before-1.6.0.dump`. Its recorded SHA-256 is
  `B2E5B2526240E280B87E031FA3AFFD4962AB02B72166381CF8C5408CE542C4E4`.
- A database rollback is an explicit operator action: stop GMS and dependent
  writers, validate the backup with `pg_restore --list`, then restore with
  `pg_restore` into PostgreSQL. Re-run system update and index restoration
  afterwards. Do not replace or delete volumes as a shortcut.

## Evidence Locations

| Path | Purpose |
| --- | --- |
| `evidence/09_datahub_governance/datahub_health.json` | GMS health response. |
| `evidence/09_datahub_governance/dataset_count.json` | Dataset, lineage, assertion, and direct entity evidence. |
| `evidence/09_datahub_governance/tag_count.json` | Governance-tag evidence. |
| `evidence/09_datahub_governance/search_results.json` | Fail-closed indexed search evidence. |
| `evidence/09_datahub_governance/runtime_recovery/` | Backup preflight, versions, system update, restore, search, and index records. |
| `evidence/09_datahub_governance/screenshots/` | Search and entity UI acceptance screenshots. |

## Convenience Targets

`make up-governance` starts the governance profile. The existing
`make down-governance` target removes Compose volumes and is therefore not a
recovery command; use a targeted `docker compose stop` when preserving the
metadata and search volumes is required.
