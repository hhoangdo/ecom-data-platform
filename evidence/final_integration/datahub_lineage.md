# DataHub Governance Evidence

## Runtime Status

The local governance stack uses DataHub `v1.6.0` with Elasticsearch `7.10.1`.
The system-update job exited `0`, PostgreSQL metadata was restored into the
persisted search volume, and all representative datasets pass indexed search.

## Current Evidence

| Gate | Result | Evidence |
| --- | --- | --- |
| PostgreSQL metadata backup | Verified before migration | `evidence/09_datahub_governance/runtime_recovery/preflight.json` |
| System update | Exit `0` | `evidence/09_datahub_governance/runtime_recovery/system_update.log` |
| Index restore | 963 persisted rows replayed | `evidence/09_datahub_governance/runtime_recovery/restore_indices.json` |
| Elasticsearch | Yellow single-node health; populated `datasetindex_v2` | `evidence/09_datahub_governance/runtime_recovery/elasticsearch_indices.json` |
| Indexed search | Iceberg, Kafka, Pinot, and S3 representatives found | `evidence/09_datahub_governance/runtime_recovery/search_results.json` |
| UI search after restart | 18 `fact_order` results | `evidence/09_datahub_governance/screenshots/datahub_search_results.png` |
| Entity page after restart | `vina_bim_shop.fact_order` lineage canvas rendered | `evidence/09_datahub_governance/screenshots/datahub_dataset_entity.png` |

Direct GMS entity and tag checks remain committed in
`evidence/09_datahub_governance/`, but they are supplemental: a failed
Elasticsearch search gate makes the runtime evidence fail.

## Topic 09 Coursework Pipeline Lineage and Contracts

The indexed DataFlow
`urn:li:dataFlow:(airflow,mini_coursework_pipeline,vina-bim-shop-local)`
contains the three rubric-facing DataJobs:

- DP1 has 5 Kafka inputs and 2 Bronze S3 outputs, with 4 passing assertions.
- DP2 has 2 Bronze inputs and 34 Silver/Gold Iceberg outputs, with a passing
  `fact_order` assertion.
- DP3 has 2 Iceberg inputs and 3 feature outputs, with 12 passing assertions.

`evidence/09_datahub_governance/coursework_pipeline/run_manifest.json` is the
fail-closed acceptance record: it requires exact GraphQL edge sets, indexed
DataFlow/DataJob search results, output schemas, linked passing assertions,
and a valid six-image UI manifest. It reports success from the Airflow
ingestion run `topic09_datahub_20260712T072932Z`; API-only proof does not
satisfy this gate. The screenshots show the actual DataHub lineage canvases
and output contract/quality pages, while the JSON verification files retain
the complete edge and assertion details that are too dense for a screenshot.

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

## Recovery and Rollback

The recovery process preserves PostgreSQL, Kafka, and Elasticsearch volumes.
Routine shutdown uses `docker compose stop`; do not use a volume-removing
`down` command during recovery. The verified custom-format PostgreSQL backup
is local and ignored at `evidence/runtime/datahub-before-1.6.0.dump`; restoring
it with `pg_restore` is an explicit rollback operation, not a normal step.
