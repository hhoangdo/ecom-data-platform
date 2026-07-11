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
