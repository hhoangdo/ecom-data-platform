# Mini-Coursework Rubric Audit

**Audit source of truth:** `evidence/final_integration/mini_coursework_rubric_manifest.json`, rebuilt and verified on 2026-07-13. The manifest validates repository-relative nonempty implementation/evidence paths, records SHA-256 hashes, and recomputes row-specific gates. A changed or missing artifact makes `--verify` fail; it cannot be upgraded manually through this audit.

## Executive Status

| Status | Rows | Points | Meaning |
| --- | ---: | ---: | --- |
| Satisfied | 45 | 100 | Every required path, hash, and row gate passed. |
| Partial | 0 | 0 | Reserved for missing, stale, or unsupported evidence. |
| Missing | 0 | 0 | Reserved for an absent complete evidence path. |

## Validated Rubric Rows

| Row | Requirement area | Points | Status | Bound evidence | Maintenance / residual note |
| ---: | --- | ---: | --- | --- | --- |
| 2 | README and reviewer navigation | 10 | Satisfied | README, API coverage, final deliverable, manifest | Re-run API audit and manifest after changing declared API/docs. |
| 3 | Docker image optimization | 1 | Satisfied | Kafka Connect image comparison | Reduction is measured, not extrapolated. |
| 4 | Multi-stage Dockerfile | 1 | Satisfied | Kafka Connect Dockerfile and plugin smoke | Preserve the builder-to-runtime copy contract. |
| 5 | Offline skew | 2 | Satisfied | Generator rubric summary | Rebuild only with a documented generator profile. |
| 6 | Offline high cardinality | 2 | Satisfied | Cardinality summary | Approximate counts remain explicitly labeled. |
| 7 | Schema evolution | 2 | Satisfied | Schema-version summary | Preserve null/evolution evidence. |
| 8 | Offline duplicates | 2 | Satisfied | Generator quality report | Keep configured and observed rates paired. |
| 9 | Offline configuration | 2 | Satisfied | Generator config and manifest | Preserve named profile settings. |
| 10 | Bronze input storage | 2 | Satisfied | Generator summary | Raw data remains Bronze-ready source input. |
| 11 | Streaming burst | 2 | Satisfied | Generator rubric summary | Keep burst controls and observed counts linked. |
| 12 | Late arrivals | 2 | Satisfied | Generator rubric summary | Keep configured rate and measured rate linked. |
| 13 | Streaming duplicates | 2 | Satisfied | Generator rubric summary | Keep configured rate and measured rate linked. |
| 14 | Streaming configuration | 2 | Satisfied | Generator manifest | Preserve submission scale and seed. |
| 15 | Spark baseline | 2 | Satisfied | Spark optimization manifest | Standalone experiments do not alter canonical semantics. |
| 16 | Spark skew handling | 2 | Satisfied | Spark optimization manifest | Keep controlled salting evidence isolated. |
| 17 | Spark high cardinality | 2 | Satisfied | Spark optimization manifest | Keep controlled repartition evidence isolated. |
| 18 | Spark schema evolution | 2 | Satisfied | Spark optimization report | Preserve current compatibility behavior. |
| 19 | Spark duplicate handling | 2 | Satisfied | Spark optimization report | Preserve quarantine/validation evidence. |
| 20 | Spark Airflow integration | 2 | Satisfied | Spark and Airflow evidence | Keep task integration documented. |
| 21 | Flink baseline | 2 | Satisfied | Flink comparison | Optimized job cancellation is documented after successful checkpoint/equality proof. |
| 22 | Flink burst | 2 | Satisfied | Flink challenge samples | Keep input/output sample proof. |
| 23 | Flink late arrivals | 2 | Satisfied | Flink challenge samples | Keep correction-path proof. |
| 24 | Flink duplicates | 2 | Satisfied | Flink challenge samples | Keep duplicate metric proof. |
| 25 | Flink windows | 2 | Satisfied | Flink comparison | Preserve event-time configuration proof. |
| 26 | Lakehouse compaction | 2 | Satisfied | Compaction result and file stats | Controlled smoke-scale layout proof, not a general performance claim. |
| 27 | Warehouse/index optimization | 2 | Satisfied | Query and DuckDB index benchmarks | Isolated timing is not a production claim. |
| 28 | DP1 ingest | 2 | Satisfied | Topic 07 DP1 artifact and screenshots | Preserve the exact six-task run contract. |
| 29 | DP1 validation | 2 | Satisfied | Topic 07 Bronze GX contract | Preserve 2/2 GX result. |
| 30 | DP2 transform | 2 | Satisfied | Topic 07 DP2 artifact | Preserve application ID and table-count proof. |
| 31 | DP2 validation | 2 | Satisfied | Topic 07 Gold GX contract | Preserve 2/2 GX result. |
| 32 | DP3 feature compute | 2 | Satisfied | Topic 07 DP3 artifact | Preserve feature application proof. |
| 33 | DP3 feature validation | 2 | Satisfied | Topic 07 feature contract | Preserve `event_timestamp`/`created` and no `created_ts`. |
| 34 | DP1 lineage | 2 | Satisfied | DataHub DP1 lineage capture/UI hash | Indexed UI proof remains required. |
| 35 | DP1 contract | 2 | Satisfied | DataHub DP1 contract capture/UI hash | Indexed UI proof remains required. |
| 36 | DP2 lineage | 2 | Satisfied | DataHub DP2 lineage capture/UI hash | Indexed UI proof remains required. |
| 37 | DP2 contract | 2 | Satisfied | DataHub DP2 contract capture/UI hash | Indexed UI proof remains required. |
| 38 | DP3 lineage | 2 | Satisfied | DataHub DP3 lineage capture/UI hash | Indexed UI proof remains required. |
| 39 | DP3 contract | 2 | Satisfied | DataHub DP3 contract capture/UI hash | Indexed UI proof remains required. |
| 40 | All-zone ERD | 2 | Satisfied | Schema manifest and rendered ERD | Preserve model inventory gate. |
| 41 | SCD2-compatible dimensions | 1 | Satisfied | Schema inventory | Current-row behavior is not full historical SCD2. |
| 42 | Feature timestamp contract | 1 | Satisfied | dbt catalog and schema inventory | Only feature outputs use `created`. |
| 43 | Dimension/fact relationships | 2 | Satisfied | Physical Gold ERD | Preserve relationship topology. |
| 44 | Layer naming conventions | 2 | Satisfied | Schema deliverable and manifest | Preserve Bronze/Silver/Gold naming table. |
| 45 | Novel Idea 1 | 5 | Satisfied | DuckDB/dbt idea JSON and screenshot | Local parity path is not distributed canonical truth. |
| 46 | Novel Idea 2 | 5 | Satisfied | Pinot idea JSON and screenshot | Pinot is fresh/provisional; Spark Gold remains canonical. |

## Reverification

```powershell
rtk uv run python scripts/qa/audit_public_documentation.py --output evidence/final_integration/public_documentation_coverage.json
rtk uv run python scripts/qa/build_mini_coursework_rubric_manifest.py --output evidence/final_integration/mini_coursework_rubric_manifest.json
rtk uv run python scripts/qa/build_mini_coursework_rubric_manifest.py --verify --manifest evidence/final_integration/mini_coursework_rubric_manifest.json
```

If any command fails or the summary includes a Partial/Missing row, retain that status and state the exact manifest failure here rather than carrying forward this audit result.
