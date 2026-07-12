# DataHub Lineage and Contract Proof Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make DP1, DP2, and DP3 visible in DataHub as real pipeline jobs with dataset lineage, schemas, contracts, assertions, indexed search, and six reviewer-ready UI screenshots.

**Architecture:** Emit one Airflow DataFlow and three DataJobs matching the six-stage DAG, attach exact input/output datasets using existing URN helpers, and associate GX/dbt assertions with representative outputs. Extend evidence capture to verify the same relationships through indexed search and GraphQL, then capture the corresponding lineage and contract/assertion UI views.

**Tech Stack:** DataHub 1.6.0, Elasticsearch 7.10.1, Airflow, dbt metadata, Great Expectations, Python SDK/REST emitter, GraphQL, pytest, JSON, Markdown, and PNG.

## Global Constraints

- Topic 08 must meet its Definition of Done before this plan starts.
- Use DataFlow ID `mini_coursework_pipeline` and DataJob IDs `dp1_raw_to_bronze`, `dp2_bronze_to_silver_gold`, and `dp3_offline_features`.
- Reuse existing dataset URN builders and canonical platform/environment naming; do not create parallel URNs for the same table.
- Every DataJob must have nonempty inputs and outputs matching actual Airflow stage work.
- Contract/assertion proof must be associated with the displayed output dataset, not only exist as an unlinked assertion entity.
- DP3 schema proof must show exact `event_timestamp` and `created` fields.
- Indexed search and UI visibility are blocking. Direct GraphQL/GMS verification is supporting evidence only.
- Do not stage or commit unless explicitly requested. Preserve unrelated changes and prefix commands with `rtk`.

---

## Rubric Coverage

| Row | Requirement | Points | Current status | Effort | Value added |
|---:|---|---:|---|---|---|
| 34 | DP1 lineage between pipeline and related tables. | 2 | Partial | M | High |
| 35 | DP1 validation and data contract. | 2 | Partial | M | High |
| 36 | DP2 lineage between pipeline and related tables. | 2 | Partial | M | High |
| 37 | DP2 validation and data contract. | 2 | Partial | M | High |
| 38 | DP3 lineage between pipeline and feature tables. | 2 | Partial | M | High |
| 39 | DP3 validation and data contract. | 2 | Partial | M | High |

## Current Implementation and Evidence

- `spark_lineage.py` and `flink_lineage.py` emit table lineage.
- `gx_assertions.py` emits quality assertions and `emitter.py` provides shared DataHub emission helpers.
- dbt and Trino recipes emit schemas and model/table lineage.
- Airflow's `datahub_ingestion` run captures successful source reports and custom lineage counts.
- Current evidence proves entities through APIs but does not show DP-specific pipeline jobs, linked contracts, or full UI graph/assertion views.

## Gap, Scope, and Non-Goals

**Gap:** DP1-DP3 pipeline identities and job-to-table relationships are not explicit in DataHub UI, assertion association is not captured per DP, and UI evidence is insufficient.

**Scope:** Add coursework DataFlow/DataJobs, verify exact edges/assertions, run ingestion after final schema/orchestration changes, capture machine evidence and six UI screenshots, and document rows 34-39 in order.

**Non-goals:** Do not redesign all existing lineage emitters, add unrelated governance domains, enable authentication, or accept synthetic screenshot mockups.

## Dependencies

- Topic 06 finalizes dbt/Spark feature schemas.
- Topic 07 provides exact Airflow DAG/TaskGroup identities and stage artifacts.
- Topic 08 provides working indexed search and UI rendering.

## Exact File Map

| Action | Path | Responsibility |
|---|---|---|
| Create | `src/vina_bim_shop/datahub_lineage/coursework_pipelines.py` | Build and emit the DataFlow, three DataJobs, exact input/output edges, ownership, tags, and descriptions. |
| Modify | `src/vina_bim_shop/datahub_lineage/gx_assertions.py` | Associate DP-specific GX/dbt assertion entities with representative output datasets. |
| Modify | `src/vina_bim_shop/orchestration/datahub_ingestion.py` | Run coursework pipeline emission and expose counts/results in the ingestion manifest. |
| Create | `tests/unit/test_datahub_coursework_lineage.py` | Verify exact IDs, edge sets, dataset URN reuse, assertion association, and idempotency. |
| Modify | `tests/unit/test_orchestration_runtime.py` | Require coursework emission in DataHub ingestion results. |
| Modify | `scripts/datahub/capture_evidence.py` | Capture DataFlow/DataJob search, edges, schemas, assertions, and DP verification results. |
| Modify | `tests/unit/test_datahub_capture_evidence.py` | Test DP success/failure gates and screenshot-manifest requirements. |
| Modify | `deliverables/09_datahub_governance.md` | Add ordered row-34-to-39 UI/evidence proof. |
| Modify | `evidence/final_integration/datahub_lineage.md` | Summarize final UI-backed pipeline lineage and contracts. |
| Create | `evidence/09_datahub_governance/coursework_pipeline/coursework_pipeline_entities.json` | DataFlow/DataJob URNs, names, tags, ownership, and search verification. |
| Create | `evidence/09_datahub_governance/coursework_pipeline/dp1_verification.json` | DP1 inputs/outputs, Bronze schema/contract, assertions, and screenshot links. |
| Create | `evidence/09_datahub_governance/coursework_pipeline/dp2_verification.json` | DP2 Bronze/Silver/Gold edges, Gold contract/assertions, and screenshots. |
| Create | `evidence/09_datahub_governance/coursework_pipeline/dp3_verification.json` | DP3 feature edges, exact feature schema, tests/assertions, and screenshots. |
| Create | `evidence/09_datahub_governance/coursework_pipeline/ui_screenshot_manifest.json` | Six paths, page URLs/entity URNs, capture time, dimensions, and SHA-256 hashes. |
| Create | `evidence/09_datahub_governance/coursework_pipeline/run_manifest.json` | Ingestion run, search/index gates, DP results, screenshots, and artifacts. |
| Create | `evidence/09_datahub_governance/screenshots/datahub_dp1_lineage.png` | DP1 DataJob lineage graph with inputs and Bronze output. |
| Create | `evidence/09_datahub_governance/screenshots/datahub_dp1_contract.png` | DP1 output schema plus linked validation/assertion view. |
| Create | `evidence/09_datahub_governance/screenshots/datahub_dp2_lineage.png` | DP2 DataJob lineage through Bronze/Silver/Gold. |
| Create | `evidence/09_datahub_governance/screenshots/datahub_dp2_contract.png` | Gold output schema plus linked validation/assertion view. |
| Create | `evidence/09_datahub_governance/screenshots/datahub_dp3_lineage.png` | DP3 DataJob lineage into all three feature tables. |
| Create | `evidence/09_datahub_governance/screenshots/datahub_dp3_contract.png` | Feature schema with `event_timestamp`/`created` and linked tests/assertions. |
| Test | `tests/unit/test_datahub_coursework_lineage.py` | Pipeline metadata contract. |
| Test | `tests/unit/test_orchestration_runtime.py` | Ingestion integration contract. |
| Test | `tests/unit/test_datahub_capture_evidence.py` | Evidence/UI gate contract. |
| Test | `tests/unit/test_datahub_adr_boundaries.py` | Governance boundary regression. |
| Regenerate | `evidence/09_datahub_governance/coursework_pipeline/` | Re-emit and re-capture current DP metadata. |
| Regenerate | `evidence/09_datahub_governance/screenshots/datahub_dp1_lineage.png` | Re-capture current DP1 lineage. |
| Regenerate | `evidence/09_datahub_governance/screenshots/datahub_dp1_contract.png` | Re-capture current DP1 schema/assertion view. |
| Regenerate | `evidence/09_datahub_governance/screenshots/datahub_dp2_lineage.png` | Re-capture current DP2 lineage. |
| Regenerate | `evidence/09_datahub_governance/screenshots/datahub_dp2_contract.png` | Re-capture current DP2 schema/assertion view. |
| Regenerate | `evidence/09_datahub_governance/screenshots/datahub_dp3_lineage.png` | Re-capture current DP3 lineage. |
| Regenerate | `evidence/09_datahub_governance/screenshots/datahub_dp3_contract.png` | Re-capture current DP3 schema/assertion view. |

## Interfaces and Governance Contract

- `coursework_pipeline_entities(env="PROD") -> dict[str, object]` returns one DataFlow spec and exactly three DataJob specs.
- DataJob inputs/outputs are derived from the same constants/helpers used by Spark/dbt lineage; tests compare exact URN sets and reject duplicates.
- DP1 representative output is Bronze commerce/order input, DP2 representative output is `fact_order`, and DP3 representative output is `feat_customer_unified`; each verification also lists all stage outputs.
- Each DP verification requires: job indexed/searchable, expected edge set, output schema present, at least one associated passing assertion/test, and both screenshot files present with positive dimensions/hash.
- `capture_evidence()` returns failed status if any DP gate or screenshot manifest entry fails, even when direct entity queries succeed.

## Ordered Tasks

### Task 1: Lock DataFlow/DataJob and assertion contracts in tests

**Files:**
- Create: `tests/unit/test_datahub_coursework_lineage.py`
- Modify: `tests/unit/test_orchestration_runtime.py`
- Modify: `tests/unit/test_datahub_capture_evidence.py`

- [x] Assert one exact DataFlow and three exact DataJob IDs.
- [x] Assert every job has nonempty exact input/output URN sets and reuses canonical helpers.
- [x] Assert two identical emissions produce identical URNs/aspects.
- [x] Assert each representative output has an associated contract/assertion mapping.
- [x] Assert evidence fails for missing indexed job, edge, schema, assertion, or screenshot entry.
- [x] Run `rtk uv run pytest tests/unit/test_datahub_coursework_lineage.py tests/unit/test_orchestration_runtime.py tests/unit/test_datahub_capture_evidence.py -q`.

Expected: tests fail before the pipeline emitter and DP gates exist.

### Task 2: Emit coursework pipeline jobs and linked assertions

**Files:**
- Create: `src/vina_bim_shop/datahub_lineage/coursework_pipelines.py`
- Modify: `src/vina_bim_shop/datahub_lineage/gx_assertions.py`
- Modify: `src/vina_bim_shop/orchestration/datahub_ingestion.py`

- [x] Build the DataFlow and three DataJobs with descriptions matching the Airflow stage semantics.
- [x] Attach tags for `bronze`, `silver`, `gold`, `quality_gate`, `official`, or `provisional` only where they reflect actual outputs.
- [x] Emit job input/output lineage and associate assertions with target datasets.
- [x] Add `coursework_pipelines` results and counts to the existing ingestion manifest; fail ingestion when this required emission fails.
- [x] Run the focused unit tests.

Expected: deterministic metadata work units represent actual DP1-DP3 work and validation relationships.

### Task 3: Extend capture and run fresh ingestion

**Files:**
- Modify: `scripts/datahub/capture_evidence.py`
- Create/Regenerate: `evidence/09_datahub_governance/coursework_pipeline/*.json`

- [x] Add indexed search queries for the DataFlow and all three DataJobs.
- [x] Query and normalize job inputs/outputs, representative dataset schema fields, and associated assertions.
- [x] Require DP3 schema fields `event_timestamp` and `created` and reject feature `created_ts`.
- [x] Trigger `datahub_ingestion` after dbt, Airflow, and custom lineage assets are current.
- [x] Run `rtk uv run python scripts/datahub/capture_evidence.py` and require successful runtime-search and all three DP machine gates before UI capture.

Expected: machine-readable evidence proves the exact entities/edges/contracts the UI should display.

### Task 4: Capture six real UI views and bind them to evidence

**Files:**
- Create: the six screenshot files and `ui_screenshot_manifest.json`

- [x] Search for and open the DP1 DataJob in `http://localhost:9002`; expand lineage until source and Bronze output labels are visible, then save `datahub_dp1_lineage.png`.
- [x] Open the DP1 representative output's schema/assertion views and save `datahub_dp1_contract.png` with the dataset name visible.
- [x] Repeat for DP2, ensuring Bronze, Silver, and Gold nodes are visible.
- [x] Repeat for DP3, ensuring the three feature outputs and exact `event_timestamp`/`created` fields are visible across its two captures.
- [x] Record URL/entity URN, timestamp, pixel dimensions, SHA-256, and associated DP verification file for each image.
- [x] Reload each page once and verify it still renders from indexed metadata before accepting the image.

Expected: six readable UI captures match the machine-verified entities and are not generic home/search pages.

### Task 5: Document rows 34-39 and run regressions

**Files:**
- Modify: `deliverables/09_datahub_governance.md`
- Modify: `evidence/final_integration/datahub_lineage.md`

- [x] Add six ordered row sections, each with job/entity names, edge summary, validation/contract summary, machine evidence, and exact UI image.
- [x] Explain the separation between runtime recovery evidence and rubric governance evidence.
- [x] Remove statements that API proof substitutes for UI proof.
- [x] Run `rtk uv run pytest tests/unit/test_datahub_coursework_lineage.py tests/unit/test_orchestration_runtime.py tests/unit/test_datahub_capture_evidence.py tests/unit/test_datahub_adr_boundaries.py tests/unit/test_deliverables_documentation.py -q`.
- [x] Run `rtk uv run pytest -q` and inspect `rtk git status --short`.

Expected: all governance/full regressions pass and rows 34-39 are independently reviewable.

## Required Evidence

- DataFlow and three DataJob entity/search payloads.
- Three DP verification JSON files with edges, schema, assertion, and screenshot gates.
- Six readable UI screenshots and a hash/dimensions manifest.
- Successful ingestion and evidence run manifest.

## Definition of Done

- DP1, DP2, and DP3 appear as indexed DataJobs under the Airflow DataFlow.
- Each job shows actual input/output lineage and its representative output shows schema plus linked passing validation/assertion evidence.
- DP3 visibly uses `event_timestamp` and `created`.
- All six screenshots match machine-verified entities and survive page reload.
- API evidence alone cannot pass; focused and full tests succeed.

## Completion Record

**Completed:** 2026-07-12 (UTC). The source coursework run is
`topic07_20260711T124521Z`; the successful metadata ingestion run is
`topic09_datahub_20260712T072932Z`. The final evidence capture completed at
`2026-07-12T07:40:27.965991+00:00` with status `success`.

### Emitted identities and contracts

| Entity | URN | Exact edges | Passing linked assertions |
| --- | --- | ---: | ---: |
| DataFlow | `urn:li:dataFlow:(airflow,mini_coursework_pipeline,vina-bim-shop-local)` | 3 DataJobs | n/a |
| DP1 | `urn:li:dataJob:(urn:li:dataFlow:(airflow,mini_coursework_pipeline,vina-bim-shop-local),dp1_raw_to_bronze)` | 5 Kafka inputs, 2 Bronze S3 outputs | 4 |
| DP2 | `urn:li:dataJob:(urn:li:dataFlow:(airflow,mini_coursework_pipeline,vina-bim-shop-local),dp2_bronze_to_silver_gold)` | 2 Bronze inputs, 15 Silver + 19 core Gold Iceberg outputs | 1 |
| DP3 | `urn:li:dataJob:(urn:li:dataFlow:(airflow,mini_coursework_pipeline,vina-bim-shop-local),dp3_offline_features)` | 2 Iceberg inputs, 3 feature outputs | 12 |

Total coursework assertion run events emitted: **17**. The representative
schema checks are `bronze.batch.path`, `fact_order.order_id` plus
`official_paid_revenue`, and `event_timestamp` plus `created` on every DP3
feature output; `created_ts` is rejected by the DP3 gate.

### Indexed and machine evidence

Elasticsearch indexed search found the exact expected URN in every query:
DataFlow total **1**; DP1 total **2**; DP2 total **23**; DP3 total **1**. The
larger DP2 result set is a text search result, while the exact expected URN is
explicitly present. GraphQL compared the complete actual I/O sets with the
contract and all three DP verification results are `success`.

- `evidence/09_datahub_governance/coursework_pipeline/coursework_pipeline_entities.json`
- `evidence/09_datahub_governance/coursework_pipeline/dp1_verification.json`
- `evidence/09_datahub_governance/coursework_pipeline/dp2_verification.json`
- `evidence/09_datahub_governance/coursework_pipeline/dp3_verification.json`
- `evidence/09_datahub_governance/coursework_pipeline/run_manifest.json`

### UI evidence

Every page was reloaded before capture and verified at **1280 x 720** pixels.

| Target | SHA-256 |
| --- | --- |
| `datahub_dp1_lineage.png` | `8109c154e0412578bcd47d0c56144e5563e2c794286877b1bfb2b3d39a4adff4` |
| `datahub_dp1_contract.png` | `8d7d597ec5c29c52d893a46f41a6cfcaa0a5a4c6217b0d579b7c66174b8a9055` |
| `datahub_dp2_lineage.png` | `a5eb31dd03bfa76d8a63a145042ec7b18c5c2c84e46e77cf8f0f2bfc48559268` |
| `datahub_dp2_contract.png` | `64a1e7a65ad1ceb01c582209e8cf5bb1a2609fe1f0e612da5b20244f5dbc3178` |
| `datahub_dp3_lineage.png` | `002d5bee443082746e0b00e61897010afcbce80479052efa62935019e10ed4e9` |
| `datahub_dp3_contract.png` | `76f948bf770649fbb9b172063fdd7851d3d1836e84fc3e7a296c70612694f679` |

The capture URLs, entity URNs, timestamps, dimensions, reload confirmations,
and hashes are in
`evidence/09_datahub_governance/coursework_pipeline/ui_screenshot_manifest.json`.
The screenshots are a required gate; direct GMS/GraphQL evidence alone cannot
make this completion record pass.

### Commands and regression results

| Command | Exit | Result |
| --- | ---: | --- |
| `rtk docker compose exec -T airflow-scheduler airflow dags trigger datahub_ingestion --run-id topic09_datahub_20260712T072932Z` | 0 | Airflow DagRun and metadata manifest succeeded. |
| `rtk uv run python scripts/datahub/capture_evidence.py` | 0 | Runtime, indexed-search, all three DP, and six-screenshot gates succeeded. |
| `rtk uv run pytest -q tests/unit/test_datahub_coursework_lineage.py tests/unit/test_orchestration_runtime.py tests/unit/test_datahub_capture_evidence.py tests/unit/test_datahub_adr_boundaries.py tests/unit/test_deliverables_documentation.py` | 0 | 48 passed, 1 skipped. |
| `rtk uv run pytest -q tests/unit/test_script_surface_documentation.py` | 0 | 5 passed. |
| `rtk uv run pytest -q` | 0 | 346 passed, 1 skipped in 112.99 seconds. |
| `rtk docker compose stop <currently-running session services>` | 0 | Stopped the 16 services started for emission/capture; a subsequent `docker compose ps --services --status running` returned no services. |

The first full run exposed a pre-existing inventory omission for the tracked
`restore_search_indices.py` recovery script. The missing one-line
`scripts/README.md` entry was added, the focused inventory test passed, and the
final full run above passed.

### Residual limitations

- The DataHub lineage canvases deliberately summarize dense DP2 output branches;
  the complete 34-output contract is retained in `dp2_verification.json` and
  compared exactly by the gate.
- The emitted canonical output schemas are minimal display contracts for the
  rubric-facing fields, not a replacement for the wider Trino/dbt schemas.
- Elasticsearch `yellow` is normal for this single-node local runtime. Volumes
  and generated evidence are preserved. The session-owned Compose services
  were stopped with `docker compose stop` (never `down -v`), and the shared
  runtime slot was released after the post-stop running-service check returned
  no services.
