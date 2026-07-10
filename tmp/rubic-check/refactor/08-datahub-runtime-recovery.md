# DataHub Runtime Recovery Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the incompatible custom search stack with an aligned DataHub 1.6.0 plus Elasticsearch 7.10.1 runtime and prove that metadata is searchable and rendered in the UI.

**Architecture:** Preserve PostgreSQL as the metadata source of truth and Kafka as the event transport, replace the ephemeral search service, run DataHub system update, restore persisted aspects into fresh search indices, and strengthen evidence capture so search/UI failures are blocking. The selected versions follow the official DataHub 1.6 quickstart defaults.

**Tech Stack:** DataHub OSS 1.6.0, Elasticsearch 7.10.1, PostgreSQL, Kafka, Docker Compose, Python 3.12, requests, GraphQL, pytest, JSON, Markdown, and browser screenshots.

## Global Constraints

- Pin `datahub-upgrade`, `datahub-gms`, and `datahub-frontend-react` to `v1.6.0`; pin `datahub-actions` to `v1.6.0-slim`; retain Airflow ingestion packages at `1.6.0`.
- Replace service `datahub-opensearch` with `datahub-elasticsearch` using image `elasticsearch:7.10.1` and persistent volume `datahub_search_data`.
- Set search implementation to `elasticsearch` and remove search-backend environment flags that only apply to the previous service.
- Take and verify a PostgreSQL metadata backup before changing the runtime.
- Never delete PostgreSQL, Kafka, or search volumes as part of normal recovery. Destructive reset requires a separate explicit user instruction.
- A healthy GMS endpoint or direct entity query is insufficient: indexed search and real UI rendering are mandatory acceptance gates.
- Use the documented restore endpoint `POST /api/gms/operations?action=restoreIndices` and save every request/response.
- Do not stage or commit unless explicitly requested. Preserve unrelated changes and prefix commands with `rtk`.

Official references:

- [DataHub 1.6 quickstart Compose](https://raw.githubusercontent.com/datahub-project/datahub/v1.6.0/docker/quickstart/docker-compose-without-neo4j.quickstart.yml)
- [DataHub Restore Indices endpoint](https://docs.datahub.com/docs/api/restli/restore-indices/)

---

## Rubric Prerequisite

This plan owns no additional workbook row. It is a blocking prerequisite for rows 34-39 in `09-datahub-lineage-and-contract-proof.md`; those rows cannot pass while search/UI rendering is broken.

| Requirement | Current status | Effort | Value added |
|---|---|---|---|
| Search backend and DataHub services use a tested, aligned version set. | Partial | M | High |
| Persisted metadata is restored into searchable indices. | Missing | M | High |
| Search results and entity pages render in the DataHub UI. | Missing | M | High |

## Current Implementation and Evidence

- `compose/governance.datahub.yml` mixes DataHub service images `v1.5.0.6`, actions `v0.0.15`, Airflow ingestion CLI `1.6.0`, and OpenSearch `2.19.3`.
- GMS health and direct metadata API evidence are successful for representative entities.
- `scripts/datahub/capture_evidence.py` explicitly states search indexing is unnecessary, allowing the known UI failure to pass.
- `README.md` and `deliverables/09_datahub_governance.md` record limited UI graph rendering as a known limitation.
- The official DataHub 1.6 quickstart defaults to Elasticsearch `7.10.1`, not the current custom search image.

## Gap, Scope, and Non-Goals

**Gap:** Version alignment, durable search storage, index restoration, search-result verification, and UI acceptance are absent.

**Scope:** Diagnose and snapshot the current state, back up metadata, align services, replace search, restore indices, strengthen capture/tests, update affected diagrams/docs, and capture working UI evidence.

**Non-goals:** Do not migrate away from PostgreSQL/Kafka, enable production authentication, introduce Kubernetes, preserve the old search volume, or accept API-only proof.

## Dependencies

- Use the documented profile bundle: `ingestion`, `lakehouse`, and `governance`.
- Docker Desktop must have at least 8 GB available to the stack.
- Topic 06 should finalize feature metadata before the final re-ingestion, but runtime search recovery can be tested with existing entities first.

## Exact File Map

| Action | Path | Responsibility |
|---|---|---|
| Modify | `compose/governance.datahub.yml` | Align DataHub images, replace search service, set Elasticsearch hosts/implementation, health checks, and persistent volume. |
| Modify | `docker-compose.yml` | Declare `datahub_search_data`. |
| Create | `scripts/datahub/restore_search_indices.py` | Batch restore persisted aspects, capture responses, verify index/search completion, and fail closed. |
| Create | `tests/unit/test_datahub_search_recovery.py` | Test pagination, response handling, search gates, and no destructive behavior. |
| Modify | `scripts/datahub/capture_evidence.py` | Add Elasticsearch index and GraphQL search evidence; make failed search blocking. |
| Modify | `tests/unit/test_datahub_capture_evidence.py` | Test search success/failure and remove API-only success assumptions. |
| Modify | `tests/unit/test_datahub_adr_boundaries.py` | Assert exact service/image/host/version contract. |
| Modify | `tests/unit/test_compose_split_contract.py` | Replace the old service-name ownership assertion. |
| Modify | `tests/unit/test_architecture_diagrams.py` | Expect Elasticsearch in governance components. |
| Modify | `architecture/diagrams/mermaid/09-governance.mmd` | Show Elasticsearch search dependency and restore path. |
| Regenerate | `architecture/diagrams/mermaid/09-governance.png` | Render updated governance diagram. |
| Modify | `architecture/diagrams/detailed-lambda_architecture.puml` | Align detailed service label. |
| Modify | `architecture/diagrams/detailed-architecture.excalidraw` | Align editable detailed diagram. |
| Regenerate | `architecture/diagrams/detailed-architecture.svg` | Render updated detailed architecture. |
| Modify | `README.md` | Remove the accepted-UI-limitation language after proof passes. |
| Modify | `deliverables/09_datahub_governance.md` | Document recovery, version matrix, restore process, search/UI acceptance, and operational rollback. |
| Create | `tmp/rubic-check/runtime/datahub-before-1.6.0.sql` | Local ignored PostgreSQL backup used only for rollback. |
| Create | `evidence/09_datahub_governance/runtime_recovery/preflight.json` | Initial versions, service states, GMS/search status, index inventory, and database counts. |
| Create | `evidence/09_datahub_governance/runtime_recovery/version_matrix.json` | Final exact image and ingestion CLI versions. |
| Create | `evidence/09_datahub_governance/runtime_recovery/system_update.log` | Successful upgrade job output. |
| Create | `evidence/09_datahub_governance/runtime_recovery/restore_indices.json` | Batched requests/responses and totals. |
| Create | `evidence/09_datahub_governance/runtime_recovery/elasticsearch_indices.json` | Final index names, health, document counts, and aliases. |
| Create | `evidence/09_datahub_governance/runtime_recovery/search_results.json` | Indexed search results for representative entities. |
| Create | `evidence/09_datahub_governance/runtime_recovery/run_manifest.json` | Backup hash, commands, versions, gates, and artifacts. |
| Create | `evidence/09_datahub_governance/screenshots/datahub_search_results.png` | UI search results containing expected datasets. |
| Create | `evidence/09_datahub_governance/screenshots/datahub_dataset_entity.png` | Loaded dataset entity overview and navigation tabs. |
| Test | `tests/unit/test_datahub_search_recovery.py` | Restore/search script contract. |
| Test | `tests/unit/test_datahub_capture_evidence.py` | Evidence gate contract. |
| Test | `tests/unit/test_datahub_adr_boundaries.py` | Compose/runtime contract. |
| Test | `tests/unit/test_compose_split_contract.py` | Compose ownership contract. |
| Test | `tests/unit/test_architecture_diagrams.py` | Diagram regression. |
| Regenerate | `evidence/09_datahub_governance/runtime_recovery/` | Re-run recovery verification after any version/config change. |
| Regenerate | `evidence/09_datahub_governance/screenshots/datahub_search_results.png` | Re-capture current UI search. |
| Regenerate | `evidence/09_datahub_governance/screenshots/datahub_dataset_entity.png` | Re-capture current entity page. |

## Interfaces and Recovery Contract

- `restore_indices(frontend_url, urn_like, batch_size, evidence_root) -> dict[str, object]` uses `urnLike="urn:li:%"`, batch size `1000`, increments `start`, records every response, and stops only when the response reports fewer restored rows than the requested batch or an explicit completion flag.
- The restore script exits nonzero on HTTP errors, repeated identical pages, malformed responses, zero restored entities when PostgreSQL contains metadata, or failed post-restore search.
- `capture_search_evidence()` verifies indexed search returns representative Kafka, Iceberg, Pinot, and S3 datasets already listed in `REPRESENTATIVE_DATASET_URNS`.
- Runtime success requires Elasticsearch health `yellow` or `green`, expected DataHub indices with positive document counts, GMS health `200`, frontend health, search results, and entity-page UI proof.
- The backup manifest stores file SHA-256, byte size, PostgreSQL database name, and capture timestamp without embedding credentials.

## Ordered Tasks

### Task 1: Capture preflight and write failing runtime/search tests

**Files:**
- Create: `tests/unit/test_datahub_search_recovery.py`
- Modify: the four existing test files listed in the Exact File Map
- Create: `evidence/09_datahub_governance/runtime_recovery/preflight.json`

- [ ] Capture current image pins, compose config, GMS health, failed/empty search behavior, Elasticsearch/OpenSearch index inventory, and PostgreSQL aspect count without changing services.
- [ ] Test exact final image tags, service name, host references, implementation value, volume, and actions tag.
- [ ] Test restore pagination, repeated-page protection, zero-restore failure when source metadata exists, and post-restore search gating.
- [ ] Update evidence tests so direct entity lookups with failed search produce overall `failed` status.
- [ ] Run `rtk uv run pytest tests/unit/test_datahub_search_recovery.py tests/unit/test_datahub_capture_evidence.py tests/unit/test_datahub_adr_boundaries.py tests/unit/test_compose_split_contract.py -q`.

Expected: tests fail against the current mixed-version search stack and permissive evidence gate.

### Task 2: Back up metadata and align the Compose runtime

**Files:**
- Modify: `compose/governance.datahub.yml`
- Modify: `docker-compose.yml`
- Create: `tmp/rubic-check/runtime/datahub-before-1.6.0.sql`

- [ ] Start database dependencies with `rtk docker compose --profile lakehouse up -d lakehouse-postgres`.
- [ ] Run `rtk proxy powershell -NoProfile -Command "rtk docker compose exec -T lakehouse-postgres pg_dump -U vina_platform -d datahub | Out-File -Encoding utf8 -LiteralPath 'tmp/rubic-check/runtime/datahub-before-1.6.0.sql'"`.
- [ ] Run `rtk proxy powershell -NoProfile -Command '$file = Get-Item -LiteralPath "tmp/rubic-check/runtime/datahub-before-1.6.0.sql"; if ($file.Length -le 0) { throw "Empty DataHub backup" }; Get-FileHash -Algorithm SHA256 -LiteralPath $file.FullName'` and verify the SQL contains DataHub schema statements before proceeding.
- [ ] Replace the search service and all host/dependency references; mount `datahub_search_data`.
- [ ] Align all four DataHub image families and remove stale search implementation flags.
- [ ] Run `rtk docker compose --profile ingestion --profile lakehouse --profile governance config` and inspect resolved services, images, dependencies, volume, and ports.
- [ ] Re-run focused compose tests.

Expected: Compose resolves one aligned DataHub 1.6.0 stack with Elasticsearch 7.10.1 and a verified metadata backup exists.

### Task 3: Implement restore and blocking search evidence

**Files:**
- Create: `scripts/datahub/restore_search_indices.py`
- Modify: `scripts/datahub/capture_evidence.py`
- Modify: corresponding unit tests

- [ ] Implement batched restore with durable response capture and explicit termination safeguards.
- [ ] Capture Elasticsearch index health/document counts through its HTTP API.
- [ ] Add indexed GraphQL search queries and exact representative-URN checks.
- [ ] Remove the note and logic that let search indexing be optional.
- [ ] Run `rtk uv run pytest tests/unit/test_datahub_search_recovery.py tests/unit/test_datahub_capture_evidence.py -q`.

Expected: pure tests prove recovery fails closed when search is absent or incomplete.

### Task 4: Run system update, restore indices, and verify UI

**Files:**
- Regenerate: `evidence/09_datahub_governance/runtime_recovery/`
- Create: the two runtime UI screenshots

- [ ] Start the aligned stack with `rtk docker compose --profile ingestion --profile lakehouse --profile governance up -d --build`.
- [ ] Require `datahub-system-update` to exit `0`; save its complete logs and stop if it fails.
- [ ] Trigger existing DataHub ingestion once so PostgreSQL has current metadata.
- [ ] Run `rtk uv run python scripts/datahub/restore_search_indices.py --frontend-url http://localhost:9002 --urn-like "urn:li:%" --batch-size 1000 --evidence-root evidence/09_datahub_governance/runtime_recovery`.
- [ ] Run `rtk uv run python scripts/datahub/capture_evidence.py` and require success with positive index/search counts.
- [ ] Search for `fact_order` in `http://localhost:9002`, open the exact Iceberg entity, verify overview/schema/lineage tabs render, and save the two screenshots.
- [ ] Confirm browser reload and container restart preserve search results through the named volume.

Expected: metadata survives restart, indexed search returns expected assets, and entity pages render fully.

### Task 5: Update documentation/diagrams and run regressions

**Files:**
- Modify/Regenerate: documentation and diagram files from the Exact File Map

- [ ] Update governance diagrams and render their generated formats.
- [ ] Replace the old known-limitation language only after UI acceptance passes.
- [ ] Document backup, upgrade, restore, verification, and rollback steps with exact evidence paths.
- [ ] Run `rtk uv run pytest tests/unit/test_datahub_search_recovery.py tests/unit/test_datahub_capture_evidence.py tests/unit/test_datahub_adr_boundaries.py tests/unit/test_compose_split_contract.py tests/unit/test_architecture_diagrams.py tests/unit/test_deliverables_documentation.py -q`.
- [ ] Run `rtk uv run pytest -q` and inspect `rtk git status --short`.

Expected: runtime, diagrams, docs, and full repository tests agree on the repaired stack.

## Required Evidence

- Verified PostgreSQL backup hash and preflight snapshot.
- Exact final version matrix and successful system-update log.
- Complete restore request/response record.
- Elasticsearch index health/counts and indexed search results.
- Search-results and entity-page UI screenshots after restart.

## Definition of Done

- DataHub services and ingestion CLI are aligned at 1.6.0 and use Elasticsearch 7.10.1.
- System update and index restoration complete without data loss.
- Expected assets appear in indexed search and real UI entity pages render after restart.
- Evidence capture fails when search fails; direct metadata lookup alone cannot pass.
- Focused and full tests pass and old limitation text is removed only after proof exists.

## Completion Record

The implementing session records date, backup hash/size, old and new versions, system-update exit status, restored counts, index/document counts, representative search results, restart test, screenshots, tests, and rollback notes here. Until then, unchecked tasks define the work.
