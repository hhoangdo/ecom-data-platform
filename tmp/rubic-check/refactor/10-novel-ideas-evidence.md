# Novel Ideas Evidence Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Package DuckDB/dbt local analytics as Novel Idea 1 and Pinot realtime serving as Novel Idea 2 with explicit problem statements, runnable proof, measured outputs, screenshots, value, and limitations.

**Architecture:** Reuse the implemented analytics and serving paths rather than inventing new features. Add one evidence aggregator that validates upstream manifests and executes one representative query per idea, then write a dedicated deliverable with stable proof links.

**Tech Stack:** DuckDB, dbt-duckdb, Spark/Trino parity evidence, Apache Pinot, Kafka, Flink, Python 3.12, pytest, JSON, Markdown, and PNG.

## Global Constraints

- Novel Idea 1 is exactly `DuckDB/dbt local analytics`.
- Novel Idea 2 is exactly `Pinot realtime serving`.
- Each idea section must include problem, implementation, why it is novel in this coursework, reproducible command, result, evidence, value, trade-offs, and limitations.
- Reuse existing canonical pipelines; do not add a third novel idea or speculative functionality.
- Machine evidence must come from a fresh successful run and screenshots must show the named idea, not a generic service home page.
- Topic 05 index evidence may support Idea 1 but is not the only proof; successful dbt models and a real analytical query are required.
- Pinot proof requires a realtime table with consuming/online segments and a successful query over derived streaming data.
- Do not stage or commit unless explicitly requested. Preserve unrelated changes and prefix commands with `rtk`.

---

## Rubric Coverage

| Row | Requirement | Points | Current status | Effort | Value added |
|---:|---|---:|---|---|---|
| 45 | Novel Idea 1 with documentation and proof it worked. | 5 | Partial | S | High |
| 46 | Novel Idea 2 with documentation and proof it worked. | 5 | Partial | S | High |

## Current Implementation and Evidence

- `deliverables/10_duckdb_dbt_local_analytics.md` documents local Bronze/Silver/Gold dbt models, contracts, portability, and Spark parity.
- `evidence/02_schema_design/` and `evidence/05_spark_batch/dbt_parity_report.*` prove dbt model/test execution and canonical parity.
- `deliverables/07_pinot_serving.md` documents realtime tables fed by Flink-derived Kafka topics.
- `evidence/07_pinot_serving/` contains health, consuming-segment, table inventory, query output, and Pinot UI screenshots.
- No deliverable labels these capabilities explicitly as Idea 1 and Idea 2 with a shared proof contract.

## Gap, Scope, and Non-Goals

**Gap:** The ideas exist but are not framed, verified, and cross-linked in the exact rubric format.

**Scope:** Add one evidence validator/aggregator, focused tests, two fresh idea summaries, two screenshots, a dedicated deliverable, and navigation links.

**Non-goals:** Do not build ML/LLM features, change dbt business logic, redesign Pinot schemas, or claim production scale.

## Dependencies

- Topic 05 provides current DuckDB optimization evidence and a successful dbt database.
- Flink/Pinot profiles and `evidence/07_pinot_serving/` must pass their current runtime checks.
- Topic 11 adds the final README link after this deliverable exists.

## Exact File Map

| Action | Path | Responsibility |
|---|---|---|
| Create | `scripts/qa/capture_novel_ideas.py` | Validate upstream manifests, execute representative DuckDB/Pinot queries, and write idea evidence/manifests. |
| Create | `tests/unit/test_novel_ideas_evidence.py` | Test required upstream gates, output schemas, fixed idea names, and failure behavior. |
| Create | `deliverables/12_novel_ideas.md` | Two exact ordered idea sections with proof, value, and limitations. |
| Modify | `deliverables/README.md` | Add the new deliverable to the index. |
| Modify | `tests/unit/test_deliverables_documentation.py` | Require the deliverable and exact idea headings/links. |
| Create | `evidence/10_novel_ideas/idea_1_duckdb_dbt.json` | dbt build/test counts, model inventory, representative query/result, parity/index links, and source hashes. |
| Create | `evidence/10_novel_ideas/idea_2_pinot_realtime.json` | Pinot table status, consuming segments, source topic, query/result, freshness, and reconciliation links. |
| Create | `evidence/10_novel_ideas/run_manifest.json` | Commands, versions, upstream manifests, two success gates, screenshots, and artifacts. |
| Create | `evidence/10_novel_ideas/screenshots/idea_1_duckdb_dbt_lineage.png` | dbt docs lineage centered on an implemented Gold model. |
| Create | `evidence/10_novel_ideas/screenshots/idea_2_pinot_realtime_query.png` | Pinot query console showing a successful realtime query and result. |
| Test | `tests/unit/test_novel_ideas_evidence.py` | Evidence aggregator contract. |
| Test | `tests/unit/test_deliverables_documentation.py` | Deliverable/navigation contract. |
| Test | `tests/unit/test_pinot_runtime.py` | Pinot evidence/query regression. |
| Test | `tests/unit/test_section02_schema_design.py` | dbt model/schema regression. |
| Regenerate | `evidence/10_novel_ideas/idea_1_duckdb_dbt.json` | Rebuild from current dbt/DuckDB/parity/index evidence. |
| Regenerate | `evidence/10_novel_ideas/idea_2_pinot_realtime.json` | Rebuild from current Pinot runtime/query evidence. |
| Regenerate | `evidence/10_novel_ideas/run_manifest.json` | Refresh both gates and artifact hashes. |
| Regenerate | `evidence/10_novel_ideas/screenshots/idea_1_duckdb_dbt_lineage.png` | Re-capture current dbt lineage. |
| Regenerate | `evidence/10_novel_ideas/screenshots/idea_2_pinot_realtime_query.png` | Re-capture current Pinot query result. |

## Interfaces and Evidence Contract

- `capture_novel_ideas(repo_root: Path, evidence_root: Path) -> dict[str, object]` writes exactly two idea files and one manifest.
- Idea names in JSON and Markdown are exact and ordered: `Novel Idea 1: DuckDB/dbt local analytics`, then `Novel Idea 2: Pinot realtime serving`.
- Idea 1 gate requires a successful current dbt build result, positive Gold model/test counts, a successful aggregate query against `gold.fact_order`, and links to Spark parity plus Topic 05 indexing evidence.
- Idea 2 gate requires healthy controller/broker, an online realtime table, at least one consuming or completed segment, successful query rows, and source-topic/derived-output provenance.
- The aggregator exits nonzero when upstream manifests are stale/failed, required results are empty, or either screenshot is missing/zero-byte at finalization.

## Ordered Tasks

### Task 1: Define the two-idea evidence contract

**Files:**
- Create: `tests/unit/test_novel_ideas_evidence.py`

- [x] Test exact idea names/order and output paths.
- [x] Test Idea 1 fails for unsuccessful dbt results, zero models/tests, failed query, or missing parity/index links.
- [x] Test Idea 2 fails for unhealthy Pinot, offline table, no segments, empty query rows, or missing topic provenance.
- [x] Test the final manifest requires both success gates and both nonempty screenshots.
- [x] Run `rtk uv run pytest tests/unit/test_novel_ideas_evidence.py -q`.

Expected: tests fail because the capture script does not exist.

### Task 2: Implement the evidence aggregator

**Files:**
- Create: `scripts/qa/capture_novel_ideas.py`

- [x] Load and validate existing dbt, parity, index, and Pinot manifests rather than copying their metrics manually.
- [x] Query canonical DuckDB read-only and capture a stable aggregate over `gold.fact_order` with column names and typed values.
- [x] Reuse the current Pinot query endpoint/query manifest and capture table/topic/segment provenance.
- [x] Record upstream file SHA-256 hashes and capture timestamps in both idea files.
- [x] Add `--allow-missing-screenshots` only for pre-capture generation; final mode remains strict.
- [x] Run `rtk uv run pytest tests/unit/test_novel_ideas_evidence.py -q`.

Expected: focused tests pass and failure modes remain explicit.

### Task 3: Execute fresh DuckDB/dbt and Pinot proof

**Files:**
- Regenerate: `evidence/10_novel_ideas/`

- [x] Run `rtk uv run dbt build --project-dir infra/analytics/dbt --profiles-dir infra/analytics/dbt` and require successful model/tests.
- [x] Run Topic 05's DuckDB index benchmark if its current manifest is absent or stale.
- [x] Start Pinot dependencies with `rtk docker compose --profile ingestion --profile streaming --profile serving up -d --build`.
- [x] Run `rtk uv run python scripts/pinot/bootstrap.py`, `rtk uv run python scripts/pinot/query_examples.py`, and `rtk uv run python scripts/pinot/capture_evidence.py`.
- [x] Run `rtk uv run python scripts/qa/capture_novel_ideas.py --allow-missing-screenshots` and inspect both idea JSON files.
- [x] Capture dbt lineage from generated dbt docs and a successful Pinot realtime query at the exact image paths.
- [x] Re-run `rtk uv run python scripts/qa/capture_novel_ideas.py` in strict final mode.

Expected: the final manifest reports both ideas successful and binds current upstream evidence plus screenshots.

### Task 4: Write the explicit novel-ideas deliverable

**Files:**
- Create: `deliverables/12_novel_ideas.md`
- Modify: `deliverables/README.md`
- Modify: `tests/unit/test_deliverables_documentation.py`

- [x] Add exact Idea 1 and Idea 2 headings in order.
- [x] For each idea, document problem, architecture, implementation paths, run command, observed result, proof links/image, value, trade-offs, and limitations.
- [x] Explain Idea 1 as a portable local analytics/parity path and Idea 2 as low-latency provisional serving reconciled by canonical batch truth.
- [x] Add the deliverable to the index and assert its headings/required evidence links in tests.
- [x] Run `rtk uv run pytest tests/unit/test_novel_ideas_evidence.py tests/unit/test_deliverables_documentation.py -q`.

Expected: both five-point rows have self-contained, explicit proof narratives.

### Task 5: Run regressions and final evidence checks

- [x] Run `rtk uv run pytest tests/unit/test_novel_ideas_evidence.py tests/unit/test_deliverables_documentation.py tests/unit/test_pinot_runtime.py tests/unit/test_section02_schema_design.py -q`.
- [x] Run `rtk uv run pytest -q`.
- [x] Verify both screenshots have positive dimensions and the manifest hashes match them.
- [x] Inspect `rtk git status --short` for only authorized changes plus pre-existing user work.

Expected: full tests pass and both idea packages are reproducible and current.

## Required Evidence

- Two machine-readable idea summaries and one strict run manifest.
- Successful dbt model/test and DuckDB query proof with parity/index links.
- Healthy Pinot realtime table/segment/query proof with stream provenance.
- One readable, idea-specific screenshot per idea.

## Definition of Done

- `deliverables/12_novel_ideas.md` labels the two selected ideas exactly and in rubric order.
- Each idea includes implementation, executable proof, observed result, value, trade-offs, and limitations.
- Final evidence aggregator passes strict mode with current upstream artifacts and nonempty screenshots.
- Focused and full tests pass; no speculative third idea is added.

## Completion Record

Completed 2026-07-12. The two idea names and their order are exact: `Novel Idea 1: DuckDB/dbt local analytics`, then `Novel Idea 2: Pinot realtime serving`. No third idea was added.

### Evidence implementation

- Added `scripts/qa/capture_novel_ideas.py`, which writes exactly `idea_1_duckdb_dbt.json`, `idea_2_pinot_realtime.json`, and `run_manifest.json`. It validates upstream evidence rather than copying assertions, executes the fixed DuckDB and Pinot queries, records SHA-256 provenance, and rejects missing/zero-byte screenshots in strict mode. The pre-capture `--allow-missing-screenshots` mode was used only before the screenshots existed.
- Added the focused evidence tests and documentation assertions, the ordered deliverable, deliverable index entry, and script-inventory entry. The aggregator additionally hashes `infra/analytics/dbt/models/gold/fact_order.sql`, so the stated model provenance is cryptographically bound to Idea 1.

### Fresh Idea 1 capture

- Ran `rtk uv run dbt build --project-dir infra/analytics/dbt --profiles-dir infra/analytics/dbt`, then `rtk uv run dbt docs generate --project-dir infra/analytics/dbt --profiles-dir infra/analytics/dbt`.
- The current `run_results.json` records dbt 1.11.11 with 52 successful models, 66 successful tests, and 22 Gold models. The existing Topic 05 parity report has 27 successful comparisons.
- Re-ran `rtk uv run python scripts/analytics/benchmark_duckdb_index.py --source-db data/gold/vina_bim_shop.duckdb --benchmark-db tmp/rubic-check/runtime/duckdb_index_benchmark.duckdb --evidence-root evidence/10_duckdb_dbt_local_analytics/index_optimization`; it preserved the canonical DuckDB hash `e093eb5894dda2f0db2171c97f1d41a950e7c365508beaf9c12a2ceaab6a479c` and the isolated `idx_benchmark_fact_order_order_id` result parity.
- The read-only `gold.fact_order` aggregate returned one typed row: `order_count=1800` (`BIGINT`), `paid_order_count=1734` (`HUGEINT`), `official_paid_revenue=5488980096.0` (`DOUBLE`), and `gross_merchandise_value=5737896000.0` (`DOUBLE`).

### Fresh Idea 2 capture and runtime coordination

- Claimed the shared dbt/Pinot/Flink/Compose runtime slot only for these actions. No DataHub, Airflow, Spark, or separate Flink evidence capture ran concurrently.
- The first profile Compose command with `--build` timed out before starting containers. The immediately following `rtk docker compose --profile ingestion --profile lakehouse --profile streaming --profile serving up -d` used the resolved local images and started the required runtime. `scripts/pinot/refresh_evidence.py` is the current wrapper that invokes bootstrap, query examples, and evidence capture in order.
- Ran `rtk uv run python scripts/flink/publish_smoke.py --evidence-root evidence/10_novel_ideas`, then `rtk uv run python scripts/pinot/refresh_evidence.py`. Both Flink jobs were running before capture. Persisted Pinot metadata initially contained stale instance registrations; the repair removed only stale Pinot realtime table/instance metadata, re-applied the serving assets, re-published the smoke data, and refreshed evidence. Docker volumes and repository evidence were not removed.
- The final Pinot gate recorded controller and broker healthy, `pinot_realtime_ops_alerts_REALTIME` in `HEALTHY` state with one segment and consuming-segment proof. Flink-derived topic, table configuration, and smoke output all agree on `realtime_ops_alerts`; the source topics are `ops_events`, `catalog_events`, and `fulfillment_events`.
- The successful Pinot query was non-partial and returned seven groups: `payment_failure_spike` / `medium` / `10`, then `duplicate_spike`, `inventory_low_stock`, `late_arrival`, `shipment_blocked_payment_failed`, `shipment_delayed`, and `traffic_burst`, each with count `4`.

### Final artifacts, screenshots, and verification

- Strict final command: `rtk uv run python scripts/qa/capture_novel_ideas.py`; result: `Novel ideas evidence status: success` at `2026-07-12T13:38:05.531634+00:00`.
- Screenshot evidence is current and hash-bound by the manifest: `idea_1_duckdb_dbt_lineage.png` is 1280x720, SHA-256 `45178abdf5bb4f47ffcfda0bfed3a552a279c87e09a727d95b21ee32d5a6c39a`; `idea_2_pinot_realtime_query.png` is 1265x712, SHA-256 `91a31ff7ee165c2990a5b3e3500ed45e8dd37cb678bb1ed78018016b14d11381`.
- Focused regression: `42 passed in 11.04s`. Full regression: `354 passed, 1 skipped in 128.25s`.
- Stopped the temporary dbt docs server after screenshot capture. Then ran `rtk docker compose stop` only for the ingestion, lakehouse, streaming, and Pinot-serving services started in this session; no volumes were removed. A post-stop `docker ps` service allowlist was empty, confirming the shared runtime slot was released.

### Limitations retained

- DuckDB/dbt is the local reproducibility and parity path, not distributed canonical truth; isolated index timings are not production performance claims.
- Pinot serves fresh provisional alerts. Late corrections require correction-aware multi-stage queries, while reconciled KPIs remain canonical in Spark Gold through Trino.
