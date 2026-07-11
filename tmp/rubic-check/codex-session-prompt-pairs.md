# Codex Session Prompt Pairs

This document turns the eleven refactor plans into separate, copy-paste-ready Codex sessions. Use the planning prompt first when the implementation plan needs a current, read-only check. Use the execution prompt only after the handoff gate is met.

Source controls:

- Audit: `tmp/rubic-check/mini-coursework-rubric-audit.md`
- Program index: `tmp/rubic-check/refactor/00-refactor-index.md`
- Topic plans: `tmp/rubic-check/refactor/01-*.md` through `11-*.md`

## Shared Session Rules

### Planning Sessions

- Planning sessions are read-only: inspect code, configuration, tests, existing evidence, and the named topic plan; do not edit files, start services, regenerate evidence, or commit.
- All eleven planning sessions may run in parallel because they should not change the repository or shared runtime.
- A planning session ends with a concise decision-complete implementation handoff: confirmed scope, current paths, prerequisite status, risks, and any necessary plan correction. It does not begin execution.

### Execution Sessions

- Make changes directly in the current branch/folder for the topic; do not create or enter a dedicated Git worktree. Start by reading the audit, index, and the exact topic plan named in the prompt.
- Run `rtk git status --short` before editing. Preserve pre-existing changes and do not stage, commit, rebase, merge, or delete unrelated files unless the user explicitly asks.
- Follow the topic plan's Exact File Map, Ordered Tasks, Required Evidence, and Definition of Done. Record actual commands, test results, artifact paths, and limitations in its Completion Record only after successful work.
- Complete focused tests before runtime work, then run the full regression suite required by the plan. Do not upgrade an audit status based only on anticipated work.
- If a session starts Docker/Compose services, after recording the required evidence and before the Completion Handoff, stop only the services it started with `docker compose stop` or `docker stop` as applicable. Preserve volumes and evidence, do not stop pre-existing services, and confirm release of the runtime slot.

### Shared Runtime Rule

All execution sessions share the current checkout. Run exactly one execution session at a time, including edit, test, and runtime work, to prevent collisions. Planning sessions remain read-only and may run in parallel.

Only one execution session may hold the shared runtime slot at a time. Before a session starts, stops, rebuilds, resets, ingests into, or captures evidence from Docker, Spark, Flink, Airflow, DataHub, or Pinot, wait for the current runtime session to finish and hand over its evidence paths.

## Recommended Schedule

| Wave | Topics | Execution rule |
|---|---|---|
| Planning | 01-11 | All planning prompts may run in parallel; they are read-only. |
| 1 | 01 Docker, 02 Generator | Execute serially in the current checkout. Topic 02 is local generator work; Topic 01 owns the runtime slot only for its image/runtime proof. |
| 2 | 03 Spark, 04 Flink | Begin only after Topic 02. Execute Topics 03 and 04 serially, including their code/test preparation and runtime/evidence captures. |
| 3 | 06 Schema/feature contracts, 05 Storage | Integrate Topic 03 before Topic 06. Run Topic 06 before the final Topic 05 dbt/index benchmark when feature schema changes affect it. |
| 4 | 07 Airflow, 08 DataHub recovery | Topic 07 requires Topics 03 and 06. Topic 08 may run earlier when no other execution session is active, but its recovery run must finish before Topic 09. |
| 5 | 09 DataHub lineage, 10 Novel ideas | Topic 09 follows Topics 06-08. Topic 10 follows Topic 05 and must not overlap Topic 09. |
| Final | 11 README/rubric navigation | Start only after Topics 01-10 have completed and their evidence is real. |

Topic 11 is strictly last: it may begin only after all implementation and evidence topics have completed successfully.

## Topic 01: Kafka Connect Docker Image Optimization

**Source plan:** `tmp/rubic-check/refactor/01-docker-image-optimization.md`

**Handoff gate:** No predecessor topic. Docker Engine with BuildKit, Docker Compose, network access for pinned images/plugins, and the ingestion profile are available. Reserve the runtime slot only for the build and connector proof.

### Planning Prompt

```text
You are in a read-only planning session for Topic 01, Kafka Connect Docker Image Optimization.

Read tmp/rubic-check/mini-coursework-rubric-audit.md, tmp/rubic-check/refactor/00-refactor-index.md, and tmp/rubic-check/refactor/01-docker-image-optimization.md. Inspect the current Kafka Connect Dockerfile, Compose service, image tags, connector registration path, tests, and existing engineering evidence without changing files or starting services.

Confirm the current baseline image behavior, the exact builder/runtime split needed, and whether the plan's file map and commands still match the repository. Return a decision-complete execution handoff: prerequisite status, exact affected paths, test-first order, runtime-slot needs, expected evidence files, risks, and any plan correction. Do not implement or commit anything.
```

### Execution Prompt

```text
Implement Topic 01 directly in the current branch/folder; do not create or enter a Git worktree. Read the audit, program index, and tmp/rubic-check/refactor/01-docker-image-optimization.md before editing. Confirm the Topic 01 handoff gate, run rtk git status --short, and preserve unrelated changes.

Follow the topic plan exactly: capture the unoptimized Kafka Connect image baseline, add the tested multi-stage builder/runtime image, measure before/after bytes and MiB, prove a positive reduction, and run the existing connector/plugin smoke proof. Create only the planned code, tests, documentation, and evidence. Use the shared runtime slot only for Docker build/Compose work; do not overlap another runtime/evidence session.

Run the focused tests, Compose validation, and full regression required by the plan. Record actual image IDs, measurements, commands, evidence paths, and residual limitations in the Topic 01 Completion Record. Before handoff, if this session started Docker/Compose services, stop only those services with `docker compose stop` or `docker stop` as applicable, preserve volumes and evidence, and confirm the runtime slot is released. Do not stage or commit unless explicitly asked.
```

## Topic 02: Generator Rubric Evidence

**Source plan:** `tmp/rubic-check/refactor/02-generator-rubric-evidence.md`

**Handoff gate:** No predecessor topic. Confirm `configs/generator/base.yaml` remains the source of configuration values and that generator/Pandas/DuckDB dependencies resolve.

### Planning Prompt

```text
You are in a read-only planning session for Topic 02, Generator Rubric Evidence.

Read tmp/rubic-check/mini-coursework-rubric-audit.md, tmp/rubic-check/refactor/00-refactor-index.md, and tmp/rubic-check/refactor/02-generator-rubric-evidence.md. Inspect generator evidence production, the parsed YAML configuration path, source DataFrames, current tests, deliverables, and existing generator evidence. Do not edit files or run generation.

Verify that the plan can add DuckDB approx_count_distinct evidence for customer_id, product_id, order_id, and event_id without changing generator behavior. Produce an execution handoff with exact current paths, proposed test-first sequence, required ten-row evidence ordering, expected artifacts, and risks. Do not implement or commit.
```

### Execution Prompt

```text
Implement Topic 02 directly in the current branch/folder; do not create or enter a Git worktree. Read the audit, program index, and tmp/rubic-check/refactor/02-generator-rubric-evidence.md. Confirm the handoff gate and run rtk git status --short before editing.

Implement only the approved evidence packaging: derive approximate distinct counts and uniqueness ratios through DuckDB, render the row-5-to-14 report from parsed configs/generator/base.yaml values, preserve generator distributions and configuration behavior, and update the designated deliverables and manifest. Add or extend the planned tests before trusting generated output.

Run focused generator tests, regenerate the approved medium evidence, then run the full regression specified by the plan. Record real commands, metrics, artifact paths, and limitations in Topic 02's Completion Record. Before handoff, if this session started Docker/Compose services, stop only those services with `docker compose stop` or `docker stop` as applicable, preserve volumes and evidence, and confirm the runtime slot is released. Make no unrequested commit.
```

## Topic 03: Spark Skew, Cardinality, and Baseline

**Source plan:** `tmp/rubic-check/refactor/03-spark-skew-cardinality-and-baseline.md`

**Handoff gate:** Topic 02 is complete with current generator scenarios and cardinality evidence. Spark batch/lakehouse profiles, History Server, MinIO checkpoint storage, and screenshot capability are available. Reserve the runtime slot for Spark submissions and evidence capture.

### Planning Prompt

```text
You are in a read-only planning session for Topic 03, Spark Skew, Cardinality, and Baseline.

Read the audit, program index, Topic 02 Completion Record, and tmp/rubic-check/refactor/03-spark-skew-cardinality-and-baseline.md. Inspect Spark batch boundaries, canonical run_job protections, the coursework generator profile, event-log configuration, History Server access, and relevant tests. Do not edit files or start Docker/Spark.

Validate the four-run experiment design: skew-baseline, skew-optimized, high-cardinality-baseline, and high-cardinality-optimized. Confirm targeted deterministic salting/repartitioning for Ho Chi Minh City and Ha Noi, exact-result assertions, all-four-ID cardinality metrics, and UI proof paths. Return the implementation handoff and any necessary plan correction without implementing.
```

### Execution Prompt

```text
Implement Topic 03 directly in the current branch/folder after Topic 02 has passed its Definition of Done; do not create or enter a Git worktree. Read the audit, index, Topic 02 Completion Record, and tmp/rubic-check/refactor/03-spark-skew-cardinality-and-baseline.md. Run rtk git status --short before edits.

Build the standalone, test-first experiment only. Preserve canonical Spark semantics and enforce the plan's no-import/no-call boundary around run_job. Implement the controlled baseline-versus-optimized experiments, exact equivalence assertions, high-cardinality approximate/exact metrics, and required documentation/evidence contracts.

Claim the shared runtime slot for the four separate Spark submissions and History Server screenshots. Do not run concurrent Flink, Airflow, DataHub, Pinot, or other Docker evidence capture. Run focused and full regression checks, then record application IDs, measurements, screenshots, test results, and limitations in Topic 03's Completion Record. Before handoff, if this session started Docker/Compose services, stop only those services with `docker compose stop` or `docker stop` as applicable, preserve volumes and evidence, and confirm the runtime slot is released. Do not commit unless asked.
```

## Topic 04: Flink Baseline and Streaming Proof

**Source plan:** `tmp/rubic-check/refactor/04-flink-baseline-and-streaming-proof.md`

**Handoff gate:** Topic 02 is complete with generated streaming scenarios and measured burst, lateness, and duplicate rates. Ingestion, lakehouse, and streaming profiles are healthy; existing Flink clean-room verification passes. Reserve the runtime slot for Flink runs.

### Planning Prompt

```text
You are in a read-only planning session for Topic 04, Flink Baseline and Streaming Proof.

Read the audit, index, Topic 02 Completion Record, and tmp/rubic-check/refactor/04-flink-baseline-and-streaming-proof.md. Inspect current Flink configs/jobs, deterministic replay inputs, clean-room verification, topics, checkpoints, current evidence, and test boundaries. Do not edit files or start services.

Confirm that baseline and optimized variants will be isolated, consume identical replay input, preserve canonical defaults, and produce comparable output plus direct proof for burst, late arrival, duplicates, and event-time windows. Return an execution handoff with runtime-slot needs, exact artifacts, test order, and risks only.
```

### Execution Prompt

```text
Implement Topic 04 directly in the current branch/folder after Topic 02 has completed; do not create or enter a Git worktree. Read the audit, index, Topic 02 Completion Record, and tmp/rubic-check/refactor/04-flink-baseline-and-streaming-proof.md. Check rtk git status --short before editing.

Implement the approved isolated baseline/optimized profiles, comparison runner, evidence contracts, tests, and row-21-to-25 documentation. Keep canonical streaming configuration unchanged. First pass focused tests; then claim the shared runtime slot to publish one deterministic replay, submit both Flink variants serially, verify output, and capture the named UI evidence.

Run the planned full regression, record real job IDs, metrics, output checks, screenshots, commands, and remaining limitations in Topic 04's Completion Record. Before handoff, if this session started Docker/Compose services, stop only those services with `docker compose stop` or `docker stop` as applicable, preserve volumes and evidence, and confirm the runtime slot is released. Do not commit without explicit user instruction.
```

## Topic 05: Storage Optimization

**Source plan:** `tmp/rubic-check/refactor/05-storage-optimization.md`

**Handoff gate:** Topic 03 has produced populated Silver/Gold Iceberg tables; dbt builds the canonical DuckDB database; lakehouse and batch profiles are healthy. Schedule the final DuckDB benchmark after Topic 06 is integrated if the feature schema has changed.

### Planning Prompt

```text
You are in a read-only planning session for Topic 05, Storage Optimization.

Read the audit, index, Topic 03 Completion Record, current Topic 06 status, and tmp/rubic-check/refactor/05-storage-optimization.md. Inspect Iceberg table layout, Spark catalog access, Trino query path, canonical DuckDB build, existing warehouse/index evidence, and tests. Do not edit files, build data, or start services.

Confirm the compaction allowlist, before/after invariants, timing policy, and isolated temporary DuckDB benchmark database. State whether Topic 06 has changed feature contracts that require a later dbt rebuild. Return a decision-complete execution handoff with test sequence, exclusive runtime needs, evidence artifacts, and risks.
```

### Execution Prompt

```text
Implement Topic 05 directly in the current branch/folder after Topic 03 is complete and after Topic 06 is integrated when it affects the final dbt schema; do not create or enter a Git worktree. Read the audit, index, predecessor Completion Records, and tmp/rubic-check/refactor/05-storage-optimization.md. Run rtk git status --short before editing.

Implement exactly the approved Iceberg compaction and isolated DuckDB index experiments. Protect the canonical DuckDB file, enforce the table allowlist, preserve row-count/result-hash invariants, store raw timing samples and plans, and make no unsupported speed claim.

Claim the shared runtime slot for Spark/Iceberg/Trino and any Compose work. Run the required focused and full tests, then record actual rewrite results, query timings, index evidence, result equivalence, artifacts, and limitations in Topic 05's Completion Record. Before handoff, if this session started Docker/Compose services, stop only those services with `docker compose stop` or `docker stop` as applicable, preserve volumes and evidence, and confirm the runtime slot is released. Do not commit unless asked.
```

## Topic 06: Schema ERD and Feature Contracts

**Source plan:** `tmp/rubic-check/refactor/06-schema-erd-and-feature-contracts.md`

**Handoff gate:** Topics 02 and 03 are complete. Fresh generator inputs are available, Spark regression expectations are known, and Topic 07 has not yet fixed its DP3 validation against an outdated feature schema.

### Planning Prompt

```text
You are in a read-only planning session for Topic 06, Schema ERD and Feature Contracts.

Read the audit, index, Topic 02 and Topic 03 Completion Records, and tmp/rubic-check/refactor/06-schema-erd-and-feature-contracts.md. Inspect dbt feature SQL/YAML, Spark feature SQL, schema evidence generation, the all-zone ERD source, relationship artifacts, and tests. Do not edit files or regenerate evidence.

Confirm that only the three Gold feature outputs change from created_ts to created, while source, Bronze, Silver, dimensions, facts, OBTs, and aggregates retain created_ts. Verify the all-zone generated ERD approach and contract-test impact. Return a decision-complete execution handoff for Topic 06; do not implement.
```

### Execution Prompt

```text
Implement Topic 06 directly in the current branch/folder after Topics 02 and 03 meet their Definitions of Done; do not create or enter a Git worktree. Read the audit, index, predecessor Completion Records, and tmp/rubic-check/refactor/06-schema-erd-and-feature-contracts.md. Run rtk git status --short before editing.

Implement only the approved feature-contract and documentation changes: make all three Gold feature outputs expose event_timestamp and created in both dbt and Spark, retain upstream created_ts fields, validate the complete Bronze/Silver/Gold ERD, and regenerate the specified schema evidence. Update contracts, tests, and documentation together.

Use the runtime slot only for the generator/dbt/evidence regeneration portion. Run focused and full regressions, then record actual model/test results, ERD source/render artifacts, exact renamed columns, and limitations in Topic 06's Completion Record. Before handoff, if this session started Docker/Compose services, stop only those services with `docker compose stop` or `docker stop` as applicable, preserve volumes and evidence, and confirm the runtime slot is released. Do not commit unless requested.
```

## Topic 07: Airflow DP Stage Orchestration

**Source plan:** `tmp/rubic-check/refactor/07-airflow-dp-stage-orchestration.md`

**Handoff gate:** Topics 03 and 06 are complete. The canonical Spark pipeline still passes, the final `created` feature contract is known, and ingestion/lakehouse/batch/orchestration profiles are healthy.

### Planning Prompt

```text
You are in a read-only planning session for Topic 07, Airflow DP Stage Orchestration.

Read the audit, index, Topic 03 and Topic 06 Completion Records, and tmp/rubic-check/refactor/07-airflow-dp-stage-orchestration.md. Inspect existing Airflow DAGs, orchestration adapters, Spark entrypoints, validation paths, Compose initialization, and tests. Do not edit files or start services.

Confirm the six exact task identities, TaskGroup order, metadata seed requirements, compatibility wrapper, stage-manifest contracts, and expected graph/grid proof. Return a decision-complete execution handoff with exact dependencies, test-first order, runtime requirements, and risks. Do not implement.
```

### Execution Prompt

```text
Implement Topic 07 directly in the current branch/folder after Topics 03 and 06 are complete; do not create or enter a Git worktree. Read the audit, index, predecessor Completion Records, and tmp/rubic-check/refactor/07-airflow-dp-stage-orchestration.md. Run rtk git status --short before editing.

Implement the approved six-stage mini-coursework pipeline, idempotent stage functions, Airflow metadata seed, compatibility wrapper, stage artifacts, validations, and documentation. Preserve existing DAG contracts outside the planned scope and validate the final feature created contract in DP3.

Pass focused tests before claiming the shared runtime slot. Then run one successful logical window, capture the named graph/grid proof and six stage artifacts, run full regression, and record real run IDs, task states, validation results, screenshots, commands, and limitations in Topic 07's Completion Record. Before handoff, if this session started Docker/Compose services, stop only those services with `docker compose stop` or `docker stop` as applicable, preserve volumes and evidence, and confirm the runtime slot is released. Do not commit unless asked.
```

## Topic 08: DataHub Runtime Recovery

**Source plan:** `tmp/rubic-check/refactor/08-datahub-runtime-recovery.md`

**Handoff gate:** No earlier topic is required for initial recovery tests. The ingestion/lakehouse/governance profile bundle is available, Docker Desktop has sufficient capacity, and the session has exclusive ownership of the shared runtime. Topic 06 should be integrated before final re-ingestion.

### Planning Prompt

```text
You are in a read-only planning session for Topic 08, DataHub Runtime Recovery.

Read the audit, index, current DataHub Compose configuration, existing DataHub evidence, and tmp/rubic-check/refactor/08-datahub-runtime-recovery.md. Inspect image versions, search backend references, persisted metadata path, capture tests, and architecture documentation. Do not edit files, restart containers, or call restore endpoints.

Confirm the aligned DataHub 1.6.0 and Elasticsearch 7.10.1 migration, metadata-backup procedure, system-update requirement, restoreIndices flow, fail-closed search gate, and UI acceptance criteria. Return an execution handoff including backup/rollback controls, exclusive-runtime requirement, test order, and risks. Do not implement.
```

### Execution Prompt

```text
Implement Topic 08 directly in the current branch/folder; do not create or enter a Git worktree. Read the audit, index, current Topic 06 status, and tmp/rubic-check/refactor/08-datahub-runtime-recovery.md. Confirm the Topic 08 handoff gate, then run rtk git status --short before editing.

First implement and test the planned Compose alignment, index-restore script, search evidence gates, diagrams, and documentation. Before changing runtime services, exclusively claim the shared runtime slot, capture and verify the PostgreSQL metadata backup, and preserve volumes. Align DataHub backend/frontend/upgrade/actions and ingestion components to the approved versions with Elasticsearch 7.10.1.

Run system update, re-ingest current metadata when appropriate, restore indices, verify indexed search and real UI entity rendering after restart, and capture the named evidence. GraphQL-only success is insufficient. Run focused and full regressions and record actual versions, backup hash, restore totals, UI proof, test results, and rollback notes in Topic 08's Completion Record. Before handoff, if this session started Docker/Compose services, stop only those services with `docker compose stop` or `docker stop` as applicable, preserve volumes and evidence, and confirm the runtime slot is released. Do not commit unless asked.
```

## Topic 09: DataHub Lineage and Contract Proof

**Source plan:** `tmp/rubic-check/refactor/09-datahub-lineage-and-contract-proof.md`

**Handoff gate:** Topics 06, 07, and 08 are complete. Final feature schemas, exact Airflow TaskGroup identities/stage artifacts, and working indexed DataHub search/UI rendering are available.

### Planning Prompt

```text
You are in a read-only planning session for Topic 09, DataHub Lineage and Contract Proof.

Read the audit, index, Completion Records for Topics 06-08, and tmp/rubic-check/refactor/09-datahub-lineage-and-contract-proof.md. Inspect DataHub URN helpers, lineage/assertion emitters, orchestration ingestion path, existing datasets, the working UI, and evidence capture tests. Do not edit files, emit metadata, or capture screenshots.

Confirm the DataFlow/DataJob identities, DP1-DP3 edge sets, output schema/assertion associations, indexed-search gates, six screenshot targets, and idempotency requirements. Return a decision-complete execution handoff with exact evidence contract, runtime-slot needs, and risks. Do not implement.
```

### Execution Prompt

```text
Implement Topic 09 directly in the current branch/folder only after Topics 06, 07, and 08 are complete; do not create or enter a Git worktree. Read the audit, index, predecessor Completion Records, and tmp/rubic-check/refactor/09-datahub-lineage-and-contract-proof.md. Run rtk git status --short before editing.

Implement the planned DataFlow/DataJobs, exact DP1-DP3 dataset edges, schema/contracts/assertion links, idempotency tests, capture gates, and governance documentation. Require indexed search, expected edges, schemas, assertions, and screenshot manifests to pass together.

Claim the shared runtime slot for metadata emission, evidence capture, and all six UI screenshots. Do not accept API-only proof. Run focused and full regressions, then record real URNs, edge/assertion counts, search results, screenshot hashes, commands, and limitations in Topic 09's Completion Record. Before handoff, if this session started Docker/Compose services, stop only those services with `docker compose stop` or `docker stop` as applicable, preserve volumes and evidence, and confirm the runtime slot is released. Do not commit without explicit instruction.
```

## Topic 10: Novel Ideas Evidence

**Source plan:** `tmp/rubic-check/refactor/10-novel-ideas-evidence.md`

**Handoff gate:** Topic 05 is complete with current DuckDB optimization evidence and a successful dbt database. Flink/Pinot runtime checks and existing Pinot evidence pass. Topic 11 has not yet finalized README links.

### Planning Prompt

```text
You are in a read-only planning session for Topic 10, Novel Ideas Evidence.

Read the audit, index, Topic 05 Completion Record, current Pinot/Flink evidence, and tmp/rubic-check/refactor/10-novel-ideas-evidence.md. Inspect dbt/DuckDB outputs, parity/index evidence, Pinot bootstrap/query/capture paths, existing deliverables, and tests. Do not edit files or start services.

Confirm the exact ordered ideas: Novel Idea 1: DuckDB/dbt local analytics and Novel Idea 2: Pinot realtime serving. Verify their success gates, representative queries, source provenance, screenshots, evidence schemas, and documentation changes. Return an execution handoff only.
```

### Execution Prompt

```text
Implement Topic 10 directly in the current branch/folder after Topic 05 has completed; do not create or enter a Git worktree. Read the audit, index, Topic 05 Completion Record, and tmp/rubic-check/refactor/10-novel-ideas-evidence.md. Run rtk git status --short before editing.

Implement the evidence aggregator, tests, ordered deliverable, manifests, and screenshots exactly as planned. Prove current dbt/DuckDB analytics with a real successful query and prove Pinot realtime serving with table/segment health, provenance, and a successful query. Keep the exact idea names and order.

Claim the shared runtime slot only for dbt/Pinot/Flink/Compose runtime actions and do not overlap a DataHub, Airflow, Spark, or Flink evidence capture. Run focused and full regressions, then record real query results, runtime evidence, screenshots, commands, and limitations in Topic 10's Completion Record. Before handoff, if this session started Docker/Compose services, stop only those services with `docker compose stop` or `docker stop` as applicable, preserve volumes and evidence, and confirm the runtime slot is released. Do not commit unless asked.
```

## Topic 11: README and Rubric Navigation

**Source plan:** `tmp/rubic-check/refactor/11-readme-and-rubric-navigation.md`

**Handoff gate:** Topics 01 through 10 are complete, each Completion Record names passing real evidence, Topic 08 has removed the obsolete UI limitation only after proof, and Topic 10 has created the novel-ideas deliverable and evidence.

### Planning Prompt

```text
You are in a read-only planning session for Topic 11, README and Rubric Navigation.

Read the audit, index, Completion Records for Topics 01-10, their generated evidence, and tmp/rubic-check/refactor/11-readme-and-rubric-navigation.md. Inspect the README, architecture documentation, deliverable indexes, declared public API modules, final integration evidence, and existing documentation tests. Do not edit files or regenerate artifacts.

Verify that every intended rubric upgrade has validated evidence, every upstream topic is genuinely complete, and the final manifest can fail closed on missing/changed artifacts. Return a decision-complete finalization handoff with exact links, documentation targets, public-API audit scope, test order, and any reason an audit status must remain Partial. Do not implement.
```

### Execution Prompt

```text
Implement Topic 11 directly in the current branch/folder only after Topics 01-10 have passed their Definitions of Done; do not create or enter a Git worktree. Read the audit, index, all predecessor Completion Records, and tmp/rubic-check/refactor/11-readme-and-rubric-navigation.md. Run rtk git status --short before editing.

Implement the final reviewer navigation, architecture/documentation conventions, declared public API docstring audit, row-ordered evidence manifest, deliverable links, and final audit refresh. The manifest and audit must remain fail-closed: retain Partial status whenever required evidence is missing, stale, or unsupported.

Run all focused documentation/manifest tests and the full regression suite. Do not claim completion until every upstream evidence link/hash is verified. Record actual commands, test outcomes, artifact hashes, final rubric statuses, and residual limitations in Topic 11's Completion Record. Before handoff, if this session started Docker/Compose services, stop only those services with `docker compose stop` or `docker stop` as applicable, preserve volumes and evidence, and confirm the runtime slot is released. Do not commit unless explicitly asked.
```

## Completion Handoff

After any execution session completes, hand the next session its completed Topic Completion Record, the exact generated evidence paths, focused/full test commands and exit codes, Docker cleanup confirmation (including stopped session-started containers and preserved volumes/evidence), runtime release confirmation, and unresolved limitations. The next execution session must re-check those facts before it begins.

Do not mark the program complete until Topic 11 verifies the final rubric manifest and audit from real artifacts.
