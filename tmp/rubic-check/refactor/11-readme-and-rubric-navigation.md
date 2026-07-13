# README and Rubric Navigation Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Finalize the repository's reviewer-facing navigation, declared public API documentation, architecture conventions, and evidence-backed rubric status after Topics 01-10 are complete.

**Architecture:** Keep `README.md` as the entry point, add one final rubric-evidence deliverable, and generate a machine-readable manifest that refuses unsupported status upgrades. Define a small deployable public API surface and audit its module/symbol docstrings at 100% instead of treating every internal helper as a public API.

**Tech Stack:** Markdown, Python AST, Python 3.12, pytest, JSON, SHA-256, Git, existing architecture artifacts, and RTK.

## Global Constraints

- Execute this plan last, after Topics 01-10 meet their Definitions of Done.
- Never mark a row `Satisfied` from planned work; every upgraded status requires existing nonempty artifacts and passing checks.
- Preserve the current business-domain statement, runnable/architectural-contract distinction, and documented Compose profile order.
- `README.md` must retain a working Table of Contents, business context, high-level deployment diagram, deployment order, evidence navigation, repository structure, limitations, and exact commands.
- Declare only the curated deployable API surface listed below; internal helpers remain internal and do not need cosmetic docstrings.
- Every curated module and symbol must have a nonempty AST docstring and a README/file-description link.
- Do not stage or commit unless explicitly requested. Preserve unrelated changes and prefix shell commands with `rtk`.

---

## Rubric Coverage

| Row | Requirement | Points | Current status | Effort | Value added |
|---:|---|---:|---|---|---|
| 2 | README/system overview, business domain, deployment diagram, repository structure, file/function/class documentation, contents, deployable units, labeled flow, and order. | 10 | Partial | S | High |

## Current Implementation and Evidence

- `README.md` already has a Table of Contents, business domain, architecture overview, staged deployment commands, evidence links, repository structure, and limitations.
- `architecture/diagrams/detailed-architecture.svg` is the primary deployment diagram and architecture tests already enforce many labels/flows.
- Topic deliverables and evidence are comprehensive but dispersed.
- No centralized rubric proof document or fail-closed evidence manifest exists.
- Public entrypoint docstrings are not centrally declared or measured, making row-2 proof ambiguous.

## Gap, Scope, and Non-Goals

**Gap:** Reviewers must assemble proof across many files, docstring/file-description scope is unclear, and the audit can be updated manually without artifact validation.

**Scope:** Define/audit the deployable API surface, add missing docstrings to seven exact modules, build a row-2-to-46 artifact manifest, create a final rubric deliverable, refresh README/navigation, validate links/diagram conventions, and update the local audit truthfully.

**Non-goals:** Do not reorganize the repository, move all documentation, add docstrings to internal helpers, alter pipeline behavior, or rewrite evidence produced by earlier topics.

## Dependencies

- Every topic plan `01` through `10` is complete and its Completion Record names passing evidence.
- Topic 08 removed obsolete DataHub limitation language only after UI proof.
- Topic 10 created `deliverables/12_novel_ideas.md` and both novel-idea evidence packages.

## Declared Deployable API Surface

| Module | Required public symbols |
|---|---|
| `src/vina_bim_shop/generators/runner.py` | `GenerationResult`, `run_generation` |
| `src/vina_bim_shop/lakehouse/spark/runner.py` | `run_batch_pipeline` |
| `src/vina_bim_shop/flink/runtime.py` | `RuntimeSettings`, `load_runtime_settings` |
| `src/vina_bim_shop/orchestration/specs.py` | `DagSpec`, `dag_specs_by_id` |
| `src/vina_bim_shop/datahub_lineage/emitter.py` | `DataHubLineageEmitter` |
| `src/vina_bim_shop/pinot/bootstrap.py` | `apply_assets` |
| `src/vina_bim_shop/quality/policies.py` | `gate_outcome_for_layer` |

Each listed module requires a module docstring. Each listed class/function requires its own docstring explaining purpose, inputs/outputs, and important failure behavior without duplicating implementation details.

## Exact File Map

| Action | Path | Responsibility |
|---|---|---|
| Create | `scripts/qa/audit_public_documentation.py` | AST-audit the exact declared modules/symbols and write coverage evidence. |
| Create | `tests/unit/test_public_documentation_audit.py` | Test target declaration, module/symbol docstrings, output schema, and fail-closed behavior. |
| Modify | `src/vina_bim_shop/generators/runner.py` | Document the generator entrypoint API. |
| Modify | `src/vina_bim_shop/lakehouse/spark/runner.py` | Document the Spark pipeline entrypoint API. |
| Modify | `src/vina_bim_shop/flink/runtime.py` | Document the Flink runtime settings API. |
| Modify | `src/vina_bim_shop/orchestration/specs.py` | Document the DAG specification API. |
| Modify | `src/vina_bim_shop/datahub_lineage/emitter.py` | Document the DataHub emitter API. |
| Modify | `src/vina_bim_shop/pinot/bootstrap.py` | Document the Pinot bootstrap API. |
| Modify | `src/vina_bim_shop/quality/policies.py` | Document the quality-gate API. |
| Create | `scripts/qa/build_mini_coursework_rubric_manifest.py` | Validate row-ordered artifact mappings, hash files, and prevent unsupported status upgrades. |
| Create | `tests/unit/test_mini_coursework_rubric_manifest.py` | Test rows 2-46 exactly once, status gates, file existence, hashes, and ordering. |
| Create | `deliverables/13_mini_coursework_rubric_evidence.md` | Final row-ordered proof index with status, implementation, evidence, and limitations. |
| Modify | `README.md` | Add rubric navigation/checklist and clarify row-2 documentation/diagram contracts. |
| Modify | `deliverables/README.md` | Link deliverables 12 and 13 in order. |
| Modify | `evidence/README.md` | Link the final manifest and explain evidence freshness/hash policy. |
| Modify | `architecture/diagrams/README.md` | Declare primary deployment diagram, source/render relationship, arrow labels, and service order. |
| Modify | `tests/unit/test_root_readme_finalization.py` | Assert final README sections and links. |
| Modify | `tests/unit/test_architecture_docs_hygiene.py` | Assert final documentation wording and navigation. |
| Modify | `tests/unit/test_architecture_diagrams.py` | Assert deployable units, labeled arrows, direction/order, and primary diagram references. |
| Modify | `tests/unit/test_deliverables_documentation.py` | Require deliverables 12/13 and row-ordered proof sections. |
| Modify | `tmp/rubic-check/mini-coursework-rubric-audit.md` | Refresh statuses/gaps/actions only from validated final artifacts. |
| Create | `evidence/final_integration/public_documentation_coverage.json` | Declared targets, documented counts, coverage percentage, and file hashes. |
| Create | `evidence/final_integration/mini_coursework_rubric_manifest.json` | Rows 2-46, status, points, artifact paths/hashes, commands, and verification timestamp. |
| Test | `tests/unit/test_public_documentation_audit.py` | Public documentation contract. |
| Test | `tests/unit/test_mini_coursework_rubric_manifest.py` | Final artifact/status contract. |
| Test | `tests/unit/test_root_readme_finalization.py` | README regression. |
| Test | `tests/unit/test_architecture_docs_hygiene.py` | Documentation regression. |
| Test | `tests/unit/test_architecture_diagrams.py` | Diagram regression. |
| Test | `tests/unit/test_deliverables_documentation.py` | Deliverable regression. |
| Regenerate | `evidence/final_integration/public_documentation_coverage.json` | Re-run AST audit after any declared API documentation change. |
| Regenerate | `evidence/final_integration/mini_coursework_rubric_manifest.json` | Rebuild after all evidence/status changes. |
| Regenerate | `tmp/rubic-check/mini-coursework-rubric-audit.md` | Re-audit only after final manifest validation. |

## Interfaces and Final Evidence Contract

- `audit_public_documentation(repo_root: Path) -> dict[str, object]` audits exactly seven modules and ten declared symbols, requires all module/symbol docstrings, and reports `coverage_percent: 100.0` only when all targets pass.
- Coverage JSON contains target path, symbol name/type, documented flag, first docstring line, and source SHA-256; no source text beyond the first summary line is copied.
- `build_manifest(repo_root: Path) -> dict[str, object]` emits exactly rows `2..46` in order.
- Every row entry contains `row`, `points`, `status`, `implementation_paths`, `evidence_paths`, `evidence_sha256`, `verification_commands`, and `notes`.
- A row may be `Satisfied` only when all required paths exist, are nonempty, hashes are captured, and row-specific gates from its owning topic pass. Otherwise it remains `Partial` or `Missing` with an explicit reason.
- README links the final deliverable and manifest; the deliverable links every row to its owning implementation and evidence.

## Ordered Tasks

### Task 1: Define failing documentation and rubric-manifest tests

**Files:**
- Create: `tests/unit/test_public_documentation_audit.py`
- Create: `tests/unit/test_mini_coursework_rubric_manifest.py`
- Modify: the four existing documentation/diagram test files listed above

- [x] Assert the exact seven-module/ten-symbol declaration and 100% documented result.
- [x] Assert rows 2-46 appear exactly once and in order in the final manifest/deliverable.
- [x] Assert unsupported `Satisfied` status fails when a required artifact is absent, empty, or hashless.
- [x] Assert README contains a rubric checklist, primary deployment diagram link, deployable-unit/arrow conventions, public API policy, repository map, deployment order, and final manifest link.
- [x] Run `rtk uv run pytest tests/unit/test_public_documentation_audit.py tests/unit/test_mini_coursework_rubric_manifest.py tests/unit/test_root_readme_finalization.py tests/unit/test_architecture_docs_hygiene.py tests/unit/test_architecture_diagrams.py tests/unit/test_deliverables_documentation.py -q`.

Expected: new tests fail because audit/manifest scripts and final documentation do not exist.

### Task 2: Document and audit the declared public API

**Files:**
- Create: `scripts/qa/audit_public_documentation.py`
- Modify: the seven declared source modules
- Create: `evidence/final_integration/public_documentation_coverage.json`

- [x] Implement the exact AST target list and reject missing modules, symbols, or docstrings.
- [x] Add concise module/symbol docstrings only to declared targets that lack them.
- [x] Run `rtk uv run python scripts/qa/audit_public_documentation.py --output evidence/final_integration/public_documentation_coverage.json`.
- [x] Require ten of ten symbols and seven of seven modules documented with `coverage_percent: 100.0`.
- [x] Run `rtk uv run pytest tests/unit/test_public_documentation_audit.py -q`.

Expected: declared public API coverage is complete without unrelated helper churn.

### Task 3: Build the fail-closed rubric manifest

**Files:**
- Create: `scripts/qa/build_mini_coursework_rubric_manifest.py`
- Create: `evidence/final_integration/mini_coursework_rubric_manifest.json`

- [x] Encode row ownership/evidence requirements from `00-refactor-index.md` in exact row order.
- [x] Validate all topic run manifests, required screenshots, documentation coverage, and row-specific success fields.
- [x] Hash every referenced artifact and fail on missing, empty, duplicate, or out-of-workspace paths.
- [x] Run `rtk uv run python scripts/qa/build_mini_coursework_rubric_manifest.py --output evidence/final_integration/mini_coursework_rubric_manifest.json`.
- [x] Run `rtk uv run pytest tests/unit/test_mini_coursework_rubric_manifest.py -q`.

Expected: the manifest truthfully distinguishes satisfied rows from any remaining partial/missing evidence.

### Task 4: Finalize README, architecture guidance, and proof deliverable

**Files:**
- Create: `deliverables/13_mini_coursework_rubric_evidence.md`
- Modify: `README.md`, `deliverables/README.md`, `evidence/README.md`, and `architecture/diagrams/README.md`

- [x] Add a README rubric-status summary derived from the manifest, not manually invented totals.
- [x] Add a row-2 checklist linking business domain, primary diagram, repository map, deployment order, public API coverage, and evidence manifest.
- [x] Verify every Table of Contents anchor and local Markdown link resolves.
- [x] Document that diagram arrows identify source/target, data/control flow label, and left-to-right or top-to-bottom execution order.
- [x] Write rows 2-46 in order in the final deliverable with implementation/evidence links and honest remaining limitations.
- [x] Run the focused documentation/diagram tests.

Expected: a reviewer can navigate from README to any rubric row and its current proof in two links or fewer.

### Task 5: Refresh the local audit and run final regression

**Files:**
- Modify: `tmp/rubic-check/mini-coursework-rubric-audit.md`

- [x] Recalculate executive status counts from `mini_coursework_rubric_manifest.json`.
- [x] Update each row's status/evidence/gap/action without changing workbook order or claiming absent proof.
- [x] Change completed actions to maintenance guidance and preserve explicit residual risks.
- [x] Run `rtk uv run pytest tests/unit/test_public_documentation_audit.py tests/unit/test_mini_coursework_rubric_manifest.py tests/unit/test_root_readme_finalization.py tests/unit/test_architecture_docs_hygiene.py tests/unit/test_architecture_diagrams.py tests/unit/test_deliverables_documentation.py -q`.
- [x] Run `rtk uv run pytest -q`.
- [x] Run the manifest builder again and require byte-stable row ordering/status output apart from its verification timestamp.
- [x] Inspect `rtk git status --short` and verify every change belongs to a completed topic or was pre-existing user work.

Expected: final tests pass and README, deliverable, manifest, and local audit agree.

## Required Evidence

- 100% coverage JSON for the declared deployable API surface.
- Fail-closed rubric manifest covering rows 2-46 in exact order.
- Final rubric evidence deliverable and working README/navigation links.
- Passing documentation, diagram, manifest, and full regression tests.

## Definition of Done

- Row 2 has centralized evidence for domain, architecture, deployment order, repository/file descriptions, and declared API docstrings.
- Every rubric row maps to current implementation/evidence from README through the final deliverable.
- The manifest prevents unsupported status upgrades and the local audit matches it exactly.
- All links/anchors resolve, the primary diagram conventions are documented/tested, and focused/full suites pass.
- No unrelated source refactor or speculative documentation is introduced.

## Completion Record — 2026-07-13

**Status:** Row 2 and all rows 3-46 are validated as `Satisfied` by the generated manifest: 45 Satisfied, 0 Partial, 0 Missing (100 points).

### Generated evidence and integrity

- `rtk uv run python scripts/qa/audit_public_documentation.py --output evidence/final_integration/public_documentation_coverage.json` exited `0`: 7/7 declared modules and 10/10 declared symbols documented, `coverage_percent: 100.0`. SHA-256: `74c36c6261026129c1b4bd2c1482bf141560628f5befe1cce7e513287eb7bd3a`.
- `rtk uv run python scripts/qa/build_mini_coursework_rubric_manifest.py --output evidence/final_integration/mini_coursework_rubric_manifest.json` exited `0`: every row 2-46 is present once in order with hash-bound implementation/evidence paths and row-specific gates. SHA-256: `482fbc50298513f9a6b3829d9333e9319f75572aecbc08e29b0917ddb2d57da7`.
- `rtk uv run python scripts/qa/build_mini_coursework_rubric_manifest.py --verify --manifest evidence/final_integration/mini_coursework_rubric_manifest.json` exited `0`; changed, missing, empty, duplicate, outside-workspace, or forged artifact/status data fails verification.
- Final Markdown check resolved 178 local links across README, indexes, final deliverable, architecture, evidence, and script documentation; 0 were missing. `rtk git diff --check` exited `0`.

### Tests

- TDD red checks: new utility tests initially failed because the two scripts did not exist; documentation tests initially failed because final navigation/deliverable/manifest did not exist.
- `rtk uv run pytest tests/unit/test_public_documentation_audit.py tests/unit/test_mini_coursework_rubric_manifest.py -q` -> `10 passed` before the final Topic 01 multistage regression test was added; the final manifest-focused test file then passed `6` tests.
- `rtk uv run pytest tests/unit/test_root_readme_finalization.py tests/unit/test_architecture_docs_hygiene.py tests/unit/test_architecture_diagrams.py tests/unit/test_deliverables_documentation.py tests/unit/test_script_surface_documentation.py tests/unit/test_public_documentation_audit.py tests/unit/test_mini_coursework_rubric_manifest.py -q` -> `40 passed`.
- `rtk uv run pytest -q` -> `370 passed, 1 skipped in 93.15s`.

### Scope and residual limitations

- Added only the declared API docstrings, two QA scripts/tests, final evidence JSON, reviewer navigation, final deliverable, and associated documentation/audit controls. No upstream evidence was regenerated and no Docker/Compose service was started, so no runtime teardown was required.
- Topic 05 remains a controlled smoke-scale compaction/layout proof; DuckDB remains a local parity path; Pinot remains provisional while Spark Gold is canonical. These are explicit limits, not Partial rows, because the manifest validates the rubric claims actually made.
