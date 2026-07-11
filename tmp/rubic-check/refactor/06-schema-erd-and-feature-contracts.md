# Schema ERD and Feature Contracts Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make the generated ERD complete across Bronze, Silver, and Gold and align every Gold feature table to the exact `event_timestamp` plus `created` contract.

**Architecture:** Keep `architecture/diagrams/schema_design.puml` as the canonical all-zone ERD source and validate its table inventory against dbt model files before rendering. Rename only feature-output audit columns in both dbt and Spark SQL, then regenerate schema inventory, contracts, tests, and diagrams together.

**Tech Stack:** dbt-duckdb, DuckDB, Spark SQL, PlantUML, Python 3.12, pytest, YAML, PNG, CSV, and Markdown.

## Global Constraints

- Only `feat_customer_90d`, `feat_stream_60m`, and `feat_customer_unified` expose `created` instead of `created_ts`.
- Source events, Bronze, Silver, dimensions, facts, OBTs, and aggregates retain their existing `created_ts` fields.
- dbt and Spark feature schemas must match exactly after the rename.
- Use the generated `architecture/diagrams/schema_design.puml` and `evidence/02_schema_design/screenshots/schema_design.png` as the all-zone ERD evidence.
- The ERD source must enumerate every dbt model under Bronze, Silver, and Gold; grouped nodes are allowed only when every table name remains readable.
- SCD2 documentation must distinguish compatible columns from full historical version generation.
- Do not stage or commit unless explicitly requested. Preserve unrelated changes and use `rtk` for shell commands.

---

## Rubric Coverage

| Row | Requirement | Points | Current status | Effort | Value added |
|---:|---|---:|---|---|---|
| 40 | Visualize tables across all zones. | 2 | Partial | S | High |
| 41 | Dimension tables include `valid_from_ts`, `valid_to_ts`, and `is_current`. | 1 | Satisfied, explanation unclear | XS | Medium |
| 42 | Feature tables include `event_timestamp` and `created`. | 1 | Partial | S | Medium |
| 43 | Show relationships between dimension and fact tables. | 2 | Satisfied, proof dispersed | XS | Medium |
| 44 | Show Bronze/Silver/Gold naming conventions. | 2 | Satisfied, proof can be clearer | XS | Low |

## Current Implementation and Evidence

- `architecture/diagrams/schema_design.puml` already groups Bronze, Silver, Gold, realtime serving, and local analytics.
- `scripts/qa/generate_section02_evidence.py` renders that source and produces schema inventory, dbt build/test summaries, and screenshots.
- `architecture/diagrams/erd/physical_gold_model.puml` and `.png` show Gold dimension/fact relationships.
- dbt feature models and `src/vina_bim_shop/lakehouse/spark/sql.py` currently expose `created_ts` on feature tables.
- SCD2-compatible columns exist in customer, product, and seller dimensions, while the current implementation remains current-row oriented.

## Gap, Scope, and Non-Goals

**Gap:** All-zone coverage is not automatically checked, row 42 uses the wrong exact output name, and row-specific schema proof is spread across files.

**Scope:** Add table-inventory tests, update the ERD source as required, rename three feature outputs in dbt and Spark, update contracts/docs, regenerate Section 02 evidence, and verify parity.

**Non-goals:** Do not rename upstream `created_ts`, implement full historical SCD2 processing, redesign the Gold model, add a second ERD source, or change feature grains/metrics.

## Dependencies

- Topic 02 provides fresh raw inputs for dbt evidence regeneration.
- Topic 03 must be complete so Spark regression expectations are known.
- Topic 07 must consume the final `created` contract when defining DP3 validation.

## Exact File Map

| Action | Path | Responsibility |
|---|---|---|
| Modify | `infra/analytics/dbt/models/gold/feat_customer_90d.sql` | Alias final feature audit timestamp as `created`. |
| Modify | `infra/analytics/dbt/models/gold/feat_stream_60m.sql` | Alias final feature audit timestamp as `created`. |
| Modify | `infra/analytics/dbt/models/gold/feat_customer_unified.sql` | Read upstream feature `created` fields and expose unified `created`. |
| Modify | `infra/analytics/dbt/models/gold/_features.yml` | Enforce exact `created` timestamp contracts on all three models. |
| Modify | `src/vina_bim_shop/lakehouse/spark/sql.py` | Match the three Spark feature queries to the dbt column names. |
| Modify | `architecture/diagrams/schema_design.puml` | Ensure every Bronze/Silver/Gold dbt table is represented and label the feature contract. |
| Modify | `deliverables/02_schema_design.md` | Add ordered row-40-to-44 proof and accurate SCD2 behavior. |
| Modify | `scripts/qa/generate_section02_evidence.py` | Validate all-zone model coverage and record rendered source/hash metadata. |
| Modify | `tests/unit/test_section02_schema_design.py` | Assert ERD inventory, relationships, naming, SCD2 columns, and feature names. |
| Modify | `tests/unit/test_section02_evidence_generation.py` | Assert generated ERD metadata and schema inventory contract. |
| Modify | `tests/unit/test_spark_batch_runtime.py` | Assert Spark feature SQL exposes `created` and does not expose feature `created_ts`. |
| Regenerate | `evidence/02_schema_design/screenshots/schema_design.png` | Render the complete all-zone ERD. |
| Regenerate | `evidence/02_schema_design/schema_inventory.csv` | Record final columns across all zones. |
| Regenerate | `evidence/02_schema_design/dbt_catalog_summary.csv` | Record final dbt feature contracts. |
| Regenerate | `evidence/02_schema_design/dbt_model_results.csv` | Record successful model builds. |
| Regenerate | `evidence/02_schema_design/dbt_test_results.csv` | Record successful feature and relationship tests. |
| Regenerate | `evidence/02_schema_design/dbt_build_report.md` | Summarize final schema results. |
| Regenerate | `evidence/02_schema_design/run_manifest.json` | Include ERD source path/hash and artifact inventory. |
| Test | `tests/unit/test_section02_schema_design.py` | Schema and diagram contract. |
| Test | `tests/unit/test_section02_evidence_generation.py` | Evidence generator contract. |
| Test | `tests/unit/test_spark_batch_runtime.py` | Canonical Spark schema regression. |
| Test | `infra/analytics/dbt/models/gold/_features.yml` | dbt-enforced model contracts executed by `dbt build`. |

## Interfaces and Schema Contract

- Every Gold feature model has exactly one audit-output column named `created` with timestamp type and one point-in-time column named `event_timestamp`.
- `feat_customer_90d` computes `max(fo.created_ts) as created`.
- `feat_stream_60m` computes `max(created_ts) as created` from Silver input.
- `feat_customer_unified` computes `greatest(c.created, coalesce(s.created, c.created)) as created`.
- Spark SQL uses the same expressions and final aliases as dbt.
- `all_zone_model_names(repo_root) -> dict[str, set[str]]` derives model names from `infra/analytics/dbt/models/{bronze,silver,gold}/*.sql`; evidence generation fails if any name is absent from the PlantUML source.
- `run_manifest.json` records `schema_design_source`, `schema_design_sha256`, `schema_design_render_mode`, and model counts for each zone.

## Ordered Tasks

### Task 1: Write failing feature and ERD contract tests

**Files:**
- Modify: `tests/unit/test_section02_schema_design.py`
- Modify: `tests/unit/test_section02_evidence_generation.py`
- Modify: `tests/unit/test_spark_batch_runtime.py`

- [ ] Derive Bronze/Silver/Gold model names from dbt SQL filenames and assert every name appears in `schema_design.puml`.
- [ ] Assert each dbt feature SQL and Spark feature query exposes `event_timestamp` and final alias `created`.
- [ ] Assert `_features.yml` declares `created` and does not declare `created_ts` for feature models.
- [ ] Assert representative non-feature models still contain `created_ts` to guard against a global rename.
- [ ] Assert the evidence manifest includes source/hash/model-count metadata.
- [ ] Run `rtk uv run pytest tests/unit/test_section02_schema_design.py tests/unit/test_section02_evidence_generation.py tests/unit/test_spark_batch_runtime.py -q`.

Expected: new assertions fail against the existing feature names and any missing ERD inventory entries.

### Task 2: Rename only the Gold feature outputs

**Files:**
- Modify: the three `infra/analytics/dbt/models/gold/feat_*.sql` files listed above
- Modify: `infra/analytics/dbt/models/gold/_features.yml`
- Modify: `src/vina_bim_shop/lakehouse/spark/sql.py`

- [ ] Apply the exact three dbt expressions from the interface contract.
- [ ] Update downstream unified-feature references from `.created_ts` to `.created`.
- [ ] Change only the three feature query blocks in Spark SQL.
- [ ] Search with `rtk rg -n "created_ts|created" infra/analytics/dbt/models/gold/feat_*.sql infra/analytics/dbt/models/gold/_features.yml src/vina_bim_shop/lakehouse/spark/sql.py` and inspect every feature occurrence.
- [ ] Re-run the focused unit tests.

Expected: dbt and Spark feature contracts agree while upstream audit fields remain unchanged.

### Task 3: Complete and validate the generated all-zone ERD

**Files:**
- Modify: `architecture/diagrams/schema_design.puml`
- Modify: `scripts/qa/generate_section02_evidence.py`

- [ ] Add any model names identified by the failing inventory test, preserving readable Bronze/Silver/Gold groups.
- [ ] Label all three feature tables with `event_timestamp` and `created` in the Gold serving node or an adjacent note.
- [ ] Implement model-name derivation and fail evidence generation before rendering when coverage is incomplete.
- [ ] Record source hash and zone counts in the manifest.
- [ ] Run `rtk uv run pytest tests/unit/test_section02_schema_design.py tests/unit/test_section02_evidence_generation.py -q`.

Expected: the PlantUML source has complete model coverage and deterministic manifest metadata.

### Task 4: Regenerate dbt and schema evidence

**Files:**
- Regenerate: `evidence/02_schema_design/`

- [ ] Generate fresh smoke input with `rtk uv run python scripts/generate/run_generator.py --scale smoke --mode full --clean`.
- [ ] Run `rtk uv run dbt build --project-dir infra/analytics/dbt --profiles-dir infra/analytics/dbt` and require all models/tests to pass.
- [ ] Run `rtk uv run python scripts/qa/generate_section02_evidence.py`.
- [ ] Inspect `schema_inventory.csv` and `dbt_catalog_summary.csv`; each feature table must contain `event_timestamp` and `created`, not feature `created_ts`.
- [ ] Open `evidence/02_schema_design/screenshots/schema_design.png` and verify every zone and table group is readable.

Expected: generated evidence reflects the exact new feature schema and a complete all-zone ERD.

### Task 5: Document rows 40-44 and run regressions

**Files:**
- Modify: `deliverables/02_schema_design.md`

- [ ] Add five ordered sections for Rows 40-44 with direct source, generated evidence, and test links.
- [ ] State that `valid_from_ts`, `valid_to_ts`, and `is_current` are SCD2-compatible columns while current builds do not generate a full historical version chain.
- [ ] Embed the generated all-zone ERD for Row 40 and the physical Gold relationship image for Row 43.
- [ ] Add a naming table: Bronze `raw_`, Silver `stg_`, Gold `dim_`, `fact_`, `obt_`, `agg_`, `feat_`, and `bridge_`.
- [ ] Run `rtk uv run pytest tests/unit/test_section02_schema_design.py tests/unit/test_section02_evidence_generation.py tests/unit/test_spark_batch_runtime.py tests/unit/test_deliverables_documentation.py -q`.
- [ ] Run `rtk uv run pytest -q` and inspect `rtk git status --short`.

Expected: rows 40-44 are reviewable in order and all schema regressions pass.

## Required Evidence

- Complete generated ERD source and rendered PNG.
- dbt catalog/schema inventory proving exact feature columns.
- dbt build and test outputs.
- Gold relationship image and naming-convention table.
- Manifest with source hash and zone model counts.

## Definition of Done

- Every dbt model name in Bronze, Silver, and Gold appears in the generated ERD source.
- All three Gold feature tables expose exact `event_timestamp` and `created` columns in both dbt and Spark.
- No upstream or non-feature `created_ts` column is unintentionally renamed.
- SCD2 behavior, relationships, and naming conventions are documented accurately.
- Focused and full suites pass and regenerated evidence is readable.

## Completion Record

Completed on 2026-07-11 in the current repository folder on branch `feature/finalize-edai1`. No files were staged or committed.

### Renamed Feature Outputs

- `feat_customer_90d`: `max(fo.created_ts)` now aliases to the output column `created` in dbt and Spark.
- `feat_stream_60m`: `max(created_ts)` now aliases to the output column `created` in dbt and Spark.
- `feat_customer_unified`: combines `c.created` and `s.created` into the output column `created` in dbt and Spark.
- All three dbt contracts expose `event_timestamp` plus `created: timestamp`. Source events, Bronze, Silver, dimensions, facts, OBTs, and aggregates were not renamed.

### ERD And Generated Evidence

- `architecture/diagrams/schema_design.puml` remains the generated all-zone ERD source. The evidence generator now derives model names from the dbt Bronze, Silver, and Gold directories and aborts when a name is missing from that source.
- The regenerated manifest records source `architecture/diagrams/schema_design.puml`, SHA-256 `fce33138edf07539c560a4c10cd0f01f560fc61353510ea88c7e02445b88861d`, PlantUML Server render mode, and model counts Bronze `16`, Silver `14`, Gold `22`.
- `physical_gold_model.puml`, `physical_gold_model.png`, and `gold_layer_ERD.dbml` now show `created` for exactly the three feature tables; their relationship topology is unchanged.
- Regenerated Section 02 artifacts include the all-zone PNG, Gold inventory PNG, dbt catalog, schema inventory, build/test CSVs, row counts, report, and manifest. The generated catalog and schema inventory show `event_timestamp` and `created`, with no feature `created_ts` column.

### Commands And Results

- Initial Topic 06 contract run: `rtk uv run pytest tests/unit/test_section02_schema_design.py tests/unit/test_section02_evidence_generation.py tests/unit/test_spark_batch_runtime.py -q` -> `8 failed, 29 passed`; failures were the intended old feature aliases, absent ERD metadata, stale evidence, and absent local DuckDB artifact.
- After scoped code/docs changes and before regeneration, the same command -> `3 failed, 35 passed`; only stale evidence and the absent DuckDB artifact remained.
- `rtk uv run python scripts/generate/run_generator.py --scale smoke --mode full --clean --evidence-root tmp/topic06_generator_evidence` -> exit `0`; fresh raw inputs were produced without rewriting committed Topic 02 evidence.
- `rtk uv run python scripts/qa/generate_section02_evidence.py` -> exit `0`; its embedded dbt build produced `52` successful models and `66` passing tests, then dbt docs generation completed successfully.
- The physical relationship PNG was re-rendered from `physical_gold_model.puml` with the existing PlantUML renderer.
- Focused regression: `rtk uv run pytest tests/unit/test_section02_schema_design.py tests/unit/test_section02_evidence_generation.py tests/unit/test_spark_batch_runtime.py tests/unit/test_deliverables_documentation.py -q` -> `43 passed in 2.03s`.
- Full regression: `rtk uv run pytest -q` -> `304 passed, 1 skipped, 1 failed in 99.71s`.
- `rtk docker compose ps --status running` returned no running services. This session did not start Docker or Compose services, so none required stopping; the generator/dbt/evidence runtime slot was released after the completed commands.

### Limitations

- The only full-suite failure is outside Topic 06: `tests/unit/test_script_surface_documentation.py::test_scripts_readme_documents_every_tracked_script` expects the already-tracked `scripts/kafka/capture_connect_image_optimization.py` entry in `scripts/README.md`. It was intentionally left for the owning documentation topic.
