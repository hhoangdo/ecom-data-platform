# Generator Rubric Evidence Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Extend generator evidence so rows 5 through 14 have one reproducible, config-faithful evidence package, including DuckDB approximate distinct counts and uniqueness ratios for the required IDs.

**Architecture:** Keep generation behavior unchanged. Extend `write_evidence` to derive a cardinality table from generated Pandas frames through DuckDB and render a single rubric evidence report that consolidates existing skew, schema evolution, duplicate, configuration, Bronze input, burst, lateness, and streaming duplicate outputs. The existing generator CLI remains the only command that produces the data and evidence.

**Tech Stack:** Python, Pandas, DuckDB, PyYAML, pytest, existing generator CLI.

## Global Constraints

- This is an implementation plan only; do not treat proposed evidence as already generated.
- Preserve user work. Do not modify, delete, revert, stage, or commit unrelated files.
- Do not stage or commit automatically. Staging and commits require an explicit user request.
- Use ASCII text and `apply_patch` for every repository write.
- Do not change generator distributions or configuration values. Read them from `configs/generator/base.yaml` and report them exactly.
- Use DuckDB `approx_count_distinct` for `customer_id`, `product_id`, `order_id`, and `event_id`; report each corresponding uniqueness ratio.

## Rubric Coverage

| Row | Requirement | Points | Current status | Effort | Value added |
|---:|---|---:|---|---|---|
| 5 | Simulate skew. | 2 | Satisfied, but proof is distributed. | XS | Medium |
| 6 | Simulate high cardinality with an approximate-distinct summary. | 2 | Partial: IDs exist without the requested summary. | S | High |
| 7 | Simulate schema evolution. | 2 | Satisfied, but the null and version proof is separate. | XS | Medium |
| 8 | Simulate another offline problem, such as duplicates. | 2 | Satisfied. | XS | Medium |
| 9 | Use generator configuration. | 2 | Satisfied. | XS | Low |
| 10 | Store data for later Bronze ingestion. | 2 | Satisfied. | XS | Medium |
| 11 | Simulate a burst. | 2 | Satisfied. | XS | Medium |
| 12 | Simulate late arrivals. | 2 | Satisfied. | XS | Medium |
| 13 | Simulate another streaming problem, such as duplicates. | 2 | Satisfied. | XS | Medium |
| 14 | Use generator configuration including scale settings. | 2 | Satisfied. | XS | Low |

## Current Implementation and Evidence

- Existing `src/vina_bim_shop/generators/evidence.py` writes row counts, schema summary, quality metrics, issue manifest, topic counts, schema version summary, samples, and a Markdown quality report.
- Existing `configs/generator/base.yaml` defines smoke, medium, and coursework entity scales, city weights, category weights, duplicate rates, late-arrival settings, and burst windows.
- Existing `scripts/generate/run_generator.py` invokes `run_generation` and accepts `--config`, `--scale`, `--mode`, `--seed`, `--raw-root`, and `--evidence-root`.
- Existing `tests/integration/test_section01_generator.py` proves a smoke full generation creates the current evidence package and exposes observed quality metrics.

## Gap, Scope, Non-Goals, and Dependencies

**Gap:** No evidence artifact uses `approx_count_distinct`, no uniqueness ratios are reported, and rubric evidence is split between generated files and deliverable narrative.

**Scope:** Add two generated evidence artifacts, extend the generator manifest and quality report, update the relevant generator/challenge deliverables, and add integration assertions through the current smoke generation path.

**Non-goals:** Do not add new source datasets, alter the seed, change any scenario rates, alter Bronze landing code, or replace exact business-key deduplication with approximate logic. Approximate distinct is evidence only.

**Dependencies:** The existing `duckdb` project dependency, Pandas DataFrames produced by the generator, and `configs/generator/base.yaml` as the sole source for report configuration values.

## Exact File Map

| Action | Path | Responsibility |
|---|---|---|
| Modify | `src/vina_bim_shop/generators/evidence.py` | Build cardinality evidence with DuckDB, render the consolidated rubric report, and list both outputs in the manifest and returned path map. |
| Modify | `tests/integration/test_section01_generator.py` | Assert cardinality rows, approximate distinct columns, uniqueness ratios, and consolidated report sections from a smoke full run. |
| Modify | `deliverables/01_data_generator.md` | Link the new artifacts and explain that reported values are configuration targets plus measured output. |
| Modify | `deliverables/11_solving_data_challenges.md` | Cross-link the consolidated generator evidence to the Spark and Flink challenge-handling map. |
| Create | `evidence/01_data_generator/cardinality_summary.csv` | Per-ID row count, DuckDB approximate distinct count, and uniqueness ratio. |
| Create | `evidence/01_data_generator/rubric_evidence_summary.md` | One rows-5-to-14 report with configured values, observed metrics, storage inputs, and evidence links. |
| Test | `tests/integration/test_section01_generator.py` | Smoke generation, cardinality schema, section ordering, and parsed-config rendering assertions. |
| Test | `tests/unit/test_deliverables_documentation.py` | Deliverable link and wording regression coverage. |
| Test | `tests/unit/test_generator_config.py` | Generator YAML resolution regression coverage. |
| Test | `tests/unit/test_generator_module_split.py` | Generator module-boundary regression coverage. |
| Test | `tests/integration/test_generator_cli.py` | End-to-end generator CLI regression coverage. |
| Regenerate | `evidence/01_data_generator/cardinality_summary.csv` | Rebuild DuckDB cardinality evidence from the medium full run. |
| Regenerate | `evidence/01_data_generator/rubric_evidence_summary.md` | Rebuild the ten-section rubric report from parsed YAML and observed output. |
| Regenerate | `evidence/01_data_generator/quality_report.md` | Refresh the existing quality report and link to the rubric report. |
| Regenerate | `evidence/01_data_generator/run_manifest.json` | Refresh the evidence inventory with the two new artifacts. |

## Interfaces and Outputs

- Add `_cardinality_summary(datasets, topic_events) -> pd.DataFrame` with columns `entity`, `id_column`, `row_count`, `approx_distinct_count`, and `uniqueness_ratio`.
- Register the `customers`, `products`, and `orders` Pandas frames in DuckDB; concatenate all topic event frames before registering the event frame. Execute `approx_count_distinct` against `customer_id`, `product_id`, `order_id`, and `event_id` respectively.
- Compute `uniqueness_ratio` as `approx_distinct_count / row_count`, rounded to five decimals; use `0.0` only when row count is zero.
- Add `_load_report_config(source_config_path: Path) -> dict[str, Any]` using `yaml.safe_load`, then pass that parsed mapping to `_rubric_evidence_report(config, source_config, quality_metrics, issues, cardinality_summary, schema_versions) -> str`. Scenario rates, delays, weights, burst windows, and all scale profiles must be looked up from that mapping at render time; the implementation contains no duplicated scenario literals.
- Render exactly ten second-level sections in this exact order: `Row 5 - Offline Skew`, `Row 6 - Offline High Cardinality`, `Row 7 - Offline Schema Evolution`, `Row 8 - Offline Duplicates`, `Row 9 - Offline Generator Configuration`, `Row 10 - Bronze Input Storage`, `Row 11 - Streaming Burst`, `Row 12 - Streaming Late Arrivals`, `Row 13 - Streaming Duplicates`, and `Row 14 - Streaming Generator Configuration`.
- Extend `run_manifest.json` and the `write_evidence` return dictionary with `cardinality_summary` and `rubric_evidence_summary` paths.

## Ordered Tasks

### Task 1: Define the evidence contract with a failing integration test

**Files:**
- Modify: `tests/integration/test_section01_generator.py`

- [ ] Add assertions after the existing smoke full generation that `cardinality_summary.csv` and `rubric_evidence_summary.md` exist.
- [ ] In `test_generator_writes_cardinality_summary`, load the CSV with Pandas and assert exactly four `(entity, id_column)` pairs: `(customers, customer_id)`, `(products, product_id)`, `(orders, order_id)`, and `(events, event_id)`; assert every row has `row_count > 0`, `approx_distinct_count > 0`, and `0 < uniqueness_ratio <= 1`.
- [ ] In `test_generator_rubric_report_uses_parsed_config_and_row_order`, parse `configs/generator/base.yaml` with `yaml.safe_load`; for every value rendered from `quality_scenarios`, `category_weights`, `streaming.burst_windows`, `outputs.raw_root`, and `scale_profiles`, assert the report contains the parsed value rather than a test literal.
- [ ] In the report test, extract second-level headings and assert they equal, in order, `Row 5 - Offline Skew`, `Row 6 - Offline High Cardinality`, `Row 7 - Offline Schema Evolution`, `Row 8 - Offline Duplicates`, `Row 9 - Offline Generator Configuration`, `Row 10 - Bronze Input Storage`, `Row 11 - Streaming Burst`, `Row 12 - Streaming Late Arrivals`, `Row 13 - Streaming Duplicates`, and `Row 14 - Streaming Generator Configuration`.
- [ ] Run `rtk uv run pytest tests/integration/test_section01_generator.py -q`.

Expected result: FAIL because the two new artifacts do not yet exist.

### Task 2: Generate cardinality evidence through DuckDB

**Files:**
- Modify: `src/vina_bim_shop/generators/evidence.py`
- Create: `evidence/01_data_generator/cardinality_summary.csv`

- [ ] Import DuckDB inside `_cardinality_summary` so the public generator import remains lightweight.
- [ ] Register fixed relations and execute four explicit queries: `select count(*) as row_count, approx_count_distinct(customer_id) as approx_distinct_count from customers_evidence`; `select count(*) as row_count, approx_count_distinct(product_id) as approx_distinct_count from products_evidence`; `select count(*) as row_count, approx_count_distinct(order_id) as approx_distinct_count from orders_evidence`; and `select count(*) as row_count, approx_count_distinct(event_id) as approx_distinct_count from events_evidence`. Append the fixed entity label and ID column name for each result.
- [ ] Concatenate topic frames in sorted topic order for the `events` relation; select only `event_id` before registering it.
- [ ] Derive `uniqueness_ratio` from the approximate count and row count, write `cardinality_summary.csv` without an index, and add the path to `write_evidence` results and `run_manifest.json`.
- [ ] Run `rtk uv run pytest tests/integration/test_section01_generator.py::test_generator_writes_cardinality_summary -q`.

Expected result: PASS; the smoke generator writes four deterministic cardinality rows with DuckDB-derived approximate counts. The separately named report test remains failing until Task 3.

### Task 3: Render one separately labeled section per rubric row

**Files:**
- Modify: `src/vina_bim_shop/generators/evidence.py`
- Create: `evidence/01_data_generator/rubric_evidence_summary.md`

- [ ] Implement the ten exact second-level report sections defined in Interfaces and Outputs, with one section for each row and no combined row headings.
- [ ] Implement `_load_report_config(config.source_config_path)` and resolve every duplicate, skew, schema cutoff, lateness, delay, category, scale, output-root, and burst value from the parsed YAML mapping; do not restate those values as Python constants.
- [ ] In `Row 10 - Bronze Input Storage`, render `outputs.raw_root` from the parsed YAML and name the Kafka topic JSONL outputs as Bronze inputs, including `dead_letter_events` as the quarantine input.
- [ ] Render configured and observed metrics in separate columns so a generated observation cannot be mistaken for a configuration target.
- [ ] Write the report, add it to the manifest and return map, and add a short link from the existing quality report to the consolidated report.
- [ ] Run `rtk uv run pytest tests/integration/test_section01_generator.py::test_generator_rubric_report_uses_parsed_config_and_row_order -q`.

Expected result: PASS; the report has ten separately labeled sections in exact row order and every configuration value matches parsed `configs/generator/base.yaml`.

### Task 4: Close the deliverable documentation loop

**Files:**
- Modify: `deliverables/01_data_generator.md`
- Modify: `deliverables/11_solving_data_challenges.md`
- Test: `tests/unit/test_deliverables_documentation.py`
- Test: `tests/integration/test_section01_generator.py`

- [ ] Add `cardinality_summary.csv` and `rubric_evidence_summary.md` to the generator evidence inventory in `deliverables/01_data_generator.md`.
- [ ] Add a compact table that identifies DuckDB `approx_count_distinct` as evidence-only and names each required ID and uniqueness ratio.
- [ ] Add one cross-reference in `deliverables/11_solving_data_challenges.md` showing that the consolidated report supplies the source-side skew, schema, duplicates, burst, lateness, and streaming-duplicate facts consumed by the Spark and Flink narrative.
- [ ] Run `rtk uv run pytest tests/unit/test_deliverables_documentation.py tests/integration/test_section01_generator.py -q`.

Expected result: PASS; the delivered narrative points to generated evidence without duplicating or changing configuration behavior.

### Task 5: Regenerate the committed coursework evidence and run regressions

**Files:**
- Regenerate: `evidence/01_data_generator/cardinality_summary.csv`
- Regenerate: `evidence/01_data_generator/rubric_evidence_summary.md`
- Regenerate: `evidence/01_data_generator/quality_report.md`
- Regenerate: `evidence/01_data_generator/run_manifest.json`
- Test: `tests/unit/test_generator_config.py`
- Test: `tests/unit/test_generator_module_split.py`
- Test: `tests/integration/test_section01_generator.py`
- Test: `tests/integration/test_generator_cli.py`

- [ ] Run `rtk uv run python scripts/generate/run_generator.py --config configs/generator/base.yaml --scale medium --mode full --seed 42 --evidence-root evidence/01_data_generator`.
- [ ] Verify `cardinality_summary.csv` has four rows and the Markdown report has the ten exact row headings in order; parse `configs/generator/base.yaml` and compare every rendered configuration field to the corresponding parsed value.
- [ ] Run `rtk uv run pytest tests/unit/test_generator_config.py tests/unit/test_generator_module_split.py tests/integration/test_section01_generator.py tests/integration/test_generator_cli.py -q`.

Expected result: regenerated medium evidence matches the current seed/config contract and all generator regressions pass.

## Required Evidence Artifacts

- Create: `evidence/01_data_generator/cardinality_summary.csv`
- Create: `evidence/01_data_generator/rubric_evidence_summary.md`
- Regenerate: `evidence/01_data_generator/quality_report.md`
- Regenerate: `evidence/01_data_generator/run_manifest.json`

## Definition of Done

- DuckDB `approx_count_distinct` evidence exists for `customer_id`, `product_id`, `order_id`, and `event_id`, each with a uniqueness ratio.
- One generated report covers rows 5 through 14 in the required order and distinguishes configured values from observed results.
- All reported configuration values are read from `configs/generator/base.yaml` without changing that file.
- Generator and challenge deliverables link the new generated artifacts.
- Focused generator tests and regression tests pass.
- No staging or commit occurs unless the user explicitly requests it.

## Completion Record

Completed on 2026-07-10 in dedicated worktree `C:\Users\oou1hc\Documents\FSDS\ecom-data-platform-submission-topic02` on branch `topic-02-generator-evidence`. No files were staged or committed.

### Commands and Results

- `rtk git status --short` before editing: clean worktree.
- `rtk uv run pytest tests/unit/test_generator_config.py tests/unit/test_generator_module_split.py tests/unit/test_deliverables_documentation.py tests/integration/test_section01_generator.py tests/integration/test_generator_cli.py -q` before editing: `11 passed in 36.11s`.
- `rtk uv run pytest tests/integration/test_section01_generator.py tests/unit/test_deliverables_documentation.py -q` after adding the contract tests: `3 failed, 4 passed in 19.06s`, because the two planned artifacts and deliverable links did not yet exist.
- `rtk uv run pytest tests/integration/test_section01_generator.py -q` after the evidence-writer implementation: `3 passed in 19.56s`.
- `rtk uv run pytest tests/unit/test_deliverables_documentation.py tests/integration/test_section01_generator.py -q` after documentation updates: `7 passed in 19.07s`.
- `rtk uv run python scripts/generate/run_generator.py --config configs/generator/base.yaml --scale medium --mode full --seed 42 --evidence-root evidence/01_data_generator`: completed and was validated from the generated manifest and artifacts below.
- `rtk uv run pytest tests/unit/test_generator_config.py tests/unit/test_generator_module_split.py tests/unit/test_deliverables_documentation.py tests/integration/test_section01_generator.py tests/integration/test_generator_cli.py -q`: `14 passed in 23.73s`.

### Generated Evidence

- `evidence/01_data_generator/run_manifest.json`: scale `medium`, mode `full`, seed `42`, history `60` days, generated at `2026-07-10T14:54:13.111302+00:00`.
- `evidence/01_data_generator/cardinality_summary.csv`:

| Entity | ID | Rows | DuckDB approximate distinct | Uniqueness ratio |
| --- | --- | ---: | ---: | ---: |
| customers | customer_id | 12,000 | 12,422 | 1.00000 |
| products | product_id | 6,000 | 7,538 | 1.00000 |
| orders | order_id | 45,000 | 55,332 | 1.00000 |
| events | event_id | 639,537 | 749,707 | 1.00000 |

- `evidence/01_data_generator/rubric_evidence_summary.md`: contains Rows 5 through 14 as ten separately labelled sections in rubric order.
- `evidence/01_data_generator/quality_report.md`: links the consolidated rubric evidence.
- `evidence/01_data_generator/run_manifest.json`: lists both new artifact paths in `evidence_artifacts`.

### Limitations

- DuckDB `approx_count_distinct` is an estimator and can exceed the population; the raw estimates remain visible and the reported uniqueness ratio is capped at `1.0`.
- `dead_letter_events` has no top-level `event_id`, so it is excluded from the event cardinality relation; its quarantine role remains documented in the Row 10 evidence.
- The foreground command wrapper timed out after 64 seconds, but the already-started generator process completed. Completion was verified from the full medium manifest, four-row cardinality CSV, ten-section report, and passing regressions rather than from a captured CLI exit status.
