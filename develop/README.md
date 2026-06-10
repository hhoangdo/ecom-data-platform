# Develop-Only Artifacts

This directory is the home for artifacts that are part of the Vina Bim Shop
platform effort but are intentionally **not** part of the official `main`
branch evidence.

The convention matches the existing
`tests/unit/test_script_surface_documentation.py` rule: helper scripts that
take screenshots, drive Playwright, or capture UI captures are kept on the
`develop` branch only. Official machine evidence is committed under
`evidence/<area>/` on `main`.

## What Lives Here

| Sub-path | Source | Reason |
| --- | --- | --- |
| `evidence/08_airflow_gx/screenshots/README.md` | moved from `evidence/08_airflow_gx/screenshots/README.md` | Manual capture note for the Airflow DAG grid and GX Data Docs UI. Not produced by the official `local_evidence_build` DAG. |
| `evidence/08_airflow_gx/runs/hourly_batch_lakehouse/manual__2026-04-26T01_00_00_00_00/spark_batch/screenshots/` | moved from `evidence/08_airflow_gx/runs/hourly_batch_lakehouse/manual__2026-04-26T01_00_00_00_00/spark_batch/screenshots/` | 70-byte placeholder PNGs and a capture note for the Spark master/history UIs. Not produced by the current `hourly_batch_lakehouse` DAG. |

## Why

- The official runtime path in `src/vina_bim_shop/orchestration/runtime.py`
  contains no screenshot or browser helpers. This is enforced by
  `tests/unit/test_script_surface_documentation.py::test_official_machine_evidence_has_no_browser_or_screenshot_helpers`.
- The previous `evidence/.../screenshots/README.md` placeholders were static
  capture notes, not generated artifacts. Keeping them on `main` risked
  blurring the line between official machine evidence and manual review
  capture.
- Moving them here keeps the official evidence tree small, machine-readable,
  and reproducible; the placeholders remain available for developers working
  on the `develop` branch.

## Promoting Back To `main`

If a screenshot capture helper graduates to official evidence, it must:

1. Be produced by a runtime function, not a manual note.
2. Add the artifact to the DAG manifest's `artifacts[]` list explicitly.
3. Extend `OFFICIAL_MACHINE_EVIDENCE_FILES` in
   `tests/unit/test_script_surface_documentation.py` only if a new file in
   `src/` is allowed to capture browser content.
4. Be reviewed against the no-browser / no-screenshot-helpers contract.
