"""``local_evidence_build`` DAG runtime.

Collects the official machine evidence for ADR 06:

- The Airflow webserver ``/health`` response.
- The list of every committed ``run_manifest.json`` under the ADR 06 runs root.
- A pointer to the GX Data Docs static index.

The manifest is written per-run and mirrored to the top-level
``evidence/08_airflow_gx/`` directory so a reviewer can read it without
walking the run tree.
"""
from __future__ import annotations

from typing import Any

from .paths import (
    ADR06_EVIDENCE_ROOT,
    DOCS_ROOT,
    REPO_ROOT,
    RUNS_ROOT,
    _utc_now,
    _write_json,
    build_run_root,
)
from .subprocess_helpers import _get_json


def run_local_evidence_build(*, run_id: str) -> dict[str, Any]:
    run_root = build_run_root("local_evidence_build", run_id)
    airflow_health = _get_json("http://airflow-webserver:8080/health")
    docs_index = DOCS_ROOT / "index.html"
    latest_manifests = sorted(
        str(path.relative_to(REPO_ROOT)).replace("\\", "/") for path in RUNS_ROOT.rglob("run_manifest.json")
    )
    manifest = {
        "captured_at": _utc_now(),
        "airflow_health": airflow_health,
        "docs_index_exists": docs_index.is_file(),
        "latest_run_manifests": latest_manifests,
        "artifacts": [
            "gx_data_docs/index.html",
        ],
    }
    _write_json(run_root / "run_manifest.json", manifest)
    _write_json(ADR06_EVIDENCE_ROOT / "airflow_health.json", airflow_health)
    _write_json(ADR06_EVIDENCE_ROOT / "run_manifest.json", manifest)
    return manifest
