"""Path, manifest, and evidence-root helpers shared by every DAG runtime.

These helpers are intentionally limited to:

- Module-level constants for the ADR 06 evidence tree.
- ``build_run_root`` to derive a per-run directory from a ``dag_id`` and ``run_id``.
- ``_utc_now`` for ISO-8601 UTC timestamps used in run manifests.
- ``_write_json`` for serialising manifest artifacts to disk.

No subprocess, HTTP, or domain logic lives here.
"""
from __future__ import annotations

import json
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


REPO_ROOT = Path(__file__).resolve().parents[3]
ADR06_EVIDENCE_ROOT = REPO_ROOT / "evidence" / "08_airflow_gx"
RUNS_ROOT = ADR06_EVIDENCE_ROOT / "runs"
DOCS_ROOT = ADR06_EVIDENCE_ROOT / "gx_data_docs"


def _slugify_run_id(run_id: str) -> str:
    return re.sub(r"[^a-zA-Z0-9._-]+", "_", run_id)


def build_run_root(dag_id: str, run_id: str) -> Path:
    path = RUNS_ROOT / dag_id / _slugify_run_id(run_id)
    path.mkdir(parents=True, exist_ok=True)
    return path


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
