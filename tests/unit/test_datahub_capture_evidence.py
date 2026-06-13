from __future__ import annotations

import importlib.util
import json
from pathlib import Path

import pytest


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[2]


def _load_script_module():
    script_path = _repo_root() / "scripts" / "datahub" / "capture_evidence.py"
    spec = importlib.util.spec_from_file_location("datahub_capture_evidence_script", script_path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_datahub_capture_evidence_exits_nonzero_and_writes_failed_manifest(monkeypatch, tmp_path: Path) -> None:
    module = _load_script_module()

    monkeypatch.setattr(module, "EVIDENCE_ROOT", tmp_path)
    monkeypatch.setattr(module, "capture_gms_health", lambda: {"healthy": False, "error": "gms unavailable"})
    monkeypatch.setattr(
        module,
        "capture_dataset_evidence",
        lambda: {"status": "missing", "error": "No successful datahub_ingestion manifest found"},
    )
    monkeypatch.setattr(module, "capture_tag_evidence", lambda: {"status": "success", "failed_tags": []})

    with pytest.raises(SystemExit) as exc_info:
        module.main()

    assert exc_info.value.code == 1
    manifest = json.loads((tmp_path / "run_manifest.json").read_text(encoding="utf-8"))
    assert manifest["status"] == "failed"
    assert manifest["failures"] == [
        {"step": "gms_health", "error": "gms unavailable"},
        {"step": "dataset_evidence", "error": "No successful datahub_ingestion manifest found"},
    ]


def test_datahub_capture_evidence_marks_tag_failures_as_partial(monkeypatch, tmp_path: Path) -> None:
    module = _load_script_module()

    monkeypatch.setattr(module, "EVIDENCE_ROOT", tmp_path)
    monkeypatch.setattr(module, "capture_gms_health", lambda: {"healthy": True, "status_code": 200})
    monkeypatch.setattr(module, "capture_dataset_evidence", lambda: {"status": "success", "latest_successful_run_id": "manual__ok"})
    monkeypatch.setattr(
        module,
        "capture_tag_evidence",
        lambda: {"status": "partial", "failed_tags": [{"urn": "urn:li:tag:gold", "error": "Tag not found"}]},
    )

    manifest = module.capture_evidence()

    assert manifest["status"] == "partial"
    assert manifest["failures"] == [{"step": "tag_evidence", "error": "1 tag lookups failed"}]
    assert (tmp_path / "run_manifest.json").is_file()
