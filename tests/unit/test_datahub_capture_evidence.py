from __future__ import annotations

import importlib.util
import json
import hashlib
from pathlib import Path

import pytest
from PIL import Image


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
    monkeypatch.setattr(module, "COURSEWORK_PIPELINE_EVIDENCE_ROOT", tmp_path / "coursework_pipeline")
    monkeypatch.setattr(module, "capture_gms_health", lambda: {"healthy": False, "error": "gms unavailable"})
    monkeypatch.setattr(
        module,
        "capture_dataset_evidence",
        lambda: {"status": "missing", "error": "No successful datahub_ingestion manifest found"},
    )
    monkeypatch.setattr(module, "capture_tag_evidence", lambda: {"status": "success", "failed_tags": []})
    monkeypatch.setattr(module, "capture_search_evidence", lambda: {"status": "success"})
    monkeypatch.setattr(module, "capture_coursework_pipeline_evidence", lambda: {"status": "success", "indexed_search": {"status": "success"}})
    monkeypatch.setattr(module, "validate_coursework_screenshot_manifest", lambda: {"status": "success", "failures": []})

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
    monkeypatch.setattr(module, "COURSEWORK_PIPELINE_EVIDENCE_ROOT", tmp_path / "coursework_pipeline")
    monkeypatch.setattr(module, "capture_gms_health", lambda: {"healthy": True, "status_code": 200})
    monkeypatch.setattr(module, "capture_dataset_evidence", lambda: {"status": "success", "latest_successful_run_id": "manual__ok"})
    monkeypatch.setattr(
        module,
        "capture_tag_evidence",
        lambda: {"status": "partial", "failed_tags": [{"urn": "urn:li:tag:gold", "error": "Tag not found"}]},
    )
    monkeypatch.setattr(module, "capture_search_evidence", lambda: {"status": "success"})
    monkeypatch.setattr(module, "capture_coursework_pipeline_evidence", lambda: {"status": "success", "indexed_search": {"status": "success"}})
    monkeypatch.setattr(module, "validate_coursework_screenshot_manifest", lambda: {"status": "success", "failures": []})

    manifest = module.capture_evidence()

    assert manifest["status"] == "partial"
    assert manifest["failures"] == [{"step": "tag_evidence", "error": "1 tag lookups failed"}]
    assert (tmp_path / "run_manifest.json").is_file()


def test_datahub_capture_evidence_fails_when_indexed_search_is_unavailable(monkeypatch, tmp_path: Path) -> None:
    module = _load_script_module()

    monkeypatch.setattr(module, "EVIDENCE_ROOT", tmp_path)
    monkeypatch.setattr(module, "COURSEWORK_PIPELINE_EVIDENCE_ROOT", tmp_path / "coursework_pipeline")
    monkeypatch.setattr(module, "capture_gms_health", lambda: {"healthy": True, "status_code": 200})
    monkeypatch.setattr(module, "capture_dataset_evidence", lambda: {"status": "success", "latest_successful_run_id": "manual__ok"})
    monkeypatch.setattr(module, "capture_tag_evidence", lambda: {"status": "success", "failed_tags": []})
    monkeypatch.setattr(module, "capture_search_evidence", lambda: {"status": "failed", "error": "indexed search returned no datasets"})
    monkeypatch.setattr(module, "capture_coursework_pipeline_evidence", lambda: {"status": "success", "indexed_search": {"status": "success"}})
    monkeypatch.setattr(module, "validate_coursework_screenshot_manifest", lambda: {"status": "success", "failures": []})

    manifest = module.capture_evidence()

    assert manifest["status"] == "failed"
    assert manifest["failures"] == [{"step": "search_evidence", "error": "indexed search returned no datasets"}]


def test_coursework_screenshot_manifest_requires_all_real_pngs_and_matching_hashes(tmp_path: Path) -> None:
    module = _load_script_module()
    screenshots = tmp_path.parent / "screenshots"
    screenshots.mkdir()
    entries = []
    for target in module.COURSEWORK_SCREENSHOT_TARGETS:
        image_path = screenshots / Path(target["path"]).name
        Image.new("RGB", (32, 24), color="white").save(image_path)
        entries.append(
            {
                **target,
                "captured_at": "2026-07-12T00:00:00+00:00",
                "width": 32,
                "height": 24,
                "sha256": hashlib.sha256(image_path.read_bytes()).hexdigest(),
                "reloaded": True,
            }
        )
    (tmp_path / "ui_screenshot_manifest.json").write_text(json.dumps({"screenshots": entries}), encoding="utf-8")

    assert module.validate_coursework_screenshot_manifest(tmp_path)["status"] == "success"

    (screenshots / Path(entries[0]["path"]).name).unlink()
    failed = module.validate_coursework_screenshot_manifest(tmp_path)
    assert failed["status"] == "failed"
    assert failed["failures"][0]["id"] == "dp1_lineage"


def test_coursework_gate_cannot_pass_with_direct_evidence_when_index_or_screenshots_fail() -> None:
    module = _load_script_module()
    machine = {"status": "success", "direct_graphql": {"status": "success"}, "indexed_search": {"status": "failed"}}
    screenshots = {"status": "failed", "failures": [{"id": "dp1_lineage", "error": "missing"}]}

    status, failures = module.coursework_gate_status(machine, screenshots)

    assert status == "failed"
    assert {failure["step"] for failure in failures} == {"coursework_indexed_search", "coursework_screenshots"}


def test_coursework_capture_queries_match_datahub_1_6_property_and_assertion_shapes() -> None:
    module = _load_script_module()

    assert "properties {" in module.GRAPHQL_DATAFLOW_QUERY
    assert "properties {" in module.GRAPHQL_DATAJOB_QUERY
    assert "datasetUrn" in module.GRAPHQL_ASSERTION_QUERY
    assert "runEvents(limit: 1)" in module.GRAPHQL_ASSERTION_QUERY
    assert "latestRunEvent" not in module.GRAPHQL_ASSERTION_QUERY


def test_coursework_assertion_verification_accepts_every_dp1_and_dp3_output() -> None:
    module = _load_script_module()

    assert module._expected_assertion_datasets("dp1_raw_to_bronze", {}) == {
        "urn:li:dataset:(urn:li:dataPlatform:s3,bronze.batch,PROD)",
        "urn:li:dataset:(urn:li:dataPlatform:s3,bronze.events,PROD)",
    }
    assert module._expected_assertion_datasets("dp3_offline_features", {}) == {
        "urn:li:dataset:(urn:li:dataPlatform:iceberg,vina_bim_shop.feat_customer_90d,PROD)",
        "urn:li:dataset:(urn:li:dataPlatform:iceberg,vina_bim_shop.feat_stream_60m,PROD)",
        "urn:li:dataset:(urn:li:dataPlatform:iceberg,vina_bim_shop.feat_customer_unified,PROD)",
    }


def test_capture_evidence_fails_closed_when_coursework_job_search_is_missing(monkeypatch, tmp_path: Path) -> None:
    module = _load_script_module()

    monkeypatch.setattr(module, "EVIDENCE_ROOT", tmp_path)
    monkeypatch.setattr(module, "COURSEWORK_PIPELINE_EVIDENCE_ROOT", tmp_path / "coursework_pipeline")
    monkeypatch.setattr(module, "capture_gms_health", lambda: {"healthy": True, "status_code": 200})
    monkeypatch.setattr(module, "capture_dataset_evidence", lambda: {"status": "success", "latest_successful_run_id": "manual__ok"})
    monkeypatch.setattr(module, "capture_tag_evidence", lambda: {"status": "success", "failed_tags": []})
    monkeypatch.setattr(module, "capture_search_evidence", lambda: {"status": "success"})
    monkeypatch.setattr(
        module,
        "capture_coursework_pipeline_evidence",
        lambda: {"status": "success", "indexed_search": {"status": "failed", "error": "DP2 job not indexed"}},
    )
    monkeypatch.setattr(module, "validate_coursework_screenshot_manifest", lambda: {"status": "success", "failures": []})

    manifest = module.capture_evidence()

    assert manifest["status"] == "failed"
    assert manifest["failures"] == [{"step": "coursework_indexed_search", "error": "DP2 job not indexed"}]
