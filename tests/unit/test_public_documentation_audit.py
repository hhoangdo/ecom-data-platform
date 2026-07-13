import importlib.util
import json
import sys
from pathlib import Path

import pytest


EXPECTED_TARGETS = (
    (
        "src/vina_bim_shop/generators/runner.py",
        (("GenerationResult", "class"), ("run_generation", "function")),
    ),
    ("src/vina_bim_shop/lakehouse/spark/runner.py", (("run_batch_pipeline", "function"),)),
    (
        "src/vina_bim_shop/flink/runtime.py",
        (("RuntimeSettings", "class"), ("load_runtime_settings", "function")),
    ),
    (
        "src/vina_bim_shop/orchestration/specs.py",
        (("DagSpec", "class"), ("dag_specs_by_id", "function")),
    ),
    ("src/vina_bim_shop/datahub_lineage/emitter.py", (("DataHubLineageEmitter", "class"),)),
    ("src/vina_bim_shop/pinot/bootstrap.py", (("apply_assets", "function"),)),
    ("src/vina_bim_shop/quality/policies.py", (("gate_outcome_for_layer", "function"),)),
)


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[2]


def _load_module():
    script_path = _repo_root() / "scripts" / "qa" / "audit_public_documentation.py"
    spec = importlib.util.spec_from_file_location("audit_public_documentation", script_path)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def test_public_documentation_audit_script_exists() -> None:
    assert (_repo_root() / "scripts" / "qa" / "audit_public_documentation.py").is_file()


def test_declared_public_api_surface_is_exact() -> None:
    module = _load_module()

    assert module.PUBLIC_API_TARGETS == EXPECTED_TARGETS


def test_audit_reports_complete_current_public_api_coverage() -> None:
    module = _load_module()

    report = module.audit_public_documentation(_repo_root())

    assert report["success"] is True
    assert report["summary"] == {
        "required_modules": 7,
        "documented_modules": 7,
        "required_symbols": 10,
        "documented_symbols": 10,
        "coverage_percent": 100.0,
    }
    assert all(item["documented"] and len(item["sha256"]) == 64 for item in report["modules"])
    assert all(item["documented"] and len(item["sha256"]) == 64 for item in report["symbols"])


def test_audit_fails_closed_for_missing_docstrings(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    module = _load_module()
    source = tmp_path / "src" / "entry.py"
    source.parent.mkdir(parents=True)
    source.write_text("def deploy():\n    return None\n", encoding="utf-8")
    monkeypatch.setattr(module, "PUBLIC_API_TARGETS", (("src/entry.py", (("deploy", "function"),)),))

    report = module.audit_public_documentation(tmp_path)

    assert report["success"] is False
    assert report["summary"]["coverage_percent"] == 0.0
    with pytest.raises(ValueError, match="Public API documentation audit failed"):
        module.write_coverage_report(tmp_path / "coverage.json", report)
    assert not (tmp_path / "coverage.json").exists()


def test_coverage_writer_persists_only_a_complete_report(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    module = _load_module()
    source = tmp_path / "src" / "entry.py"
    source.parent.mkdir(parents=True)
    source.write_text('"""Deployable entry point."""\n\ndef deploy():\n    """Deploys and returns no value; caller failures propagate."""\n    return None\n', encoding="utf-8")
    monkeypatch.setattr(module, "PUBLIC_API_TARGETS", (("src/entry.py", (("deploy", "function"),)),))

    report = module.audit_public_documentation(tmp_path)
    output = tmp_path / "coverage.json"
    module.write_coverage_report(output, report)

    assert json.loads(output.read_text(encoding="utf-8")) == report
