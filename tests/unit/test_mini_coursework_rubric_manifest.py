import importlib.util
import json
import sys
from dataclasses import replace
from pathlib import Path

import pytest


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[2]


def _load_module():
    script_path = _repo_root() / "scripts" / "qa" / "build_mini_coursework_rubric_manifest.py"
    spec = importlib.util.spec_from_file_location("build_mini_coursework_rubric_manifest", script_path)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def test_rubric_manifest_builder_exists() -> None:
    assert (_repo_root() / "scripts" / "qa" / "build_mini_coursework_rubric_manifest.py").is_file()


def test_static_requirements_cover_rows_2_through_46_once_in_order() -> None:
    module = _load_module()

    assert [requirement.row for requirement in module.ROW_REQUIREMENTS] == list(range(2, 47))
    assert all(requirement.implementation_paths for requirement in module.ROW_REQUIREMENTS)
    assert all(requirement.evidence_paths for requirement in module.ROW_REQUIREMENTS)


def test_topic01_gate_accepts_the_existing_two_from_multistage_dockerfile() -> None:
    module = _load_module()

    module._gate_multistage_connect(_repo_root())


def test_build_manifest_reports_partial_for_missing_required_evidence(tmp_path: Path) -> None:
    module = _load_module()
    implementation = tmp_path / "src" / "entry.py"
    implementation.parent.mkdir(parents=True)
    implementation.write_text("pass\n", encoding="utf-8")
    requirement = module.RowRequirement(
        row=2,
        points=10,
        implementation_paths=("src/entry.py",),
        evidence_paths=("evidence/result.json",),
        notes="fixture",
        gate=lambda _root: None,
    )

    manifest = module.build_manifest(tmp_path, requirements=(requirement,))

    assert manifest["rows"][0]["status"] == "Partial"
    assert "evidence/result.json" in manifest["rows"][0]["notes"]


def test_verify_manifest_rejects_changed_artifacts_and_forged_satisfied_status(tmp_path: Path) -> None:
    module = _load_module()
    implementation = tmp_path / "src" / "entry.py"
    evidence = tmp_path / "evidence" / "result.json"
    implementation.parent.mkdir(parents=True)
    evidence.parent.mkdir(parents=True)
    implementation.write_text("pass\n", encoding="utf-8")
    evidence.write_text('{"success": true}\n', encoding="utf-8")
    requirement = module.RowRequirement(
        row=2,
        points=10,
        implementation_paths=("src/entry.py",),
        evidence_paths=("evidence/result.json",),
        notes="fixture",
        gate=lambda _root: None,
    )
    manifest = module.build_manifest(tmp_path, requirements=(requirement,))
    persisted = tmp_path / "manifest.json"
    module.write_manifest(persisted, manifest)
    module.verify_manifest(tmp_path, persisted, requirements=(requirement,))

    evidence.write_text('{"success": false}\n', encoding="utf-8")
    with pytest.raises(ValueError, match="does not match current artifacts"):
        module.verify_manifest(tmp_path, persisted, requirements=(requirement,))

    evidence.write_text('{"success": true}\n', encoding="utf-8")
    forged = json.loads(persisted.read_text(encoding="utf-8"))
    forged["rows"][0]["status"] = "Satisfied"
    forged["rows"][0]["evidence_sha256"] = {}
    persisted.write_text(json.dumps(forged), encoding="utf-8")
    with pytest.raises(ValueError, match="does not match current artifacts"):
        module.verify_manifest(tmp_path, persisted, requirements=(requirement,))


def test_manifest_rejects_duplicate_or_outside_requirement_paths(tmp_path: Path) -> None:
    module = _load_module()
    source = tmp_path / "src" / "entry.py"
    source.parent.mkdir(parents=True)
    source.write_text("pass\n", encoding="utf-8")
    duplicate = module.RowRequirement(
        row=2,
        points=10,
        implementation_paths=("src/entry.py", "src/entry.py"),
        evidence_paths=("src/entry.py",),
        notes="fixture",
        gate=lambda _root: None,
    )
    outside = replace(duplicate, implementation_paths=("../outside.py",))

    with pytest.raises(ValueError, match="duplicate"):
        module.build_manifest(tmp_path, requirements=(duplicate,))
    with pytest.raises(ValueError, match="outside"):
        module.build_manifest(tmp_path, requirements=(outside,))
