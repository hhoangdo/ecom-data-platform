from __future__ import annotations

import ast
import importlib.util
import sys
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]
MODULE_PATH = REPO_ROOT / "src" / "vina_bim_shop" / "lakehouse" / "spark" / "optimization_experiments.py"
SCRIPT_PATH = REPO_ROOT / "scripts" / "spark" / "run_optimization_experiments.py"

VARIANTS = (
    "skew-baseline",
    "skew-optimized",
    "high-cardinality-baseline",
    "high-cardinality-optimized",
)


def _read(path: Path) -> str:
    assert path.is_file(), f"Expected experiment source at {path.relative_to(REPO_ROOT)}."
    return path.read_text(encoding="utf-8")


def _tree(path: Path) -> ast.Module:
    return ast.parse(_read(path), filename=str(path))


def _load_script_module():
    spec = importlib.util.spec_from_file_location("optimization_experiment_script", SCRIPT_PATH)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_experiment_contract_declares_exact_variants_hot_cities_and_salt_count() -> None:
    source = _read(MODULE_PATH)

    for value in VARIANTS:
        assert value in source
    assert "Ho Chi Minh City" in source
    assert "Ha Noi" in source
    assert "SALT_BUCKETS = 16" in source
    assert "pmod" in source
    assert "xxhash64" in source
    assert "expression = expression.when" in source


def test_experiment_sources_cannot_import_or_call_canonical_run_job() -> None:
    forbidden_text = (
        "iceberg.",
        "MERGE INTO",
        "CREATE TABLE",
        "_persist_silver_tables_for_window",
        "_persist_gold_tables",
    )

    for path in (MODULE_PATH, SCRIPT_PATH):
        source = _read(path)
        tree = _tree(path)
        assert all(fragment not in source for fragment in forbidden_text)

        for node in ast.walk(tree):
            if isinstance(node, ast.ImportFrom):
                assert node.module != "vina_bim_shop.lakehouse.spark.job"
                assert all(alias.name != "run_job" for alias in node.names)
            if isinstance(node, ast.Import):
                assert all(alias.name != "vina_bim_shop.lakehouse.spark.job" for alias in node.names)
            if isinstance(node, ast.Call):
                if isinstance(node.func, ast.Name):
                    assert node.func.id != "run_job"
                if isinstance(node.func, ast.Attribute):
                    assert node.func.attr != "run_job"


def test_cli_requires_one_exact_variant_and_coursework_scale(monkeypatch, tmp_path: Path) -> None:
    module = _load_script_module()
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "run_optimization_experiments.py",
            "--variant",
            "skew-baseline",
            "--evidence-root",
            str(tmp_path),
        ],
    )

    args = module.parse_args()

    assert args.config == "configs/generator/base.yaml"
    assert args.scale == "coursework"
    assert args.variant == "skew-baseline"
    assert args.evidence_root == str(tmp_path)
