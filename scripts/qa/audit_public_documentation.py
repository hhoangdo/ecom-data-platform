"""Audit the small, declared deployable Python API surface for documentation."""

from __future__ import annotations

import argparse
import ast
import hashlib
import json
from pathlib import Path
from typing import Any


PUBLIC_API_TARGETS = (
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


def _summary_line(docstring: str | None) -> str:
    if not docstring:
        return ""
    return next((line.strip() for line in docstring.splitlines() if line.strip()), "")


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def audit_public_documentation(repo_root: Path) -> dict[str, object]:
    """Return AST documentation coverage for the exact declared public API."""

    modules: list[dict[str, object]] = []
    symbols: list[dict[str, object]] = []
    for relative_path, expected_symbols in PUBLIC_API_TARGETS:
        source_path = repo_root / relative_path
        if source_path.is_file():
            source = source_path.read_text(encoding="utf-8")
            tree = ast.parse(source, filename=relative_path)
            module_docstring = ast.get_docstring(tree)
            declarations = {
                node.name: node
                for node in tree.body
                if isinstance(node, (ast.ClassDef, ast.FunctionDef, ast.AsyncFunctionDef))
            }
            source_hash = _sha256(source_path)
        else:
            module_docstring = None
            declarations = {}
            source_hash = ""

        modules.append(
            {
                "path": relative_path,
                "documented": bool(_summary_line(module_docstring)),
                "summary": _summary_line(module_docstring),
                "sha256": source_hash,
            }
        )
        for name, expected_kind in expected_symbols:
            node = declarations.get(name)
            actual_kind = "class" if isinstance(node, ast.ClassDef) else "function" if node else "missing"
            docstring = ast.get_docstring(node) if node is not None else None
            symbols.append(
                {
                    "path": relative_path,
                    "name": name,
                    "kind": expected_kind,
                    "documented": actual_kind == expected_kind and bool(_summary_line(docstring)),
                    "summary": _summary_line(docstring),
                    "sha256": source_hash,
                }
            )

    documented_modules = sum(bool(item["documented"]) for item in modules)
    documented_symbols = sum(bool(item["documented"]) for item in symbols)
    required_targets = len(modules) + len(symbols)
    documented_targets = documented_modules + documented_symbols
    summary = {
        "required_modules": len(modules),
        "documented_modules": documented_modules,
        "required_symbols": len(symbols),
        "documented_symbols": documented_symbols,
        "coverage_percent": round((documented_targets / required_targets) * 100, 2) if required_targets else 0.0,
    }
    return {
        "schema_version": 1,
        "success": documented_targets == required_targets,
        "summary": summary,
        "modules": modules,
        "symbols": symbols,
    }


def write_coverage_report(output_path: Path, report: dict[str, object]) -> None:
    """Persist only a complete coverage report so missing docs fail closed."""

    if not report["success"]:
        raise ValueError("Public API documentation audit failed; coverage evidence was not written.")
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    repo_root = Path(__file__).resolve().parents[2]
    report = audit_public_documentation(repo_root)
    write_coverage_report(args.output, report)
    print(f"Public API documentation coverage: {report['summary']['coverage_percent']}%")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
