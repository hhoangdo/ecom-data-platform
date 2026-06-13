import builtins
import importlib
import sys
from contextlib import contextmanager
from pathlib import Path

import pytest


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[2]


def _clear_cached_modules(*module_names: str) -> None:
    for module_name in module_names:
        sys.modules.pop(module_name, None)


@contextmanager
def _fresh_modules(*module_names: str):
    cached = {module_name: sys.modules.get(module_name) for module_name in module_names}
    parent_attrs: dict[str, tuple[object | None, bool, object | None]] = {}
    for module_name in module_names:
        parent_name, _, attr_name = module_name.rpartition(".")
        if not parent_name:
            continue
        parent_module = sys.modules.get(parent_name)
        had_attr = parent_module is not None and hasattr(parent_module, attr_name)
        attr_value = getattr(parent_module, attr_name) if had_attr else None
        parent_attrs[module_name] = (parent_module, had_attr, attr_value)
    _clear_cached_modules(*module_names)
    try:
        yield
    finally:
        for module_name in module_names:
            sys.modules.pop(module_name, None)
            cached_module = cached[module_name]
            if cached_module is not None:
                sys.modules[module_name] = cached_module
            parent_module, had_attr, attr_value = parent_attrs.get(module_name, (None, False, None))
            if parent_module is None:
                continue
            _, _, attr_name = module_name.rpartition(".")
            if had_attr:
                setattr(parent_module, attr_name, attr_value)
            elif hasattr(parent_module, attr_name):
                delattr(parent_module, attr_name)


def _block_duckdb_import(monkeypatch: pytest.MonkeyPatch) -> None:
    original_import = builtins.__import__

    def guarded_import(name, globals=None, locals=None, fromlist=(), level=0):
        if name == "duckdb":
            raise ModuleNotFoundError("No module named 'duckdb'")
        return original_import(name, globals, locals, fromlist, level)

    monkeypatch.setattr(builtins, "__import__", guarded_import)


def test_orchestration_runtime_import_still_collects_without_duckdb(monkeypatch: pytest.MonkeyPatch) -> None:
    _block_duckdb_import(monkeypatch)
    with _fresh_modules(
        "duckdb",
        "vina_bim_shop.lakehouse.spark.parity",
        "vina_bim_shop.lakehouse.spark.runner",
        "vina_bim_shop.orchestration.paths",
    ):
        paths = importlib.import_module("vina_bim_shop.orchestration.paths")

    assert paths.REPO_ROOT == _repo_root()


def test_run_parity_checks_raises_actionable_error_without_duckdb(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    with _fresh_modules("vina_bim_shop.lakehouse.spark.parity"):
        parity = importlib.import_module("vina_bim_shop.lakehouse.spark.parity")
        original_import_module = parity.importlib.import_module

        def fake_import_module(name: str):
            if name == "duckdb":
                raise ModuleNotFoundError("No module named 'duckdb'")
            return original_import_module(name)

        monkeypatch.setattr(parity.importlib, "import_module", fake_import_module)

        with pytest.raises(RuntimeError, match="DuckDB parity checks require project dependencies; run via `uv run`\\."):
            parity.run_parity_checks(evidence_root=tmp_path)
