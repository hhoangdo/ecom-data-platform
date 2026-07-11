from __future__ import annotations

import hashlib
import importlib.util
import json
from pathlib import Path

import duckdb
import pytest


REPO_ROOT = Path(__file__).resolve().parents[2]
SCRIPT_PATH = REPO_ROOT / "scripts" / "analytics" / "benchmark_duckdb_index.py"


def _load_script_module():
    spec = importlib.util.spec_from_file_location("benchmark_duckdb_index_script", SCRIPT_PATH)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _create_source_database(path: Path) -> None:
    with duckdb.connect(str(path)) as connection:
        connection.execute("create schema gold")
        connection.execute(
            """
            create table gold.fact_order (
                order_id varchar,
                official_paid_revenue decimal(18, 2)
            )
            """
        )
        connection.executemany(
            "insert into gold.fact_order values (?, ?)",
            [("order-001", 100.00), ("order-002", 200.00), ("order-003", 300.00)],
        )


def test_benchmark_refuses_identical_source_and_destination(tmp_path: Path) -> None:
    module = _load_script_module()
    source = tmp_path / "canonical.duckdb"
    _create_source_database(source)

    with pytest.raises(ValueError, match="must differ"):
        module.benchmark_index(source, source, tmp_path / "evidence")


def test_benchmark_preserves_source_and_records_index_evidence(tmp_path: Path) -> None:
    module = _load_script_module()
    source = tmp_path / "canonical.duckdb"
    benchmark = tmp_path / "benchmark.duckdb"
    evidence_root = tmp_path / "evidence"
    _create_source_database(source)
    source_hash_before = _sha256(source)

    result = module.benchmark_index(source, benchmark, evidence_root)

    assert _sha256(source) == source_hash_before
    assert result["source_sha256_before"] == source_hash_before
    assert result["source_sha256_after"] == source_hash_before
    assert result["index_name"] == "idx_benchmark_fact_order_order_id"
    assert result["index_column"] == "order_id"
    assert result["result_hashes"]["baseline"] == result["result_hashes"]["indexed"]
    assert len(result["timings"]["baseline"]["warmups_ms"]) == 2
    assert len(result["timings"]["baseline"]["measured_ms"]) == 7
    assert len(result["timings"]["indexed"]["measured_ms"]) == 7
    assert result["index_metadata"]
    assert (evidence_root / "baseline_explain.txt").is_file()
    assert (evidence_root / "indexed_explain.txt").is_file()
    assert json.loads((evidence_root / "index_benchmark.json").read_text(encoding="utf-8")) == result

    with duckdb.connect(str(benchmark), read_only=True) as connection:
        index_names = {
            row[0]
            for row in connection.execute(
                "select index_name from duckdb_indexes() where table_name = 'benchmark_fact_order'"
            ).fetchall()
        }
    assert index_names == {"idx_benchmark_fact_order_order_id"}


def test_benchmark_replaces_an_existing_temporary_database(tmp_path: Path) -> None:
    module = _load_script_module()
    source = tmp_path / "canonical.duckdb"
    benchmark = tmp_path / "benchmark.duckdb"
    _create_source_database(source)
    with duckdb.connect(str(benchmark)) as connection:
        connection.execute("create table stale_data (value integer)")

    module.benchmark_index(source, benchmark, tmp_path / "evidence")

    with duckdb.connect(str(benchmark), read_only=True) as connection:
        table_names = {
            row[0]
            for row in connection.execute(
                "select table_name from duckdb_tables() where schema_name = 'main'"
            ).fetchall()
        }
    assert table_names == {"benchmark_fact_order"}
