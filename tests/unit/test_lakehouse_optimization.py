from __future__ import annotations

import csv
import importlib.util
import json
from pathlib import Path

import pytest

from vina_bim_shop.lakehouse.spark.maintenance import (
    TABLE_ALLOWLIST,
    compare_invariants,
    rewrite_data_files,
    rewrite_data_files_sql,
    validate_tables,
)


REPO_ROOT = Path(__file__).resolve().parents[2]
SCRIPT_PATH = REPO_ROOT / "scripts" / "lakehouse" / "optimize_iceberg.py"


def _load_script_module():
    spec = importlib.util.spec_from_file_location("optimize_iceberg_script", SCRIPT_PATH)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_table_allowlist_accepts_only_storage_optimization_targets() -> None:
    assert TABLE_ALLOWLIST == (
        "silver.stg_orders",
        "silver.stg_order_items",
        "gold.fact_order",
        "gold.fact_order_item",
    )
    assert validate_tables(["silver.stg_orders", "gold.fact_order_item"]) == (
        "silver.stg_orders",
        "gold.fact_order_item",
    )

    with pytest.raises(ValueError, match="not approved"):
        validate_tables(["gold.feat_customer_90d"])


def test_rewrite_sql_uses_the_required_iceberg_options() -> None:
    statement = rewrite_data_files_sql("gold.fact_order")

    assert "CALL iceberg.system.rewrite_data_files" in statement
    assert "table => 'gold.fact_order'" in statement
    assert "'target-file-size-bytes', '134217728'" in statement
    assert "'min-input-files', '2'" in statement


def test_invariant_comparison_ignores_layout_but_rejects_logical_changes() -> None:
    before = {"file_count": 8, "total_bytes": 900, "row_count": 12, "aggregate_hash": "before"}
    changed_layout = {"file_count": 1, "total_bytes": 700, "row_count": 12, "aggregate_hash": "before"}

    assert compare_invariants(before, changed_layout) == {"success": True, "failures": []}

    with pytest.raises(ValueError, match="row_count"):
        compare_invariants(before, {**changed_layout, "row_count": 11})
    with pytest.raises(ValueError, match="aggregate_hash"):
        compare_invariants(before, {**changed_layout, "aggregate_hash": "after"})


def test_rewrite_skips_tables_without_two_input_files() -> None:
    class FakeSpark:
        def sql(self, _statement: str):
            raise AssertionError("rewrite procedure must not run")

    result = rewrite_data_files(FakeSpark(), "silver.stg_orders", input_file_count=1)

    assert result == {
        "table": "silver.stg_orders",
        "status": "skipped_insufficient_files",
        "input_file_count": 1,
        "minimum_input_files": 2,
        "rewritten_data_files_count": 0,
    }


def test_rewrite_records_when_partition_groups_are_not_eligible() -> None:
    class FakeRow:
        def asDict(self, recursive: bool) -> dict[str, int]:
            assert recursive is True
            return {"rewritten_data_files_count": 0}

    class FakeResult:
        def collect(self) -> list[FakeRow]:
            return [FakeRow()]

    class FakeSpark:
        def sql(self, statement: str) -> FakeResult:
            assert "rewrite_data_files" in statement
            return FakeResult()

    result = rewrite_data_files(FakeSpark(), "gold.fact_order", input_file_count=7)

    assert result["status"] == "skipped_no_eligible_file_groups"
    assert result["rewritten_data_files_count"] == 0


def test_cli_writes_before_file_stats_before_rewrite(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    module = _load_script_module()
    events: list[str] = []

    def fake_build_spark_session():
        return object()

    def fake_collect_file_stats(_spark: object, table_name: str) -> dict[str, object]:
        events.append(f"stats:{table_name}")
        return {
            "table": table_name,
            "file_count": 2,
            "total_bytes": 100,
            "min_bytes": 50,
            "max_bytes": 50,
            "average_bytes": 50.0,
            "row_count": 3,
            "aggregate_hash": "same",
        }

    def fake_rewrite(_spark: object, table_name: str, **_kwargs: object) -> dict[str, object]:
        before_csv = tmp_path / "before_file_stats.csv"
        assert before_csv.is_file()
        with before_csv.open(newline="", encoding="utf-8") as handle:
            rows = list(csv.DictReader(handle))
        assert [row["table"] for row in rows] == ["silver.stg_orders"]
        events.append(f"rewrite:{table_name}")
        return {
            "table": table_name,
            "status": "rewritten",
            "input_file_count": 2,
            "minimum_input_files": 2,
            "rewritten_data_files_count": 2,
        }

    monkeypatch.setattr(module, "build_spark_session", fake_build_spark_session)
    monkeypatch.setattr(module, "collect_file_stats", fake_collect_file_stats)
    monkeypatch.setattr(module, "rewrite_data_files", fake_rewrite)
    monkeypatch.setattr(module, "benchmark_trino_queries", lambda **_kwargs: {"queries": []})
    monkeypatch.setattr(
        "sys.argv",
        [
            "optimize_iceberg.py",
            "--tables",
            "silver.stg_orders",
            "--evidence-root",
            str(tmp_path),
        ],
    )

    module.main()

    assert events == ["stats:silver.stg_orders", "rewrite:silver.stg_orders", "stats:silver.stg_orders"]


def test_cli_persists_blocked_evidence_when_no_file_group_is_eligible(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = _load_script_module()

    def fake_stats(_spark: object, table_name: str) -> dict[str, object]:
        return {
            "table": table_name,
            "file_count": 7,
            "total_bytes": 700,
            "min_bytes": 100,
            "max_bytes": 100,
            "average_bytes": 100.0,
            "row_count": 3,
            "aggregate_hash": "same",
        }

    monkeypatch.setattr(module, "build_spark_session", lambda: object())
    monkeypatch.setattr(module, "collect_file_stats", fake_stats)
    monkeypatch.setattr(
        module,
        "rewrite_data_files",
        lambda _spark, table_name, **_kwargs: {
            "table": table_name,
            "status": "skipped_no_eligible_file_groups",
            "input_file_count": 7,
            "minimum_input_files": 2,
            "rewritten_data_files_count": 0,
        },
    )
    monkeypatch.setattr(
        module,
        "benchmark_trino_queries",
        lambda **_kwargs: {"warmup_runs": 2, "measured_runs": 7, "queries": []},
    )
    monkeypatch.setattr(
        "sys.argv",
        [
            "optimize_iceberg.py",
            "--tables",
            "gold.fact_order",
            "--evidence-root",
            str(tmp_path),
        ],
    )

    with pytest.raises(RuntimeError, match="did not rewrite"):
        module.main()

    results = json.loads((tmp_path / "compaction_results.json").read_text(encoding="utf-8"))
    benchmark = json.loads((tmp_path / "query_benchmark.json").read_text(encoding="utf-8"))
    assert results["success"] is False
    assert results["status"] == "blocked_no_eligible_file_groups"
    assert results["invariants"] == [{"failures": [], "success": True, "table": "gold.fact_order"}]
    assert benchmark["after"] is None
    assert (tmp_path / "report.md").is_file()
    assert (tmp_path / "run_manifest.json").is_file()


def test_cli_uses_the_spark_image_python_310_compatible_utc_form() -> None:
    source = SCRIPT_PATH.read_text(encoding="utf-8")

    assert "from datetime import UTC" not in source
    assert "datetime.now(timezone.utc)" in source


def test_cli_defaults_to_the_compose_trino_service(monkeypatch: pytest.MonkeyPatch) -> None:
    module = _load_script_module()
    monkeypatch.delenv("VBS_TRINO_URL", raising=False)
    monkeypatch.setattr("sys.argv", ["optimize_iceberg.py", "--tables", "gold.fact_order"])

    assert module.parse_args().trino_url == "http://trino:8080"
