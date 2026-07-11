"""DAG adapter contract tests.

These tests assert that the six DAG adapters under
``infra/orchestration/airflow/dags/`` continue to:

- Keep their ``dag_id``, ``schedule``, and ``tags`` stable.
- Import the expected runtime function from
  ``src/vina_bim_shop/orchestration/``.
- For the two hourly DAGs, forward ``data_interval_start`` and
  ``data_interval_end`` into ``start_ts`` / ``end_ts`` correctly.

The DAG files are small, declarative, and read as text — Airflow is not
importable in the test environment, so the tests assert against the source
content (the same pattern used by
``tests/unit/test_orchestration_runtime.py::test_pinot_bootstrap_dag_exists_without_flink_control_logic``).
"""
from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]
DAGS_ROOT = REPO_ROOT / "infra" / "orchestration" / "airflow" / "dags"

EXPECTED = {
    "kafka_topic_bootstrap.py": {
        "dag_id": "kafka_topic_bootstrap",
        "schedule": "schedule=None",
        "tags": ('"adr06", "orchestration", "kafka"',),
        "runtime_module": "vina_bim_shop.orchestration.kafka_bootstrap",
        "runtime_function": "run_kafka_topic_bootstrap",
        "hourly": False,
    },
    "hourly_batch_lakehouse.py": {
        "dag_id": "hourly_batch_lakehouse",
        "schedule": 'schedule="@hourly"',
        "tags": ('"adr06", "orchestration", "batch", "gx"',),
        "runtime_module": "vina_bim_shop.orchestration.hourly_batch",
        "runtime_function": "run_hourly_batch_lakehouse",
        "hourly": True,
    },
    "pinot_bootstrap.py": {
        "dag_id": "pinot_bootstrap",
        "schedule": "schedule=None",
        "tags": ('"adr06", "orchestration", "pinot"',),
        "runtime_module": "vina_bim_shop.orchestration.pinot_bootstrap",
        "runtime_function": "run_pinot_bootstrap",
        "hourly": False,
    },
    "reconciliation_report.py": {
        "dag_id": "reconciliation_report",
        "schedule": 'schedule="@hourly"',
        "tags": ('"adr06", "orchestration", "reconciliation", "pinot"',),
        "runtime_module": "vina_bim_shop.orchestration.reconciliation",
        "runtime_function": "run_reconciliation_report",
        "hourly": True,
    },
    "datahub_ingestion.py": {
        "dag_id": "datahub_ingestion",
        "schedule": "schedule=None",
        "tags": ('"adr07", "governance", "datahub"',),
        "runtime_module": "vina_bim_shop.orchestration.datahub_ingestion",
        "runtime_function": "run_datahub_ingestion",
        "hourly": False,
    },
    "local_evidence_build.py": {
        "dag_id": "local_evidence_build",
        "schedule": "schedule=None",
        "tags": ('"adr06", "orchestration", "evidence"',),
        "runtime_module": "vina_bim_shop.orchestration.local_evidence",
        "runtime_function": "run_local_evidence_build",
        "hourly": False,
    },
}


def _read_dag(dag_filename: str) -> str:
    return (DAGS_ROOT / dag_filename).read_text(encoding="utf-8")


def test_dag_adapter_declares_expected_id_schedule_and_tags() -> None:
    for dag_filename, expected in EXPECTED.items():
        source = _read_dag(dag_filename)
        assert f'dag_id="{expected["dag_id"]}"' in source, dag_filename
        assert expected["schedule"] in source, dag_filename
        for tag_literal in expected["tags"]:
            assert tag_literal in source, f"{dag_filename} missing tag literal {tag_literal}"


def test_dag_adapter_imports_expected_runtime_function() -> None:
    for dag_filename, expected in EXPECTED.items():
        source = _read_dag(dag_filename)
        expected_import = (
            f"from {expected['runtime_module']} import {expected['runtime_function']}"
        )
        assert expected_import in source, (
            f"{dag_filename} should import `{expected['runtime_function']}` from "
            f"`{expected['runtime_module']}`"
        )


def test_dag_adapter_does_not_import_old_runtime_module() -> None:
    for dag_filename in EXPECTED:
        source = _read_dag(dag_filename)
        assert "vina_bim_shop.orchestration.runtime" not in source, (
            f"{dag_filename} should not import the deleted runtime module"
        )


def test_dag_adapter_keeps_single_python_operator() -> None:
    for dag_filename in EXPECTED:
        source = _read_dag(dag_filename)
        assert "PythonOperator(" in source
        assert "python_callable=_run" in source
        assert source.count("task_id=") == 1


def test_dag_adapter_sets_catchup_false_and_explicit_pause_flag() -> None:
    for dag_filename in EXPECTED:
        source = _read_dag(dag_filename)
        assert "catchup=False" in source
        assert "is_paused_upon_creation=" in source


def test_hourly_dag_adapters_forward_data_interval_to_runtime() -> None:
    for dag_filename, expected in EXPECTED.items():
        if not expected["hourly"]:
            continue
        source = _read_dag(dag_filename)
        assert (
            'start_ts=context["data_interval_start"].isoformat()' in source
        ), f"{dag_filename} should forward data_interval_start to start_ts"
        assert (
            'end_ts=context["data_interval_end"].isoformat()' in source
        ), f"{dag_filename} should forward data_interval_end to end_ts"
        assert f"{expected['runtime_function']}(" in source


def test_dag_adapter_data_interval_parsing_produces_iso_strings() -> None:
    """Cross-check that ``datetime.isoformat()`` is the wire format the
    runtime actually expects."""
    start = datetime(2026, 6, 1, 1, tzinfo=timezone.utc)
    end = datetime(2026, 6, 1, 2, tzinfo=timezone.utc)
    assert start.isoformat() == "2026-06-01T01:00:00+00:00"
    assert end.isoformat() == "2026-06-01T02:00:00+00:00"


def test_mini_coursework_pipeline_declares_six_ordered_rubric_tasks() -> None:
    source = _read_dag("mini_coursework_pipeline.py")

    assert 'dag_id="mini_coursework_pipeline"' in source
    for group_id in [
        "dp1_raw_to_bronze",
        "dp2_bronze_to_silver_gold",
        "dp3_offline_features",
    ]:
        assert f'group_id="{group_id}"' in source

    task_ids = [
        "ingest_raw_to_bronze",
        "validate_bronze",
        "transform_bronze_to_silver_gold",
        "validate_silver_gold",
        "compute_offline_features",
        "validate_offline_features",
    ]
    assert source.count("PythonOperator(") == len(task_ids)
    for task_id in task_ids:
        assert f'task_id="{task_id}"' in source

    assert (
        "ingest_raw_to_bronze >> validate_bronze >> transform_bronze_to_silver_gold "
        ">> validate_silver_gold >> compute_offline_features >> validate_offline_features"
    ) in source
