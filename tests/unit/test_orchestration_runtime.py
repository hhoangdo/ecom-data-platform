import importlib
import importlib.util
import json
from pathlib import Path

from compose_model import load_compose_model
from vina_bim_shop.orchestration import datahub_ingestion, hourly_batch
from vina_bim_shop.orchestration.specs import REQUIRED_DAG_IDS, dag_specs_by_id
from vina_bim_shop.quality.policies import (
    ValidationSeverity,
    gate_outcome_for_layer,
    should_fail_reconciliation,
)
from vina_bim_shop.quality.reports import ValidationReport


def test_required_airflow_dags_are_declared_with_manual_or_demo_schedules() -> None:
    dag_specs = dag_specs_by_id()

    assert REQUIRED_DAG_IDS == (
        "hourly_batch_lakehouse",
        "kafka_topic_bootstrap",
        "pinot_bootstrap",
        "datahub_ingestion",
        "reconciliation_report",
        "local_evidence_build",
        "mini_coursework_pipeline",
    )
    assert tuple(dag_specs) == REQUIRED_DAG_IDS

    assert dag_specs["kafka_topic_bootstrap"].schedule == "manual"
    assert dag_specs["pinot_bootstrap"].schedule == "manual"
    assert dag_specs["datahub_ingestion"].schedule == "manual"
    assert dag_specs["local_evidence_build"].schedule == "manual"
    assert dag_specs["hourly_batch_lakehouse"].schedule == "hourly_demo"
    assert dag_specs["reconciliation_report"].schedule == "hourly_demo"
    assert dag_specs["mini_coursework_pipeline"].schedule == "hourly_demo"


def test_required_airflow_dags_preserve_adr06_boundaries() -> None:
    dag_specs = dag_specs_by_id()

    assert dag_specs["hourly_batch_lakehouse"].supports_hourly_logical_window is True
    assert dag_specs["reconciliation_report"].supports_hourly_logical_window is True
    assert dag_specs["pinot_bootstrap"].supports_hourly_logical_window is False
    assert dag_specs["kafka_topic_bootstrap"].supports_hourly_logical_window is False

    for dag_id in REQUIRED_DAG_IDS:
        assert dag_specs[dag_id].monitors_flink is False


def test_quality_gate_policy_matches_adr06_failure_contract() -> None:
    bronze_warning = gate_outcome_for_layer("bronze_raw", success=False)
    assert bronze_warning.severity is ValidationSeverity.WARNING
    assert bronze_warning.blocks_dag is False
    assert bronze_warning.requires_quarantine is True

    silver_failure = gate_outcome_for_layer("silver", success=False)
    assert silver_failure.severity is ValidationSeverity.ERROR
    assert silver_failure.blocks_dag is True
    assert silver_failure.requires_quarantine is False

    gold_failure = gate_outcome_for_layer("gold_trino", success=False)
    assert gold_failure.severity is ValidationSeverity.ERROR
    assert gold_failure.blocks_dag is True

    pinot_warning = gate_outcome_for_layer("pinot_queries", success=False)
    assert pinot_warning.severity is ValidationSeverity.WARNING
    assert pinot_warning.blocks_dag is False

    datahub_warning = gate_outcome_for_layer("datahub", success=False)
    assert datahub_warning.severity is ValidationSeverity.WARNING
    assert datahub_warning.blocks_dag is False

    critical_datahub_failure = gate_outcome_for_layer("datahub", success=False, critical=True)
    assert critical_datahub_failure.blocks_dag is True


def test_reconciliation_is_the_only_pinot_path_that_escalates_to_failure() -> None:
    assert should_fail_reconciliation(pinot_success=False, reconciliation_success=True) is False
    assert should_fail_reconciliation(pinot_success=False, reconciliation_success=False) is True
    assert should_fail_reconciliation(pinot_success=True, reconciliation_success=False) is True


def test_pinot_bootstrap_dag_exists_without_flink_control_logic() -> None:
    repo_root = Path(__file__).resolve().parents[2]
    dag_path = repo_root / "infra" / "orchestration" / "airflow" / "dags" / "pinot_bootstrap.py"
    dag_source = dag_path.read_text(encoding="utf-8")

    assert "pinot_bootstrap" in dag_source
    assert "vina_bim_shop.flink" not in dag_source
    assert "scripts/flink/run_" not in dag_source
    assert "restart" not in dag_source.lower()


def test_prepare_spark_evidence_root_uses_root_exec_for_shared_workspace(monkeypatch, tmp_path) -> None:
    captured: dict[str, object] = {}

    def fake_run_command(command: list[str], *, cwd: Path | None = None) -> str:
        captured["command"] = command
        captured["cwd"] = cwd
        return ""

    monkeypatch.setattr(hourly_batch, "_run_command", fake_run_command)

    evidence_root = tmp_path / "runs" / "hourly_batch_lakehouse" / "spark_batch"
    hourly_batch._prepare_spark_evidence_root(evidence_root)

    command = captured["command"]
    assert isinstance(command, list)
    assert command[:7] == ["docker", "compose", "exec", "-T", "--user", "root", "spark-master"]
    assert command[7:9] == ["bash", "-lc"]
    assert "mkdir -p" in command[9]
    assert "chmod -R 0777" in command[9]
    assert evidence_root.as_posix() in command[9]
    assert captured["cwd"] == hourly_batch.REPO_ROOT


def test_prepare_gx_docs_root_uses_root_exec_for_static_site_mount(monkeypatch) -> None:
    captured: dict[str, object] = {}

    def fake_run_command(command: list[str], *, cwd: Path | None = None) -> str:
        captured["command"] = command
        captured["cwd"] = cwd
        return ""

    monkeypatch.setattr(hourly_batch, "_run_command", fake_run_command)

    hourly_batch._prepare_gx_docs_root()

    command = captured["command"]
    assert isinstance(command, list)
    assert command[:7] == ["docker", "compose", "exec", "-T", "--user", "root", "gx-docs"]
    assert command[7:9] == ["sh", "-lc"]
    assert "mkdir -p /usr/share/nginx/html" in command[9]
    assert "chmod -R 0777 /usr/share/nginx/html" in command[9]
    assert captured["cwd"] == hourly_batch.REPO_ROOT


def test_airflow_webserver_allows_slow_local_plugin_startup() -> None:
    repo_root = Path(__file__).resolve().parents[2]
    compose = load_compose_model(repo_root)
    env_vars = compose["services"]["airflow-webserver"]["environment"]

    assert int(env_vars["AIRFLOW__WEBSERVER__WEB_SERVER_MASTER_TIMEOUT"]) >= 300
    assert int(env_vars["AIRFLOW__WEBSERVER__WEB_SERVER_WORKER_TIMEOUT"]) >= 300


def test_run_datahub_ingestion_includes_all_repo_recipes(monkeypatch, tmp_path) -> None:
    calls: list[list[str]] = []

    def fake_build_run_root(_dag_id: str, _run_id: str):
        return tmp_path

    def fake_run_command(command: list[str], *, cwd: Path | None = None) -> str:
        calls.append(command)
        return "ok"

    monkeypatch.setattr(datahub_ingestion, "build_run_root", fake_build_run_root)
    monkeypatch.setattr(datahub_ingestion, "_run_command", fake_run_command)
    monkeypatch.setattr(
        datahub_ingestion,
        "_run_custom_lineage_emission",
        lambda: {
            "spark": "ok",
            "flink": "ok",
            "coursework_pipelines": {"status": "success"},
            "coursework_assertions": {"status": "success"},
        },
    )
    monkeypatch.setattr(datahub_ingestion, "_render_docs", lambda reports: reports)

    manifest = datahub_ingestion.run_datahub_ingestion(run_id="manual__2026-06-03T00:00:00+00:00")

    recipe_names = [Path(command[-1]).name for command in calls if command[:3] == ["datahub", "ingest", "run"]]
    assert recipe_names == [
        "kafka_topics.yml",
        "minio_storage.yml",
        "trino_tables.yml",
        "dbt_legacy.yml",
    ]
    assert manifest["status"] == "success"


def test_run_datahub_ingestion_fails_when_coursework_metadata_emission_fails(monkeypatch, tmp_path) -> None:
    monkeypatch.setattr(datahub_ingestion, "build_run_root", lambda _dag_id, _run_id: tmp_path)
    monkeypatch.setattr(datahub_ingestion, "_run_command", lambda command, cwd=None: "ok")
    monkeypatch.setattr(
        datahub_ingestion,
        "_run_custom_lineage_emission",
        lambda: {"coursework_pipelines": {"status": "failed", "reason": "missing assertions"}},
    )
    monkeypatch.setattr(datahub_ingestion, "_render_docs", lambda reports: reports)

    manifest = datahub_ingestion.run_datahub_ingestion(run_id="manual__coursework_failure")

    assert manifest["status"] == "failed"
    assert manifest["ingestion_results"]["custom_lineage"]["coursework_pipelines"]["status"] == "failed"


def test_run_datahub_ingestion_preserves_existing_quality_reports(monkeypatch, tmp_path) -> None:
    existing_report = ValidationReport(
        layer="bronze_raw",
        suite_name="bronze_raw_minio",
        success=False,
        status="warning",
        severity="warning",
        blocks_dag=False,
        requires_quarantine=True,
        summary="1/2 expectations passed.",
        artifacts=["quality/bronze_raw_minio.json"],
        details={
            "results": [
                {
                    "success": False,
                    "expectation_config": {
                        "type": "expect_column_values_to_not_be_null",
                        "kwargs": {"column": "path"},
                    },
                    "result": {"unexpected_count": 1},
                }
            ]
        },
    )
    quality_dir = tmp_path / "runs" / "hourly_batch_lakehouse" / "manual__2026" / "quality"
    quality_dir.mkdir(parents=True)
    (quality_dir / "bronze_raw_minio.json").write_text(
        json.dumps(existing_report.to_dict()),
        encoding="utf-8",
    )

    captured_reports: list[ValidationReport] = []

    def fake_render_docs(reports: list[ValidationReport]) -> None:
        captured_reports.extend(reports)

    monkeypatch.setattr(datahub_ingestion, "RUNS_ROOT", tmp_path / "runs")
    run_root = tmp_path / "runs" / "datahub_ingestion" / "manual__2026"
    monkeypatch.setattr(datahub_ingestion, "build_run_root", lambda _dag_id, _run_id: run_root)
    monkeypatch.setattr(datahub_ingestion, "_run_command", lambda command, cwd=None: "ok")
    monkeypatch.setattr(datahub_ingestion, "_run_custom_lineage_emission", lambda: {"spark": "ok", "flink": "ok"})
    monkeypatch.setattr(datahub_ingestion, "_render_docs", fake_render_docs)

    datahub_ingestion.run_datahub_ingestion(run_id="manual__2026-06-03T00:00:00+00:00")

    rendered_suites = {report.suite_name for report in captured_reports}
    assert rendered_suites == {"bronze_raw_minio", "datahub_ingestion"}
    assert (run_root / "run_manifest.json").is_file()


def test_legacy_hourly_wrapper_composes_the_six_coursework_stages(monkeypatch, tmp_path) -> None:
    spec = importlib.util.find_spec("vina_bim_shop.orchestration.mini_coursework_pipeline")
    assert spec is not None, "Expected the six-stage coursework pipeline runtime module."
    mini_coursework_pipeline = importlib.import_module(
        "vina_bim_shop.orchestration.mini_coursework_pipeline"
    )
    calls: list[str] = []

    monkeypatch.setattr(hourly_batch, "build_run_root", lambda _dag_id, _run_id: tmp_path)
    monkeypatch.setattr(
        hourly_batch,
        "settings_from_environment",
        lambda: mini_coursework_pipeline.CourseworkPipelineSettings.for_tests(),
    )

    for function_name in mini_coursework_pipeline.STAGE_FUNCTION_NAMES:
        def fake_stage(*, _function_name=function_name, **_kwargs):
            calls.append(_function_name)
            return {
                "stage": _function_name,
                "artifact": f"{mini_coursework_pipeline.STAGE_DETAILS[_function_name][0]}.json",
            }

        monkeypatch.setattr(hourly_batch, function_name, fake_stage)

    result = hourly_batch.run_hourly_batch_lakehouse(
        run_id="manual__2026-04-26T01:00:00+00:00",
        start_ts="2026-04-26T00:00:00+00:00",
        end_ts="2026-04-26T01:00:00+00:00",
    )

    assert calls == list(mini_coursework_pipeline.STAGE_FUNCTION_NAMES)
    assert result["manifest"]["stage_artifacts"] == [
        f"{mini_coursework_pipeline.STAGE_DETAILS[name][0]}.json"
        for name in mini_coursework_pipeline.STAGE_FUNCTION_NAMES
    ]


def test_coursework_dp1_copy_uses_airflow_minio_client(monkeypatch, tmp_path) -> None:
    pipeline = importlib.import_module("vina_bim_shop.orchestration.mini_coursework_pipeline")
    source = tmp_path / "customers" / "part-000.parquet"
    source.parent.mkdir()
    source.write_text("stub", encoding="utf-8")
    commands: list[list[str]] = []

    monkeypatch.setattr(pipeline, "_run_command", lambda command: commands.append(command) or "")

    pipeline._copy_to_bronze(
        source=source,
        object_key="bronze/batch/customers/snapshot_date=2026-04-26/part-000.parquet",
        settings=pipeline.CourseworkPipelineSettings.for_tests(),
    )

    assert commands == [
        [
            "mc",
            "alias",
            "set",
            "coursework",
            "http://minio:9000",
            "vina_minio",
            "vina_minio_password",
        ],
        [
            "mc",
            "cp",
            str(source),
            "coursework/bronze/batch/customers/snapshot_date=2026-04-26/part-000.parquet",
        ],
    ]


def test_coursework_spark_stages_run_from_the_compose_project_root(monkeypatch, tmp_path) -> None:
    pipeline = importlib.import_module("vina_bim_shop.orchestration.mini_coursework_pipeline")
    observed_working_directories: list[Path] = []

    def fake_spark_stage(**_kwargs):
        observed_working_directories.append(Path.cwd())
        return {"spark_stdout": "ok"}

    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(pipeline, "_prepare_spark_evidence_root", lambda _path: None)
    monkeypatch.setattr(pipeline, "run_core_transform", fake_spark_stage)
    monkeypatch.setattr(pipeline, "run_feature_compute", fake_spark_stage)
    monkeypatch.setattr(pipeline, "_latest_spark_application_id", lambda: "app-123")
    monkeypatch.setattr(
        pipeline,
        "execute_trino_query",
        lambda _query, **_kwargs: {"rows": [["event_timestamp"], ["created"]]},
    )

    run_root = tmp_path / "run"
    settings = pipeline.CourseworkPipelineSettings.for_tests()
    pipeline.transform_bronze_to_silver_gold(
        run_id="manual__2026-04-26T01:00:00+00:00",
        start_ts="2026-04-26T00:00:00+00:00",
        end_ts="2026-04-26T01:00:00+00:00",
        settings=settings,
        run_root=run_root,
    )
    pipeline.compute_offline_features(
        run_id="manual__2026-04-26T01:00:00+00:00",
        start_ts="2026-04-26T00:00:00+00:00",
        end_ts="2026-04-26T01:00:00+00:00",
        settings=settings,
        run_root=run_root,
    )

    assert observed_working_directories == [pipeline.REPO_ROOT, pipeline.REPO_ROOT]
