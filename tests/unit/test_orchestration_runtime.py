from pathlib import Path

from vina_bim_shop.orchestration import runtime
from vina_bim_shop.orchestration.specs import REQUIRED_DAG_IDS, dag_specs_by_id
from vina_bim_shop.quality.policies import (
    ValidationSeverity,
    gate_outcome_for_layer,
    should_fail_reconciliation,
)


def test_required_airflow_dags_are_declared_with_manual_or_demo_schedules() -> None:
    dag_specs = dag_specs_by_id()

    assert REQUIRED_DAG_IDS == (
        "hourly_batch_lakehouse",
        "kafka_topic_bootstrap",
        "pinot_bootstrap",
        "datahub_ingestion",
        "reconciliation_report",
        "local_evidence_build",
    )
    assert tuple(dag_specs) == REQUIRED_DAG_IDS

    assert dag_specs["kafka_topic_bootstrap"].schedule == "manual"
    assert dag_specs["pinot_bootstrap"].schedule == "manual"
    assert dag_specs["datahub_ingestion"].schedule == "manual"
    assert dag_specs["local_evidence_build"].schedule == "manual"
    assert dag_specs["hourly_batch_lakehouse"].schedule == "hourly_demo"
    assert dag_specs["reconciliation_report"].schedule == "hourly_demo"


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

    monkeypatch.setattr(runtime, "_run_command", fake_run_command)

    evidence_root = tmp_path / "runs" / "hourly_batch_lakehouse" / "spark_batch"
    runtime._prepare_spark_evidence_root(evidence_root)

    command = captured["command"]
    assert isinstance(command, list)
    assert command[:7] == ["docker", "compose", "exec", "-T", "--user", "root", "spark-master"]
    assert command[7:9] == ["bash", "-lc"]
    assert "mkdir -p" in command[9]
    assert "chmod -R 0777" in command[9]
    assert evidence_root.as_posix() in command[9]
    assert captured["cwd"] == runtime.REPO_ROOT


def test_prepare_gx_docs_root_uses_root_exec_for_static_site_mount(monkeypatch) -> None:
    captured: dict[str, object] = {}

    def fake_run_command(command: list[str], *, cwd: Path | None = None) -> str:
        captured["command"] = command
        captured["cwd"] = cwd
        return ""

    monkeypatch.setattr(runtime, "_run_command", fake_run_command)

    runtime._prepare_gx_docs_root()

    command = captured["command"]
    assert isinstance(command, list)
    assert command[:7] == ["docker", "compose", "exec", "-T", "--user", "root", "gx-docs"]
    assert command[7:9] == ["sh", "-lc"]
    assert "mkdir -p /usr/share/nginx/html" in command[9]
    assert "chmod -R 0777 /usr/share/nginx/html" in command[9]
    assert captured["cwd"] == runtime.REPO_ROOT


def test_airflow_webserver_allows_slow_local_plugin_startup() -> None:
    import yaml

    repo_root = Path(__file__).resolve().parents[2]
    compose = yaml.safe_load((repo_root / "docker-compose.yml").read_text(encoding="utf-8"))
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

    monkeypatch.setattr(runtime, "build_run_root", fake_build_run_root)
    monkeypatch.setattr(runtime, "_run_command", fake_run_command)
    monkeypatch.setattr(runtime, "_run_custom_lineage_emission", lambda: {"spark": "ok", "flink": "ok"})
    monkeypatch.setattr(runtime, "_render_docs", lambda reports: reports)

    manifest = runtime.run_datahub_ingestion(run_id="manual__2026-06-03T00:00:00+00:00")

    recipe_names = [Path(command[-1]).name for command in calls if command[:3] == ["datahub", "ingest", "run"]]
    assert recipe_names == [
        "kafka_topics.yml",
        "minio_storage.yml",
        "trino_tables.yml",
        "dbt_legacy.yml",
    ]
    assert manifest["status"] == "success"


def test_run_datahub_ingestion_preserves_existing_quality_reports(monkeypatch, tmp_path) -> None:
    existing_report = runtime.ValidationReport(
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
        runtime.json.dumps(existing_report.to_dict()),
        encoding="utf-8",
    )

    captured_reports: list[runtime.ValidationReport] = []

    def fake_render_docs(reports: list[runtime.ValidationReport]) -> None:
        captured_reports.extend(reports)

    monkeypatch.setattr(runtime, "RUNS_ROOT", tmp_path / "runs")
    monkeypatch.setattr(runtime, "_run_command", lambda command, cwd=None: "ok")
    monkeypatch.setattr(runtime, "_run_custom_lineage_emission", lambda: {"spark": "ok", "flink": "ok"})
    monkeypatch.setattr(runtime, "_render_docs", fake_render_docs)

    runtime.run_datahub_ingestion(run_id="manual__2026-06-03T00:00:00+00:00")

    rendered_suites = {report.suite_name for report in captured_reports}
    assert rendered_suites == {"bronze_raw_minio", "datahub_ingestion"}


def test_emit_datajob_lineage_emits_datajob_info_and_input_output(monkeypatch) -> None:
    """v2 DataJob lineage must emit DataJobInfoClass + DataJobInputOutputClass per job."""
    pytest = __import__("pytest")
    datahub = pytest.importorskip("datahub")

    from datahub.metadata.schema_classes import DataJobInfoClass, DataJobInputOutputClass

    from vina_bim_shop.datahub_lineage import datajob_lineage

    captured: list = []

    class MockRestEmitter:
        def emit(self, mcp) -> None:
            captured.append(mcp)

    monkeypatch.setattr(datajob_lineage, "DataHubRestEmitter", MockRestEmitter)

    results = datajob_lineage.emit_datajob_lineage("http://fake-gms:8080")

    info_aspects = [mcp.aspect for mcp in captured if isinstance(mcp.aspect, DataJobInfoClass)]
    io_aspects = [mcp.aspect for mcp in captured if isinstance(mcp.aspect, DataJobInputOutputClass)]

    assert info_aspects, "Expected at least one DataJobInfoClass"
    assert io_aspects, "Expected at least one DataJobInputOutputClass"

    fact_order_jobs = [
        mcp
        for mcp in captured
        if isinstance(mcp.aspect, DataJobInputOutputClass)
        and mcp.aspect.outputDatasets
        and "fact_order" in mcp.aspect.outputDatasets[0]
    ]
    assert len(fact_order_jobs) == 1
    assert len(fact_order_jobs[0].aspect.inputDatasets) > 0
    assert any("stg_orders" in urn for urn in fact_order_jobs[0].aspect.inputDatasets)

    job_urns = [mcp.entityUrn for mcp in captured]
    assert all(urn.startswith("urn:li:dataJob:(urn:li:dataFlow:") for urn in job_urns)
    assert all(urn.endswith("success") for urn in results.values()) or all(
        v == "success" or v.startswith("warning:") for v in results.values()
    )
    assert all(v == "success" for v in results.values())


def test_emit_datajob_lineage_swallows_per_job_errors(monkeypatch) -> None:
    """A failure on one job must not abort emission of the others."""
    pytest = __import__("pytest")
    pytest.importorskip("datahub")

    from vina_bim_shop.datahub_lineage import datajob_lineage

    call_count = {"n": 0}

    class FlakyEmitter:
        def emit(self, mcp) -> None:
            call_count["n"] += 1
            if call_count["n"] == 1:
                raise RuntimeError("simulated GMS outage")

    monkeypatch.setattr(datajob_lineage, "DataHubRestEmitter", FlakyEmitter)

    results = datajob_lineage.emit_datajob_lineage("http://fake-gms:8080")

    assert any(v.startswith("warning:") for v in results.values())
    assert any(v == "success" for v in results.values())
    assert call_count["n"] > 1


def test_run_custom_lineage_emission_includes_datajob_block(monkeypatch) -> None:
    """_run_custom_lineage_emission must invoke the v2 emitter and surface its result."""
    pytest = __import__("pytest")
    pytest.importorskip("datahub")

    from vina_bim_shop.datahub_lineage import (
        datajob_lineage,
        flink_lineage,
        gx_assertions,
        spark_lineage,
    )
    from vina_bim_shop.orchestration import runtime

    captured: dict[str, object] = {}

    def fake_emit_datajob_lineage(gms_url: str) -> dict[str, str]:
        captured["gms_url"] = gms_url
        return {
            "urn:li:dataJob:(urn:li:dataFlow:(spark,vina-bim-shop-batch,local),iceberg_transform_fact_order)": "success"
        }

    monkeypatch.setattr(datajob_lineage, "emit_datajob_lineage", fake_emit_datajob_lineage)
    monkeypatch.setattr(spark_lineage, "emit_spark_batch_lineage", lambda gms_url: {"fact_order": "success"})
    monkeypatch.setattr(flink_lineage, "emit_flink_streaming_lineage", lambda gms_url: {"realtime_commerce_metrics_1m": "success"})
    monkeypatch.setattr(gx_assertions, "emit_gx_assertions_to_datahub", lambda gms_url: {"assertions_emitted": 8})

    results = runtime._run_custom_lineage_emission()

    assert "datajob" in results
    assert captured["gms_url"] == "http://datahub-gms:8080"
    assert "fact_order" in next(iter(results["datajob"].keys()))
    assert "spark" in results
    assert "flink" in results
    assert "gx_assertions" in results
