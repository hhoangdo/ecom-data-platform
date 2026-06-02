import importlib.util
import json
import sys
from types import SimpleNamespace
from pathlib import Path

import pytest
import yaml

from vina_bim_shop.lakehouse.spark.constants import REQUIRED_GOLD_TABLES
from vina_bim_shop.lakehouse.spark.evidence import DEFAULT_EVIDENCE_ROOT, capture_evidence
from vina_bim_shop.lakehouse.spark.parity import run_parity_checks
from vina_bim_shop.lakehouse.spark.runner import _run_command, build_spark_submit_command
from vina_bim_shop.lakehouse.spark.trino import run_gold_smoke_queries
from vina_bim_shop.lakehouse.spark.window import BatchWindow


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[2]


def _load_script_module(script_relative_path: str, module_name: str):
    script_path = _repo_root() / script_relative_path
    assert script_path.is_file(), f"Expected script at {script_relative_path}."
    spec = importlib.util.spec_from_file_location(module_name, script_path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_root_compose_declares_batch_services_and_ui_ports() -> None:
    compose = yaml.safe_load((_repo_root() / "docker-compose.yml").read_text(encoding="utf-8"))

    services = compose["services"]
    expected_services = {"spark-master", "spark-worker", "spark-history-server"}
    assert expected_services.issubset(services)

    for service_name in expected_services:
        assert services[service_name]["profiles"] == ["batch", "all"]

    assert services["spark-master"]["command"] == ["master"]
    assert "8085:8080" in services["spark-master"]["ports"]
    assert services["spark-worker"]["command"] == ["worker"]
    assert "ports" not in services["spark-worker"]
    assert services["spark-history-server"]["command"] == ["history"]
    assert "18080:18080" in services["spark-history-server"]["ports"]
    assert "spark-master" in services["spark-worker"]["depends_on"]
    assert "spark-master" in services["spark-history-server"]["depends_on"]


def test_env_example_documents_spark_batch_urls() -> None:
    env_example = (_repo_root() / ".env.example").read_text(encoding="utf-8")

    for expected_line in [
        "VBS_SPARK_MASTER_URL=spark://spark-master:7077",
        "VBS_SPARK_MASTER_UI_URL=http://localhost:8085",
        "VBS_SPARK_HISTORY_URL=http://localhost:18080",
    ]:
        assert expected_line in env_example


def test_spark_entrypoint_configures_iceberg_minio_and_history_server() -> None:
    entrypoint = (_repo_root() / "infra" / "spark" / "bin" / "entrypoint.sh").read_text(encoding="utf-8")

    expected_lines = [
        "export PYSPARK_PYTHON=python3",
        "export PYSPARK_DRIVER_PYTHON=python3",
        "spark.eventLog.dir s3a://${VBS_CHECKPOINTS_BUCKET:-checkpoints}/spark-events",
        "spark.history.fs.logDirectory s3a://${VBS_CHECKPOINTS_BUCKET:-checkpoints}/spark-events",
        "spark.sql.catalog.iceberg org.apache.iceberg.spark.SparkCatalog",
        "spark.sql.catalog.iceberg.type hive",
        "spark.sql.catalog.iceberg.uri ${VBS_HIVE_METASTORE_INTERNAL_URI:-thrift://hive-metastore:9083}",
        "spark.sql.catalog.iceberg.warehouse s3a://${VBS_SILVER_BUCKET:-silver}/warehouse",
        "spark.hadoop.fs.s3a.endpoint ${VBS_MINIO_INTERNAL_ENDPOINT:-http://minio:9000}",
        "spark.hadoop.fs.s3a.path.style.access true",
        "spark.hadoop.fs.s3a.connection.ssl.enabled false",
        "--webui-port 8080",
        "--webui-port 8081",
        "org.apache.spark.deploy.history.HistoryServer",
    ]
    for expected_line in expected_lines:
        assert expected_line in entrypoint


def test_spark_dockerfile_bundles_iceberg_s3a_and_gx_dependencies() -> None:
    dockerfile = (_repo_root() / "infra" / "spark" / "Dockerfile").read_text(encoding="utf-8")

    for expected_fragment in [
        "FROM apache/spark:4.0.0",
        "iceberg-spark-runtime-4.0_2.13-${ICEBERG_VERSION}.jar",
        "hadoop-aws-${HADOOP_AWS_VERSION}.jar",
        "bundle-${AWS_BUNDLE_VERSION}.jar",
        "python3 -m pip install --no-cache-dir",
        "ln -sf /usr/bin/python3 /usr/local/bin/python",
        "\"great-expectations==1.17.2\"",
        "\"pyarrow==19.0.1\"",
    ]:
        assert expected_fragment in dockerfile


def test_batch_window_normalizes_utc_hourly_ranges() -> None:
    window = BatchWindow.from_args(
        start_ts="2026-06-01T00:00:00+07:00",
        end_ts="2026-06-01T01:00:00+07:00",
        mode="hourly",
    )

    assert window.start_ts.isoformat() == "2026-05-31T17:00:00+00:00"
    assert window.end_ts.isoformat() == "2026-05-31T18:00:00+00:00"
    assert window.to_cli_args() == [
        "--start-ts",
        "2026-05-31T17:00:00Z",
        "--end-ts",
        "2026-05-31T18:00:00Z",
        "--mode",
        "hourly",
    ]


def test_batch_window_rejects_non_hourly_hourly_range() -> None:
    with pytest.raises(ValueError, match="exact one-hour UTC window"):
        BatchWindow.from_args(
            start_ts="2026-06-01T00:00:00Z",
            end_ts="2026-06-01T00:30:00Z",
            mode="hourly",
        )


def test_build_spark_submit_command_uses_containerized_workspace_paths() -> None:
    window = BatchWindow.from_args(
        start_ts="2026-06-01T00:00:00Z",
        end_ts="2026-06-01T01:00:00Z",
        mode="hourly",
    )

    command = build_spark_submit_command(window, evidence_root="evidence/05_spark_batch")

    assert command[:5] == ["docker", "compose", "exec", "-T", "spark-master"]
    command_text = command[-1]
    assert "cd /workspace" in command_text
    assert "PYTHONPATH=/workspace/src spark-submit" in command_text
    assert "scripts/spark/job.py" in command_text
    assert "--start-ts 2026-06-01T00:00:00Z" in command_text
    assert "--end-ts 2026-06-01T01:00:00Z" in command_text
    assert "--mode hourly" in command_text
    assert "--evidence-root /workspace/evidence/05_spark_batch" in command_text


def test_run_command_uses_utf8_with_replacement(monkeypatch) -> None:
    recorded = {}

    def fake_run(command, **kwargs):
        recorded["command"] = command
        recorded["kwargs"] = kwargs
        return "ok"

    monkeypatch.setattr("vina_bim_shop.lakehouse.spark.runner.subprocess.run", fake_run)

    result = _run_command(["echo", "hello"])

    assert result == "ok"
    assert recorded == {
        "command": ["echo", "hello"],
        "kwargs": {
            "check": True,
            "text": True,
            "capture_output": True,
            "encoding": "utf-8",
            "errors": "replace",
        },
    }


def test_capture_evidence_writes_manifest_and_screenshot_placeholders(tmp_path: Path) -> None:
    screenshots = {}

    def fake_get_json(url: str):
        if url.endswith("/json/"):
            return {"status": "ALIVE", "workers": 1}
        if url.endswith("/api/v1/applications"):
            return [{"id": "app-001", "name": "vina-bim-shop-batch"}]
        raise AssertionError(f"Unexpected URL: {url}")

    def fake_screenshot_capturer(*, master_url: str, history_url: str, screenshots_path: Path) -> dict[str, str]:
        screenshots["master_url"] = master_url
        screenshots["history_url"] = history_url
        screenshots["path"] = str(screenshots_path)
        for filename in ["spark_master_ui.png", "spark_history_server.png"]:
            (screenshots_path / filename).write_text("stub", encoding="utf-8")
        return {
            "spark_master_ui": str((screenshots_path / "spark_master_ui.png").resolve()),
            "spark_history_server": str((screenshots_path / "spark_history_server.png").resolve()),
        }

    manifest = capture_evidence(
        evidence_root=tmp_path,
        master_url="http://localhost:8085",
        history_url="http://localhost:18080",
        get_json=fake_get_json,
        screenshot_capturer=fake_screenshot_capturer,
    )

    assert DEFAULT_EVIDENCE_ROOT == Path("evidence/05_spark_batch")
    assert json.loads((tmp_path / "spark_master_status.json").read_text(encoding="utf-8"))["status"] == "ALIVE"
    assert json.loads((tmp_path / "spark_history_applications.json").read_text(encoding="utf-8"))[0]["id"] == "app-001"
    assert (tmp_path / "screenshots" / "README.md").is_file()
    assert (tmp_path / "screenshots" / "spark_master_ui.png").is_file()
    assert (tmp_path / "screenshots" / "spark_history_server.png").is_file()
    assert "spark_master_status.json" in manifest["artifacts"]
    assert "screenshots/spark_master_ui.png" in manifest["artifacts"]
    assert screenshots == {
        "master_url": "http://localhost:8085",
        "history_url": "http://localhost:18080",
        "path": str(tmp_path / "screenshots"),
    }


def test_run_gold_smoke_queries_writes_results_artifact(tmp_path: Path, monkeypatch) -> None:
    calls = []

    def fake_execute_trino_query(query: str, *, trino_url: str, user: str):
        calls.append((query, trino_url, user))
        return {"query": query, "columns": ["value"], "rows": [[1]], "stats": {}}

    monkeypatch.setattr("vina_bim_shop.lakehouse.spark.trino.execute_trino_query", fake_execute_trino_query)
    results = run_gold_smoke_queries(evidence_root=tmp_path, trino_url="http://localhost:8080", user="vina_analyst")

    assert set(results) == {
        "gold_inventory",
        "fact_order_count",
        "fact_order_paid_revenue",
        "hourly_kpi_sample",
    }
    assert len(calls) == 4
    assert json.loads((tmp_path / "trino_gold_smoke_results.json").read_text(encoding="utf-8"))["fact_order_count"]["rows"] == [[1]]


def test_run_parity_checks_writes_json_and_markdown_reports(tmp_path: Path, monkeypatch) -> None:
    duckdb_path = tmp_path / "vina_bim_shop.duckdb"
    import duckdb

    connection = duckdb.connect(str(duckdb_path))
    connection.execute("create schema gold")
    for table_name in REQUIRED_GOLD_TABLES:
        if table_name == "fact_order":
            connection.execute(
                "create table gold.fact_order (official_paid_revenue double, gross_merchandise_value double)"
            )
        elif table_name == "fact_order_item":
            connection.execute("create table gold.fact_order_item (estimated_cost double, estimated_margin double)")
        elif table_name == "agg_hourly_reconciled_kpi":
            connection.execute("create table gold.agg_hourly_reconciled_kpi (official_paid_revenue double)")
        else:
            connection.execute(f"create table gold.{table_name} (dummy integer)")
    connection.close()

    def fake_execute_trino_query(query: str, *, trino_url: str, user: str):
        scalar = None if "sum(" in query.lower() else 0
        return {"query": query, "columns": ["value"], "rows": [[scalar]], "stats": {}}

    monkeypatch.setattr("vina_bim_shop.lakehouse.spark.parity.execute_trino_query", fake_execute_trino_query)
    report = run_parity_checks(
        evidence_root=tmp_path,
        duckdb_path=duckdb_path,
        trino_url="http://localhost:8080",
        user="vina_analyst",
    )

    assert report["success"] is True
    assert (tmp_path / "dbt_parity_report.json").is_file()
    assert (tmp_path / "dbt_parity_report.md").is_file()
    assert "Overall success: PASS" in (tmp_path / "dbt_parity_report.md").read_text(encoding="utf-8")


def test_required_gold_table_inventory_matches_adr03_scope() -> None:
    assert len(REQUIRED_GOLD_TABLES) == len(set(REQUIRED_GOLD_TABLES))
    assert REQUIRED_GOLD_TABLES == (
        "dim_customer",
        "dim_seller",
        "dim_product",
        "dim_category",
        "dim_date",
        "dim_payment_method",
        "dim_order_status",
        "dim_shipment_status",
        "dim_shipping_method",
        "dim_promotion",
        "bridge_product_category",
        "fact_order",
        "fact_order_item",
        "fact_payment_attempt",
        "fact_shipment",
        "fact_inventory_snapshot",
        "fact_promotion_application",
        "obt_order_performance",
        "agg_hourly_reconciled_kpi",
        "feat_customer_90d",
        "feat_stream_60m",
        "feat_customer_unified",
    )


def test_spark_batch_smoke_sql_reads_gold_tables_only() -> None:
    smoke_sql = (_repo_root() / "infra" / "lakehouse" / "trino" / "sql" / "spark_batch_smoke.sql").read_text(
        encoding="utf-8"
    )

    assert "SHOW TABLES FROM iceberg.gold" in smoke_sql
    assert "iceberg.gold.fact_order" in smoke_sql
    assert "iceberg.gold.agg_hourly_reconciled_kpi" in smoke_sql
    assert "INSERT INTO" not in smoke_sql
    assert "CREATE TABLE" not in smoke_sql
    assert "DROP TABLE" not in smoke_sql


def test_run_batch_script_parses_args_and_prints_summary(monkeypatch, capsys, tmp_path: Path) -> None:
    module = _load_script_module("scripts/spark/run_batch.py", "run_batch_script")

    monkeypatch.setattr(
        sys,
        "argv",
        [
            "run_batch.py",
            "--start-ts",
            "2026-06-01T00:00:00Z",
            "--end-ts",
            "2026-06-01T01:00:00Z",
            "--mode",
            "hourly",
            "--evidence-root",
            str(tmp_path),
        ],
    )
    args = module.parse_args()
    assert args.start_ts == "2026-06-01T00:00:00Z"
    assert args.end_ts == "2026-06-01T01:00:00Z"
    assert args.mode == "hourly"
    assert args.evidence_root == str(tmp_path)

    calls = []

    def fake_run_batch_pipeline(**kwargs):
        calls.append(kwargs)
        return {
            "window": {
                "start_ts": kwargs["start_ts"],
                "end_ts": kwargs["end_ts"],
                "mode": kwargs["mode"],
            }
        }

    monkeypatch.setattr(module, "run_batch_pipeline", fake_run_batch_pipeline)
    module.main()

    assert calls == [
        {
            "start_ts": "2026-06-01T00:00:00Z",
            "end_ts": "2026-06-01T01:00:00Z",
            "mode": "hourly",
            "evidence_root": str(tmp_path),
        }
    ]
    assert capsys.readouterr().out.strip() == (
        "Spark batch completed for 2026-06-01T00:00:00Z -> 2026-06-01T01:00:00Z (hourly)."
    )


def test_run_batch_pipeline_accepts_custom_capture_evidence_function(monkeypatch, tmp_path: Path) -> None:
    from vina_bim_shop.lakehouse.spark.runner import run_batch_pipeline

    commands = []
    captured = {}

    def fake_run_command(command):
        commands.append(command)
        return SimpleNamespace(stdout="ok")

    def fake_run_parity_checks(*, evidence_root):
        assert Path(evidence_root) == tmp_path
        return {"success": True}

    def fake_run_gold_smoke_queries(*, evidence_root):
        assert Path(evidence_root) == tmp_path
        return {"fact_order_count": {"rows": [[1]]}}

    def fake_capture_evidence_fn(*, evidence_root):
        captured["evidence_root"] = Path(evidence_root)
        return {"artifacts": ["screenshots/README.md"]}

    monkeypatch.setattr("vina_bim_shop.lakehouse.spark.runner.run_parity_checks", fake_run_parity_checks)
    monkeypatch.setattr("vina_bim_shop.lakehouse.spark.runner.run_gold_smoke_queries", fake_run_gold_smoke_queries)

    summary = run_batch_pipeline(
        start_ts="2026-06-01T00:00:00Z",
        end_ts="2026-06-01T01:00:00Z",
        mode="hourly",
        evidence_root=tmp_path,
        run_command=fake_run_command,
        capture_evidence_fn=fake_capture_evidence_fn,
    )

    assert len(commands) == 2
    assert captured["evidence_root"] == tmp_path
    assert summary["evidence_artifact_count"] == 1
