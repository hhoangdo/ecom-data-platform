import importlib.util
import json
import sys
from types import SimpleNamespace
from pathlib import Path

import pytest

from compose_model import load_compose_model
from vina_bim_shop.lakehouse.spark.constants import REQUIRED_GOLD_TABLES
from vina_bim_shop.lakehouse.spark.evidence import DEFAULT_EVIDENCE_ROOT, capture_evidence
from vina_bim_shop.lakehouse.spark.layout_profile import (
    COMPACTION_EVIDENCE_BUCKET_COUNT,
    COMPACTION_EVIDENCE_LAYOUT_PROFILE,
    STANDARD_LAYOUT_PROFILE,
    compaction_evidence_target,
    validate_layout_profile,
)
from vina_bim_shop.lakehouse.spark.parity import run_parity_checks
from vina_bim_shop.lakehouse.spark.runner import _run_command, build_spark_submit_command
from vina_bim_shop.lakehouse.spark import sql as spark_sql
from vina_bim_shop.lakehouse.spark.sql import ordered_gold_queries
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
    compose = load_compose_model(_repo_root())

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
    assert "--conf spark.eventLog.enabled=true" in command_text
    assert "--conf spark.eventLog.dir=s3a://checkpoints/spark-events" in command_text
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


def test_capture_evidence_writes_machine_verifiable_manifest(tmp_path: Path) -> None:
    def fake_get_json(url: str):
        if url.endswith("/json/"):
            return {"status": "ALIVE", "workers": 1}
        if url.endswith("/api/v1/applications"):
            return [{"id": "app-001", "name": "vina-bim-shop-batch"}]
        raise AssertionError(f"Unexpected URL: {url}")

    manifest = capture_evidence(
        evidence_root=tmp_path,
        master_url="http://localhost:8085",
        history_url="http://localhost:18080",
        get_json=fake_get_json,
    )

    assert DEFAULT_EVIDENCE_ROOT == Path("evidence/05_spark_batch")
    assert json.loads((tmp_path / "spark_master_status.json").read_text(encoding="utf-8"))["status"] == "ALIVE"
    assert json.loads((tmp_path / "spark_history_applications.json").read_text(encoding="utf-8"))[0]["id"] == "app-001"
    assert "spark_master_status.json" in manifest["artifacts"]
    assert not (tmp_path / "screenshots").exists()
    assert all(not artifact.startswith("screenshots/") for artifact in manifest["artifacts"])


def test_capture_evidence_includes_optimization_manifest_when_present(tmp_path: Path) -> None:
    optimization = tmp_path / "optimization"
    optimization.mkdir()
    (optimization / "run_manifest.json").write_text("{}", encoding="utf-8")

    def fake_get_json(url: str):
        if url.endswith("/json/"):
            return {"status": "ALIVE"}
        if url.endswith("/api/v1/applications"):
            return []
        raise AssertionError(f"Unexpected URL: {url}")

    manifest = capture_evidence(evidence_root=tmp_path, get_json=fake_get_json)

    assert "optimization/run_manifest.json" in manifest["artifacts"]


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


def test_execute_trino_query_preserves_column_metadata() -> None:
    from vina_bim_shop.lakehouse.spark.trino import execute_trino_query

    class FakeResponse:
        def raise_for_status(self) -> None:
            return None

        def json(self) -> dict:
            return {
                "columns": [
                    {"name": "table_name", "type": "varchar"},
                    {"name": "row_count", "type": "bigint"},
                ],
                "data": [["fact_order", 1800]],
                "stats": {"state": "FINISHED"},
            }

    result = execute_trino_query(
        "select * from iceberg.gold.fact_order",
        trino_url="http://localhost:8080",
        user="vina_analyst",
        post=lambda *args, **kwargs: FakeResponse(),
    )

    assert result["columns"] == ["table_name", "row_count"]
    assert result["column_metadata"] == [
        {"name": "table_name", "type": "varchar"},
        {"name": "row_count", "type": "bigint"},
    ]


def test_export_executive_mart_copies_all_gold_tables_from_trino(tmp_path: Path) -> None:
    import duckdb

    from vina_bim_shop.lakehouse.spark.executive_mart import export_executive_mart

    duckdb_path = tmp_path / "vina_bim_shop_executive.duckdb"
    evidence_root = tmp_path / "evidence"
    calls = []

    def fake_execute_query(query: str, *, trino_url: str, user: str):
        table_name = query.rsplit("iceberg.gold.", 1)[1].strip()
        calls.append((table_name, trino_url, user))
        return {
            "query": query,
            "columns": ["table_name", "row_number"],
            "column_metadata": [
                {"name": "table_name", "type": "varchar"},
                {"name": "row_number", "type": "bigint"},
            ],
            "rows": [[table_name, 1]],
            "stats": {"state": "FINISHED"},
        }

    manifest = export_executive_mart(
        duckdb_path=duckdb_path,
        evidence_root=evidence_root,
        trino_url="http://trino:8080",
        user="vina_analyst",
        execute_query=fake_execute_query,
        exported_at="2026-06-06T00:00:00+00:00",
    )

    assert duckdb_path.is_file()
    assert manifest["duckdb_path"] == str(duckdb_path)
    assert manifest["table_count"] == len(REQUIRED_GOLD_TABLES)
    assert manifest["total_row_count"] == len(REQUIRED_GOLD_TABLES)
    assert [call[0] for call in calls] == list(REQUIRED_GOLD_TABLES)

    with duckdb.connect(str(duckdb_path), read_only=True) as connection:
        gold_table_count = connection.execute(
            """
            select count(*)
            from information_schema.tables
            where table_schema = 'gold'
            """
        ).fetchone()[0]
        metadata_table_count = connection.execute("select count(*) from mart_metadata.table_manifest").fetchone()[0]
        fact_order_rows = connection.execute("select table_name, row_number from gold.fact_order").fetchall()

    assert gold_table_count == len(REQUIRED_GOLD_TABLES)
    assert metadata_table_count == len(REQUIRED_GOLD_TABLES)
    assert fact_order_rows == [("fact_order", 1)]
    assert (evidence_root / "executive_mart_export_manifest.json").is_file()
    assert "DuckDB Executive Mart Export Report" in (
        evidence_root / "executive_mart_export_report.md"
    ).read_text(encoding="utf-8")


def test_export_executive_mart_script_parses_args_and_prints_summary(monkeypatch, capsys, tmp_path: Path) -> None:
    module = _load_script_module("scripts/spark/export_executive_mart.py", "export_executive_mart_script")

    duckdb_path = tmp_path / "executive.duckdb"
    evidence_root = tmp_path / "evidence"
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "export_executive_mart.py",
            "--duckdb-path",
            str(duckdb_path),
            "--evidence-root",
            str(evidence_root),
        ],
    )
    args = module.parse_args()
    assert args.duckdb_path == str(duckdb_path)
    assert args.evidence_root == str(evidence_root)

    calls = []

    def fake_export_executive_mart(**kwargs):
        calls.append(kwargs)
        return {"duckdb_path": str(duckdb_path), "table_count": 22, "total_row_count": 123}

    monkeypatch.setattr(module, "export_executive_mart", fake_export_executive_mart)
    module.main()

    assert calls == [{"duckdb_path": str(duckdb_path), "evidence_root": str(evidence_root)}]
    assert capsys.readouterr().out.strip() == (
        f"Exported 22 Gold tables and 123 rows to {duckdb_path}."
    )


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


def test_spark_feature_queries_expose_created_output_contract() -> None:
    queries = dict(ordered_gold_queries())

    assert "max(fo.created_ts) as created" in queries["feat_customer_90d"]
    assert "\n  created\nfrom customer_orders" in queries["feat_customer_90d"]
    assert " as created_ts" not in queries["feat_customer_90d"]

    assert "max(created_ts) as created" in queries["feat_stream_60m"]
    assert " as created_ts" not in queries["feat_stream_60m"]

    assert "greatest(c.created, coalesce(s.created, c.created)) as created" in queries["feat_customer_unified"]
    assert "created_ts" not in queries["feat_customer_unified"]


def test_compaction_evidence_layout_profile_is_two_bucket_and_targeted() -> None:
    assert STANDARD_LAYOUT_PROFILE == "standard"
    assert COMPACTION_EVIDENCE_LAYOUT_PROFILE == "compaction-evidence"
    assert COMPACTION_EVIDENCE_BUCKET_COUNT == 2
    assert compaction_evidence_target("fact_order") == ("order_id", "order_date_key")
    assert compaction_evidence_target("fact_order_item") == ("order_item_id", "order_date_key")
    assert compaction_evidence_target("dim_customer") is None

    with pytest.raises(ValueError, match="Unsupported Spark layout profile"):
        validate_layout_profile("small-files")


def test_canonical_job_exposes_the_opt_in_compaction_evidence_profile() -> None:
    source = (_repo_root() / "src" / "vina_bim_shop" / "lakehouse" / "spark" / "job.py").read_text(
        encoding="utf-8"
    )

    assert '"--layout-profile"' in source
    assert "COMPACTION_EVIDENCE_LAYOUT_PROFILE" in source
    assert "repartitionByRange" in source
    assert "spark.sql.iceberg.distribution-mode" in source
    assert "compaction_layout_manifest.json" in source
    assert "layout_profile=args.layout_profile" in source


def test_gold_query_groups_partition_the_existing_combined_contract() -> None:
    assert hasattr(spark_sql, "ordered_core_gold_queries")
    assert hasattr(spark_sql, "ordered_feature_queries")

    core = spark_sql.ordered_core_gold_queries()
    features = spark_sql.ordered_feature_queries()

    assert core + features == ordered_gold_queries()
    assert [name for name, _query in features] == [
        "feat_customer_90d",
        "feat_stream_60m",
        "feat_customer_unified",
    ]
    assert all(not name.startswith("feat_") for name, _query in core)
    assert len({name for name, _query in core + features}) == len(core + features)


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

    def fake_export_executive_mart(*, evidence_root):
        assert Path(evidence_root) == tmp_path
        return {
            "duckdb_path": "data/gold/vina_bim_shop_executive.duckdb",
            "table_count": len(REQUIRED_GOLD_TABLES),
            "total_row_count": 123,
        }

    def fake_capture_evidence_fn(*, evidence_root):
        captured["evidence_root"] = Path(evidence_root)
        return {"artifacts": ["run_manifest.json"]}

    monkeypatch.setattr("vina_bim_shop.lakehouse.spark.runner.run_parity_checks", fake_run_parity_checks)
    monkeypatch.setattr("vina_bim_shop.lakehouse.spark.runner.run_gold_smoke_queries", fake_run_gold_smoke_queries)
    monkeypatch.setattr("vina_bim_shop.lakehouse.spark.runner.export_executive_mart", fake_export_executive_mart)

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
    assert summary["executive_mart"] == {
        "duckdb_path": "data/gold/vina_bim_shop_executive.duckdb",
        "table_count": len(REQUIRED_GOLD_TABLES),
        "total_row_count": 123,
    }
