import hashlib
import importlib.util
import json
import sys
from pathlib import Path

import duckdb
import pytest
from PIL import Image


def test_novel_ideas_capture_script_exists() -> None:
    repo_root = Path(__file__).resolve().parents[2]

    assert (repo_root / "scripts" / "qa" / "capture_novel_ideas.py").is_file()


def _load_module():
    script_path = Path(__file__).resolve().parents[2] / "scripts" / "qa" / "capture_novel_ideas.py"
    spec = importlib.util.spec_from_file_location("capture_novel_ideas_script", script_path)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def _write_json(path: Path, payload: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _build_repo_fixture(repo_root: Path) -> None:
    database_path = repo_root / "data" / "gold" / "vina_bim_shop.duckdb"
    database_path.parent.mkdir(parents=True)
    connection = duckdb.connect(str(database_path))
    connection.execute("create schema gold")
    connection.execute(
        """
        create table gold.fact_order (
            order_id varchar,
            is_paid_order boolean,
            official_paid_revenue double,
            gross_merchandise_value double
        )
        """
    )
    connection.execute(
        "insert into gold.fact_order values ('ORD-1', true, 120.0, 150.0), ('ORD-2', false, 0.0, 50.0)"
    )
    connection.close()

    _write_json(
        repo_root / "infra" / "analytics" / "dbt" / "target" / "run_results.json",
        {
            "metadata": {"generated_at": "2026-07-12T00:00:00Z", "dbt_version": "1.11.11"},
            "results": [
                {"unique_id": "model.vina_bim_shop.fact_order", "status": "success"},
                {"unique_id": "test.vina_bim_shop.not_null_fact_order_order_id", "status": "pass"},
            ],
        },
    )
    _write_json(
        repo_root / "infra" / "analytics" / "dbt" / "target" / "manifest.json",
        {
            "nodes": {
                "model.vina_bim_shop.fact_order": {
                    "resource_type": "model",
                    "config": {"schema": "gold"},
                }
            }
        },
    )
    model_path = repo_root / "infra" / "analytics" / "dbt" / "models" / "gold" / "fact_order.sql"
    model_path.parent.mkdir(parents=True, exist_ok=True)
    model_path.write_text("select * from stg_orders\n", encoding="utf-8")
    _write_json(
        repo_root / "evidence" / "05_spark_batch" / "dbt_parity_report.json",
        {"comparisons": [{"name": "fact_order.row_count", "success": True}]},
    )
    _write_json(
        repo_root / "evidence" / "05_spark_batch" / "run_manifest.json",
        {"artifacts": ["dbt_parity_report.json"]},
    )
    source_hash = _sha256(database_path)
    _write_json(
        repo_root / "evidence" / "10_duckdb_dbt_local_analytics" / "index_optimization" / "index_benchmark.json",
        {
            "source_sha256_before": source_hash,
            "source_sha256_after": source_hash,
            "index_name": "idx_benchmark_fact_order_order_id",
            "result_hashes": {"baseline": "same", "indexed": "same"},
        },
    )
    _write_json(
        repo_root / "evidence" / "10_duckdb_dbt_local_analytics" / "index_optimization" / "run_manifest.json",
        {
            "source_sha256": source_hash,
            "index_name": "idx_benchmark_fact_order_order_id",
            "artifacts": ["index_benchmark.json"]
        },
    )

    (repo_root / "infra" / "pinot" / "tables").mkdir(parents=True)
    _write_json(
        repo_root / "infra" / "pinot" / "tables" / "pinot_realtime_ops_alerts_realtime.json",
        {
            "tableName": "pinot_realtime_ops_alerts",
            "ingestionConfig": {
                "streamIngestionConfig": {
                    "streamConfigMaps": [{"stream.kafka.topic.name": "realtime_ops_alerts"}]
                }
            },
        },
    )
    pipeline_path = repo_root / "configs" / "pipelines" / "flink_streaming.yaml"
    pipeline_path.parent.mkdir(parents=True)
    pipeline_path.write_text("derived_topics:\n  ops_alerts: realtime_ops_alerts\n", encoding="utf-8")
    _write_json(
        repo_root / "evidence" / "10_novel_ideas" / "flink_smoke_publish_summary.json",
        {"expected_outputs": {"ops_alerts_topic": "realtime_ops_alerts"}},
    )

    pinot_root = repo_root / "evidence" / "07_pinot_serving"
    _write_json(pinot_root / "controller_health.json", {"text": "OK"})
    _write_json(pinot_root / "broker_health.json", {"text": "OK"})
    _write_json(
        pinot_root / "table_status.json",
        {
            "pinot_realtime_ops_alerts_REALTIME": [
                {"ingestionStatus": {"ingestionState": "HEALTHY"}, "numSegments": 1}
            ]
        },
    )
    _write_json(
        pinot_root / "consuming_segments.json",
        {
            "pinot_realtime_ops_alerts_REALTIME": {
                "_segmentToConsumingInfoMap": {"segment-1": [{"consumerState": "CONSUMING"}]}
            }
        },
    )
    _write_json(pinot_root / "run_manifest.json", {"artifacts": ["table_status.json", "consuming_segments.json"]})
    _write_json(pinot_root / "refresh_evidence_manifest.json", {"run_manifest": "run_manifest.json"})
    _write_json(
        pinot_root / "version_matrix.json",
        {"pinot_version": "1.4.0"},
    )
    query_report = pinot_root / "query_outputs" / "reconciliation_report.md"
    query_report.parent.mkdir(parents=True, exist_ok=True)
    query_report.write_text("Pinot is provisional; Trino is canonical.\n", encoding="utf-8")


def _write_screenshots(evidence_root: Path) -> None:
    screenshots = evidence_root / "screenshots"
    screenshots.mkdir(parents=True, exist_ok=True)
    for filename in ["idea_1_duckdb_dbt_lineage.png", "idea_2_pinot_realtime_query.png"]:
        Image.new("RGB", (40, 30), color="white").save(screenshots / filename)


def _successful_pinot_response():
    class _Response:
        def raise_for_status(self) -> None:
            return None

        def json(self) -> dict:
            return {
                "exceptions": [],
                "partialResult": False,
                "resultTable": {
                    "dataSchema": {
                        "columnNames": ["alert_type", "severity", "alert_count"],
                        "columnDataTypes": ["STRING", "STRING", "LONG"],
                    },
                    "rows": [["traffic_burst_detected", "high", 1]],
                },
            }

    return _Response()


def test_capture_writes_exact_ordered_evidence_with_strict_screenshots(tmp_path: Path, monkeypatch) -> None:
    module = _load_module()
    repo_root = tmp_path / "repo"
    evidence_root = repo_root / "evidence" / "10_novel_ideas"
    _build_repo_fixture(repo_root)
    _write_screenshots(evidence_root)

    monkeypatch.setattr(module.requests, "post", lambda *_args, **_kwargs: _successful_pinot_response())

    manifest = module.capture_novel_ideas(repo_root, evidence_root)

    assert manifest["status"] == "success"
    idea_1 = json.loads((evidence_root / "idea_1_duckdb_dbt.json").read_text(encoding="utf-8"))
    idea_2 = json.loads((evidence_root / "idea_2_pinot_realtime.json").read_text(encoding="utf-8"))
    assert [idea_1["idea_name"], idea_2["idea_name"]] == [
        "Novel Idea 1: DuckDB/dbt local analytics",
        "Novel Idea 2: Pinot realtime serving",
    ]
    assert idea_1["representative_query"]["rows"] == [[2, 1, 120.0, 200.0]]
    assert any(
        artifact["path"] == "infra/analytics/dbt/models/gold/fact_order.sql"
        for artifact in idea_1["upstream_artifacts"]
    )
    assert idea_2["source_provenance"]["derived_kafka_topic"] == "realtime_ops_alerts"
    assert idea_2["representative_query"]["success"] is True
    assert all(screenshot["sha256"] for screenshot in manifest["screenshots"])


def test_capture_fails_closed_when_a_required_screenshot_is_missing(tmp_path: Path, monkeypatch) -> None:
    module = _load_module()
    repo_root = tmp_path / "repo"
    evidence_root = repo_root / "evidence" / "10_novel_ideas"
    _build_repo_fixture(repo_root)
    monkeypatch.setattr(module.requests, "post", lambda *_args, **_kwargs: _successful_pinot_response())

    with pytest.raises(ValueError, match="Required screenshots"):
        module.capture_novel_ideas(repo_root, evidence_root)

    manifest = json.loads((evidence_root / "run_manifest.json").read_text(encoding="utf-8"))
    assert manifest["status"] == "failed"


def test_capture_rejects_unsuccessful_dbt_results(tmp_path: Path, monkeypatch) -> None:
    module = _load_module()
    repo_root = tmp_path / "repo"
    evidence_root = repo_root / "evidence" / "10_novel_ideas"
    _build_repo_fixture(repo_root)
    _write_screenshots(evidence_root)
    run_results_path = repo_root / "infra" / "analytics" / "dbt" / "target" / "run_results.json"
    run_results = json.loads(run_results_path.read_text(encoding="utf-8"))
    run_results["results"][0]["status"] = "error"
    _write_json(run_results_path, run_results)
    monkeypatch.setattr(module.requests, "post", lambda *_args, **_kwargs: _successful_pinot_response())

    with pytest.raises(ValueError, match="DuckDB/dbt upstream gate failed"):
        module.capture_novel_ideas(repo_root, evidence_root)


def test_capture_accepts_dbt_success_status_for_test_nodes(tmp_path: Path, monkeypatch) -> None:
    module = _load_module()
    repo_root = tmp_path / "repo"
    evidence_root = repo_root / "evidence" / "10_novel_ideas"
    _build_repo_fixture(repo_root)
    _write_screenshots(evidence_root)
    run_results_path = repo_root / "infra" / "analytics" / "dbt" / "target" / "run_results.json"
    run_results = json.loads(run_results_path.read_text(encoding="utf-8"))
    run_results["results"][1]["status"] = "success"
    _write_json(run_results_path, run_results)
    monkeypatch.setattr(module.requests, "post", lambda *_args, **_kwargs: _successful_pinot_response())

    manifest = module.capture_novel_ideas(repo_root, evidence_root)

    assert manifest["success_gates"]["idea_1"] is True


def test_capture_uses_the_flink_publisher_summary_filename(tmp_path: Path, monkeypatch) -> None:
    module = _load_module()
    repo_root = tmp_path / "repo"
    evidence_root = repo_root / "evidence" / "10_novel_ideas"
    _build_repo_fixture(repo_root)
    _write_screenshots(evidence_root)
    monkeypatch.setattr(module.requests, "post", lambda *_args, **_kwargs: _successful_pinot_response())

    manifest = module.capture_novel_ideas(repo_root, evidence_root)

    assert manifest["success_gates"]["idea_2"] is True


def test_capture_rejects_unhealthy_pinot_or_missing_segment_provenance(tmp_path: Path, monkeypatch) -> None:
    module = _load_module()
    repo_root = tmp_path / "repo"
    evidence_root = repo_root / "evidence" / "10_novel_ideas"
    _build_repo_fixture(repo_root)
    _write_screenshots(evidence_root)
    _write_json(repo_root / "evidence" / "07_pinot_serving" / "broker_health.json", {"text": "DOWN"})
    _write_json(
        repo_root / "evidence" / "07_pinot_serving" / "consuming_segments.json",
        {"pinot_realtime_ops_alerts_REALTIME": {"_segmentToConsumingInfoMap": {}}},
    )
    monkeypatch.setattr(module.requests, "post", lambda *_args, **_kwargs: _successful_pinot_response())

    with pytest.raises(ValueError, match="Pinot realtime upstream gate failed"):
        module.capture_novel_ideas(repo_root, evidence_root)
