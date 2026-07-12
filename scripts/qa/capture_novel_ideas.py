"""Capture machine-verifiable evidence for the two coursework novel ideas."""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import requests
import yaml
from PIL import Image


REPO_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_EVIDENCE_ROOT = Path("evidence/10_novel_ideas")
IDEA_1_NAME = "Novel Idea 1: DuckDB/dbt local analytics"
IDEA_2_NAME = "Novel Idea 2: Pinot realtime serving"
DUCKDB_QUERY = """
SELECT
  count(*) AS order_count,
  sum(CASE WHEN is_paid_order THEN 1 ELSE 0 END) AS paid_order_count,
  sum(official_paid_revenue) AS official_paid_revenue,
  sum(gross_merchandise_value) AS gross_merchandise_value
FROM gold.fact_order
""".strip()
PINOT_QUERY = """
SELECT alert_type, severity, count(*) AS alert_count
FROM pinot_realtime_ops_alerts
GROUP BY alert_type, severity
ORDER BY alert_count DESC, alert_type
LIMIT 20
""".strip()
PINOT_TABLE = "pinot_realtime_ops_alerts"
PINOT_REALTIME_TABLE = f"{PINOT_TABLE}_REALTIME"
PINOT_TOPIC = "realtime_ops_alerts"
PINOT_POLL_TIMEOUT_SECONDS = 180
PINOT_POLL_INTERVAL_SECONDS = 2


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _read_json(path: Path) -> dict[str, Any]:
    if not path.is_file():
        raise FileNotFoundError(f"Required evidence artifact is missing: {path}")
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"Evidence artifact must contain an object: {path}")
    return payload


def _relative_path(path: Path, repo_root: Path) -> str:
    return path.resolve().relative_to(repo_root.resolve()).as_posix()


def _artifact(path: Path, repo_root: Path) -> dict[str, str]:
    if not path.is_file():
        raise FileNotFoundError(f"Required evidence artifact is missing: {path}")
    return {"path": _relative_path(path, repo_root), "sha256": _sha256(path)}


def _json_rows(rows: list[tuple[Any, ...]]) -> list[list[Any]]:
    return json.loads(json.dumps(rows, default=str))


def _query_duckdb(database_path: Path) -> dict[str, Any]:
    import duckdb

    if not database_path.is_file():
        raise FileNotFoundError(f"Canonical DuckDB database is missing: {database_path}")
    connection = duckdb.connect(str(database_path), read_only=True)
    try:
        cursor = connection.execute(DUCKDB_QUERY)
        rows = cursor.fetchall()
        columns = [
            {"name": str(column[0]), "type": str(column[1])}
            for column in cursor.description
        ]
    finally:
        connection.close()
    if not rows:
        raise ValueError("DuckDB representative query returned no rows.")
    return {
        "engine": "duckdb",
        "sql": DUCKDB_QUERY,
        "columns": columns,
        "rows": _json_rows(rows),
        "row_count": len(rows),
        "success": True,
    }


def _dbt_gate(repo_root: Path, database_path: Path) -> tuple[dict[str, Any], list[dict[str, str]]]:
    run_results_path = repo_root / "infra/analytics/dbt/target/run_results.json"
    manifest_path = repo_root / "infra/analytics/dbt/target/manifest.json"
    parity_path = repo_root / "evidence/05_spark_batch/dbt_parity_report.json"
    parity_manifest_path = repo_root / "evidence/05_spark_batch/run_manifest.json"
    index_path = repo_root / "evidence/10_duckdb_dbt_local_analytics/index_optimization/index_benchmark.json"
    index_manifest_path = repo_root / "evidence/10_duckdb_dbt_local_analytics/index_optimization/run_manifest.json"
    fact_order_model_path = repo_root / "infra/analytics/dbt/models/gold/fact_order.sql"

    run_results = _read_json(run_results_path)
    dbt_manifest = _read_json(manifest_path)
    parity = _read_json(parity_path)
    parity_manifest = _read_json(parity_manifest_path)
    index = _read_json(index_path)
    index_manifest = _read_json(index_manifest_path)

    results = run_results.get("results", [])
    model_success_count = sum(
        1
        for result in results
        if str(result.get("unique_id", "")).startswith("model.") and result.get("status") == "success"
    )
    test_pass_count = sum(
        1
        for result in results
        if str(result.get("unique_id", "")).startswith("test.") and result.get("status") in {"pass", "success"}
    )
    gold_model_count = sum(
        1
        for node in dbt_manifest.get("nodes", {}).values()
        if isinstance(node, dict)
        and node.get("resource_type") == "model"
        and (
            node.get("config", {}).get("schema") == "gold"
            or "models/gold/" in str(node.get("original_file_path", "")).replace("\\", "/")
        )
    )
    parity_checks = parity.get("comparisons", [])
    parity_success = bool(parity_checks) and all(check.get("success") is True for check in parity_checks)
    if "dbt_parity_report.json" not in parity_manifest.get("artifacts", []):
        parity_success = False

    source_hash = _sha256(database_path)
    index_success = (
        index.get("source_sha256_before") == source_hash
        and index.get("source_sha256_after") == source_hash
        and index_manifest.get("source_sha256") == source_hash
        and index.get("index_name") == "idx_benchmark_fact_order_order_id"
        and index_manifest.get("index_name") == "idx_benchmark_fact_order_order_id"
        and index.get("result_hashes", {}).get("baseline") == index.get("result_hashes", {}).get("indexed")
    )
    gate = {
        "dbt_build": {
            "generated_at": run_results.get("metadata", {}).get("generated_at"),
            "dbt_version": run_results.get("metadata", {}).get("dbt_version"),
            "model_success_count": model_success_count,
            "test_pass_count": test_pass_count,
            "gold_model_count": gold_model_count,
            "passed": model_success_count > 0 and test_pass_count > 0 and gold_model_count > 0,
        },
        "parity": {"comparison_count": len(parity_checks), "passed": parity_success},
        "index_evidence": {
            "source_sha256": source_hash,
            "index_name": index.get("index_name"),
            "passed": index_success,
        },
    }
    if not all(item["passed"] for item in gate.values()):
        raise ValueError(f"DuckDB/dbt upstream gate failed: {gate}")
    artifacts = [
        _artifact(path, repo_root)
        for path in [
            run_results_path,
            manifest_path,
            parity_path,
            parity_manifest_path,
            index_path,
            index_manifest_path,
            fact_order_model_path,
        ]
    ]
    return gate, artifacts


def _health_is_good(payload: dict[str, Any]) -> bool:
    return any(
        str(payload.get(key, "")).strip().upper() in {"GOOD", "HEALTHY", "OK"}
        for key in ("status", "text", "body")
    )


def _first_table_status(payload: dict[str, Any]) -> dict[str, Any]:
    value = payload.get(PINOT_REALTIME_TABLE, payload.get(PINOT_TABLE))
    if isinstance(value, list):
        return value[0] if value and isinstance(value[0], dict) else {}
    return value if isinstance(value, dict) else {}


def _has_consuming_or_completed_segment(payload: dict[str, Any]) -> bool:
    segment_map = payload.get(PINOT_REALTIME_TABLE, payload.get(PINOT_TABLE, {}))
    if not isinstance(segment_map, dict):
        return False
    consuming = segment_map.get("_segmentToConsumingInfoMap", {})
    if not isinstance(consuming, dict):
        return False
    for states in consuming.values():
        for state in states if isinstance(states, list) else []:
            if str(state.get("consumerState", "")).upper() in {"CONSUMING", "ONLINE", "COMPLETED"}:
                return True
    return False


def _query_pinot(broker_url: str) -> dict[str, Any]:
    response = requests.post(
        f"{broker_url.rstrip('/')}/query/sql",
        json={"sql": PINOT_QUERY},
        timeout=30,
    )
    response.raise_for_status()
    payload = response.json()
    rows = payload.get("resultTable", {}).get("rows", []) or []
    exceptions = payload.get("exceptions", []) or []
    success = not exceptions and payload.get("partialResult") is not True and bool(rows)
    schema = payload.get("resultTable", {}).get("dataSchema", {})
    columns = [
        {"name": name, "type": data_type}
        for name, data_type in zip(schema.get("columnNames", []), schema.get("columnDataTypes", []), strict=True)
    ]
    return {
        "engine": "pinot",
        "sql": PINOT_QUERY,
        "columns": columns,
        "rows": rows,
        "row_count": len(rows),
        "success": success,
        "exceptions": exceptions,
        "partial_result": payload.get("partialResult", False),
    }


def _wait_for_pinot_query(broker_url: str) -> dict[str, Any]:
    deadline = time.monotonic() + PINOT_POLL_TIMEOUT_SECONDS
    last_result: dict[str, Any] = {}
    while True:
        last_result = _query_pinot(broker_url)
        if last_result["success"]:
            return last_result
        if time.monotonic() >= deadline:
            raise ValueError(f"Pinot representative query did not return rows: {last_result}")
        time.sleep(PINOT_POLL_INTERVAL_SECONDS)


def _pinot_gate(repo_root: Path, evidence_root: Path, broker_url: str) -> tuple[dict[str, Any], dict[str, Any], list[dict[str, str]]]:
    pinot_root = repo_root / "evidence/07_pinot_serving"
    controller_health_path = pinot_root / "controller_health.json"
    broker_health_path = pinot_root / "broker_health.json"
    table_status_path = pinot_root / "table_status.json"
    consuming_segments_path = pinot_root / "consuming_segments.json"
    run_manifest_path = pinot_root / "run_manifest.json"
    refresh_manifest_path = pinot_root / "refresh_evidence_manifest.json"
    version_matrix_path = pinot_root / "version_matrix.json"
    reconciliation_report_path = pinot_root / "query_outputs/reconciliation_report.md"
    table_config_path = repo_root / "infra/pinot/tables/pinot_realtime_ops_alerts_realtime.json"
    flink_config_path = repo_root / "configs/pipelines/flink_streaming.yaml"
    smoke_summary_path = evidence_root / "flink_smoke_publish_summary.json"

    controller_health = _read_json(controller_health_path)
    broker_health = _read_json(broker_health_path)
    table_status = _first_table_status(_read_json(table_status_path))
    consuming_segments = _read_json(consuming_segments_path)
    table_config = _read_json(table_config_path)
    streaming_config = yaml.safe_load(flink_config_path.read_text(encoding="utf-8"))
    smoke_summary = _read_json(smoke_summary_path)
    query = _wait_for_pinot_query(broker_url)

    stream_maps = table_config.get("ingestionConfig", {}).get("streamIngestionConfig", {}).get("streamConfigMaps", [])
    configured_topic = stream_maps[0].get("stream.kafka.topic.name") if stream_maps else None
    derived_topic = streaming_config.get("derived_topics", {}).get("ops_alerts") if isinstance(streaming_config, dict) else None
    smoke_topic = smoke_summary.get("expected_outputs", {}).get("ops_alerts_topic")
    ingestion_state = str(table_status.get("ingestionStatus", {}).get("ingestionState", "")).upper()
    segment_count = int(table_status.get("numSegments", 0) or 0)

    gate = {
        "health": {
            "controller": _health_is_good(controller_health),
            "broker": _health_is_good(broker_health),
            "passed": _health_is_good(controller_health) and _health_is_good(broker_health),
        },
        "realtime_table": {
            "table": PINOT_REALTIME_TABLE,
            "ingestion_state": ingestion_state,
            "segment_count": segment_count,
            "passed": ingestion_state in {"HEALTHY", "GOOD", "ONLINE"} and segment_count > 0,
        },
        "segments": {"passed": _has_consuming_or_completed_segment(consuming_segments)},
        "query": {"row_count": query["row_count"], "passed": query["success"]},
        "provenance": {
            "configured_topic": configured_topic,
            "derived_topic": derived_topic,
            "smoke_topic": smoke_topic,
            "passed": configured_topic == PINOT_TOPIC and derived_topic == PINOT_TOPIC and smoke_topic == PINOT_TOPIC,
        },
    }
    if not all(item["passed"] for item in gate.values()):
        raise ValueError(f"Pinot realtime upstream gate failed: {gate}")

    artifacts = [
        _artifact(path, repo_root)
        for path in [
            controller_health_path,
            broker_health_path,
            table_status_path,
            consuming_segments_path,
            run_manifest_path,
            refresh_manifest_path,
            version_matrix_path,
            reconciliation_report_path,
            table_config_path,
            flink_config_path,
            smoke_summary_path,
        ]
    ]
    provenance = {
        "source_topics": ["ops_events", "catalog_events", "fulfillment_events"],
        "flink_job": "vina-bim-shop-ops-alerts",
        "derived_kafka_topic": PINOT_TOPIC,
        "pinot_realtime_table": PINOT_TABLE,
        "truth_policy": "provisional_pinot_alert_stream",
        "canonical_truth_reference": _relative_path(reconciliation_report_path, repo_root),
    }
    return gate, {**query, "source_provenance": provenance}, artifacts


def _screenshot_metadata(evidence_root: Path, *, allow_missing: bool) -> list[dict[str, Any]]:
    screenshots = [
        evidence_root / "screenshots/idea_1_duckdb_dbt_lineage.png",
        evidence_root / "screenshots/idea_2_pinot_realtime_query.png",
    ]
    metadata: list[dict[str, Any]] = []
    missing: list[Path] = []
    for path in screenshots:
        item: dict[str, Any] = {"path": path.relative_to(evidence_root).as_posix(), "present": path.is_file() and path.stat().st_size > 0}
        if item["present"]:
            try:
                with Image.open(path) as image:
                    item["width"], item["height"] = image.size
                if not item["width"] or not item["height"]:
                    item["present"] = False
                else:
                    item["sha256"] = _sha256(path)
            except OSError:
                item["present"] = False
        if not item["present"]:
            missing.append(path)
        metadata.append(item)
    if missing and not allow_missing:
        raise ValueError(f"Required screenshots are missing or invalid: {', '.join(str(path) for path in missing)}")
    return metadata


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def capture_novel_ideas(
    repo_root: Path,
    evidence_root: Path,
    *,
    allow_missing_screenshots: bool = False,
    broker_url: str = "http://localhost:8000",
) -> dict[str, Any]:
    """Write the two ordered evidence summaries and their strict run manifest."""

    repo_root = repo_root.resolve()
    evidence_root = evidence_root.resolve()
    captured_at = _utc_now()
    manifest_path = evidence_root / "run_manifest.json"
    try:
        database_path = repo_root / "data/gold/vina_bim_shop.duckdb"
        duckdb_gate, duckdb_artifacts = _dbt_gate(repo_root, database_path)
        duckdb_query = _query_duckdb(database_path)
        duckdb_gate["representative_query"] = {"row_count": duckdb_query["row_count"], "passed": duckdb_query["success"]}

        pinot_gate, pinot_query, pinot_artifacts = _pinot_gate(repo_root, evidence_root, broker_url)
        screenshots = _screenshot_metadata(evidence_root, allow_missing=allow_missing_screenshots)
        screenshots_ready = all(screenshot["present"] for screenshot in screenshots)

        idea_1 = {
            "schema_version": 1,
            "idea_number": 1,
            "idea_name": IDEA_1_NAME,
            "captured_at": captured_at,
            "success": all(item["passed"] for item in duckdb_gate.values()),
            "success_gates": duckdb_gate,
            "representative_query": duckdb_query,
            "source_provenance": {
                "dbt_project": "infra/analytics/dbt",
                "dbt_model": "infra/analytics/dbt/models/gold/fact_order.sql",
                "canonical_duckdb": "data/gold/vina_bim_shop.duckdb",
                "truth_policy": "local_dbt_parity_oracle",
            },
            "upstream_artifacts": duckdb_artifacts,
            "limitations": [
                "DuckDB is a local reproducibility and parity path, not the canonical distributed truth.",
                "The isolated ART-index timing is an observed local result, not a production performance guarantee.",
            ],
        }
        idea_2 = {
            "schema_version": 1,
            "idea_number": 2,
            "idea_name": IDEA_2_NAME,
            "captured_at": captured_at,
            "success": all(item["passed"] for item in pinot_gate.values()),
            "success_gates": pinot_gate,
            "representative_query": {key: value for key, value in pinot_query.items() if key != "source_provenance"},
            "source_provenance": pinot_query["source_provenance"],
            "upstream_artifacts": pinot_artifacts,
            "limitations": [
                "Pinot is a fresh, provisional serving layer; Spark Gold through Trino remains canonical.",
                "The correction-aware dashboard contract requires Pinot multi-stage execution and is linked as a limitation, not this gate.",
            ],
        }
        _write_json(evidence_root / "idea_1_duckdb_dbt.json", idea_1)
        _write_json(evidence_root / "idea_2_pinot_realtime.json", idea_2)
        status = "success" if screenshots_ready else "pending_screenshots"
        manifest = {
            "schema_version": 1,
            "captured_at": captured_at,
            "status": status,
            "reproduction_commands": [
                "uv run dbt build --project-dir infra/analytics/dbt --profiles-dir infra/analytics/dbt",
                "uv run python scripts/pinot/refresh_evidence.py",
                "uv run python scripts/qa/capture_novel_ideas.py",
            ],
            "versions": {
                "dbt": duckdb_gate["dbt_build"]["dbt_version"],
                "duckdb": __import__("duckdb").__version__,
                "pinot": _read_json(repo_root / "evidence/07_pinot_serving/version_matrix.json").get("pinot_version"),
            },
            "success_gates": {"idea_1": idea_1["success"], "idea_2": idea_2["success"]},
            "screenshots": screenshots,
            "upstream_artifacts": duckdb_artifacts + pinot_artifacts,
            "artifacts": [
                "idea_1_duckdb_dbt.json",
                "idea_2_pinot_realtime.json",
                "run_manifest.json",
                "screenshots/idea_1_duckdb_dbt_lineage.png",
                "screenshots/idea_2_pinot_realtime_query.png",
            ],
        }
        _write_json(manifest_path, manifest)
        return manifest
    except Exception as exc:
        manifest = {
            "schema_version": 1,
            "captured_at": captured_at,
            "status": "failed",
            "error": str(exc),
        }
        _write_json(manifest_path, manifest)
        raise


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Capture evidence for the two coursework novel ideas.")
    parser.add_argument("--repo-root", type=Path, default=REPO_ROOT)
    parser.add_argument("--evidence-root", type=Path, default=DEFAULT_EVIDENCE_ROOT)
    parser.add_argument("--broker-url", default="http://localhost:8000")
    parser.add_argument("--allow-missing-screenshots", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    try:
        manifest = capture_novel_ideas(
            args.repo_root,
            args.evidence_root,
            allow_missing_screenshots=args.allow_missing_screenshots,
            broker_url=args.broker_url,
        )
    except Exception as exc:
        print(f"Novel ideas evidence failed: {exc}", file=sys.stderr)
        return 1
    print(f"Novel ideas evidence status: {manifest['status']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
