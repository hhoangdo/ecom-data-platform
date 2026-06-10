from __future__ import annotations

import json
from collections.abc import Callable
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import requests


DEFAULT_EVIDENCE_ROOT = Path("evidence/07_pinot_serving")


def _get_json(url: str) -> Any:
    response = requests.get(url, timeout=30)
    response.raise_for_status()
    if not response.content:
        return {}
    if "application/json" in response.headers.get("content-type", ""):
        return response.json()
    return {"text": response.text}


def _post_json(url: str, payload: dict[str, Any]) -> Any:
    response = requests.post(url, json=payload, timeout=30)
    response.raise_for_status()
    return response.json()


def capture_evidence(
    *,
    evidence_root: str | Path = DEFAULT_EVIDENCE_ROOT,
    controller_url: str = "http://localhost:9003",
    broker_url: str = "http://localhost:8000",
    get_json: Callable[[str], Any] = _get_json,
    post_json: Callable[[str, dict[str, Any]], Any] = _post_json,
) -> dict[str, Any]:
    evidence_path = Path(evidence_root)
    evidence_path.mkdir(parents=True, exist_ok=True)

    controller_health = get_json(f"{controller_url.rstrip('/')}/health")
    broker_health = get_json(f"{broker_url.rstrip('/')}/health")
    table_inventory = get_json(f"{controller_url.rstrip('/')}/tables")

    table_names = [
        "pinot_realtime_commerce_metrics_1m_REALTIME",
        "pinot_realtime_metric_corrections_REALTIME",
        "pinot_realtime_ops_alerts_REALTIME",
    ]
    table_status = {
        table_name: get_json(f"{controller_url.rstrip('/')}/debug/tables/{table_name.removesuffix('_REALTIME')}")
        for table_name in table_names
    }
    consuming_segments = {
        table_name: get_json(f"{controller_url.rstrip('/')}/tables/{table_name.removesuffix('_REALTIME')}/consumingSegmentsInfo")
        for table_name in table_names
    }
    row_counts = {
        "pinot_realtime_commerce_metrics_1m": post_json(
            f"{broker_url.rstrip('/')}/query/sql",
            {"sql": "select count(*) as row_count from pinot_realtime_commerce_metrics_1m"},
        ),
        "pinot_realtime_metric_corrections": post_json(
            f"{broker_url.rstrip('/')}/query/sql",
            {"sql": "select count(*) as row_count from pinot_realtime_metric_corrections"},
        ),
        "pinot_realtime_ops_alerts": post_json(
            f"{broker_url.rstrip('/')}/query/sql",
            {"sql": "select count(*) as row_count from pinot_realtime_ops_alerts"},
        ),
    }

    (evidence_path / "controller_health.json").write_text(json.dumps(controller_health, indent=2, sort_keys=True), encoding="utf-8")
    (evidence_path / "broker_health.json").write_text(json.dumps(broker_health, indent=2, sort_keys=True), encoding="utf-8")
    (evidence_path / "table_inventory.json").write_text(json.dumps(table_inventory, indent=2, sort_keys=True), encoding="utf-8")
    (evidence_path / "table_status.json").write_text(json.dumps(table_status, indent=2, sort_keys=True), encoding="utf-8")
    (evidence_path / "consuming_segments.json").write_text(
        json.dumps(consuming_segments, indent=2, sort_keys=True),
        encoding="utf-8",
    )
    (evidence_path / "row_counts.json").write_text(json.dumps(row_counts, indent=2, sort_keys=True), encoding="utf-8")

    version_matrix = {
        "pinot_image": "apachepinot/pinot:1.4.0",
        "pinot_version": "1.4.0",
        "zookeeper_image": "zookeeper:3.9.3",
    }
    (evidence_path / "version_matrix.json").write_text(json.dumps(version_matrix, indent=2, sort_keys=True), encoding="utf-8")

    query_output_artifacts = []
    query_output_dir = evidence_path / "query_outputs"
    if query_output_dir.exists():
        for path in sorted(query_output_dir.rglob("*")):
            if path.is_file():
                query_output_artifacts.append(str(path.relative_to(evidence_path)).replace("\\", "/"))

    manifest = {
        "captured_at": datetime.now(timezone.utc).isoformat(),
        "service_urls": {
            "pinot_controller": controller_url,
            "pinot_broker": broker_url,
        },
        "artifacts": [
            "controller_health.json",
            "broker_health.json",
            "table_inventory.json",
            "table_status.json",
            "consuming_segments.json",
            "row_counts.json",
            "version_matrix.json",
            "run_manifest.json",
            *query_output_artifacts,
        ],
    }
    (evidence_path / "run_manifest.json").write_text(json.dumps(manifest, indent=2, sort_keys=True), encoding="utf-8")
    return manifest
