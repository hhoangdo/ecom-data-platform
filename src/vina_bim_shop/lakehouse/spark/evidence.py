from __future__ import annotations

import json
from collections.abc import Callable
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import requests


DEFAULT_EVIDENCE_ROOT = Path("evidence/05_spark_batch")


def _get_json(url: str) -> Any:
    response = requests.get(url, timeout=30)
    response.raise_for_status()
    return response.json()


def capture_evidence(
    *,
    evidence_root: str | Path = DEFAULT_EVIDENCE_ROOT,
    master_url: str = "http://localhost:8085",
    history_url: str = "http://localhost:18080",
    get_json: Callable[[str], Any] = _get_json,
) -> dict[str, Any]:
    evidence_path = Path(evidence_root)
    evidence_path.mkdir(parents=True, exist_ok=True)

    master_status = get_json(f"{master_url.rstrip('/')}/json/")
    history_applications = get_json(f"{history_url.rstrip('/')}/api/v1/applications")

    (evidence_path / "spark_master_status.json").write_text(
        json.dumps(master_status, indent=2, sort_keys=True),
        encoding="utf-8",
    )
    (evidence_path / "spark_history_applications.json").write_text(
        json.dumps(history_applications, indent=2, sort_keys=True),
        encoding="utf-8",
    )

    version_matrix = {
        "spark_image": "vina-bim-shop/spark:4.0.0-iceberg-1.10.1",
        "spark": "4.0.0",
        "iceberg_runtime": "1.10.1",
        "hadoop_aws": "3.4.1",
        "aws_bundle": "2.24.6",
        "great_expectations": "1.17.2",
    }
    (evidence_path / "version_matrix.json").write_text(
        json.dumps(version_matrix, indent=2, sort_keys=True),
        encoding="utf-8",
    )

    artifacts = [
        "spark_master_status.json",
        "spark_history_applications.json",
        "spark_job_manifest.json",
        "spark_table_row_counts.json",
        "pyspark_validation_report.json",
        "dbt_parity_report.json",
        "dbt_parity_report.md",
        "trino_gold_smoke_results.json",
        "executive_mart_export_manifest.json",
        "executive_mart_export_report.md",
        "version_matrix.json",
        "gx/validation_results.json",
    ]
    if (evidence_path / "optimization" / "run_manifest.json").is_file():
        artifacts.append("optimization/run_manifest.json")

    manifest = {
        "captured_at": datetime.now(timezone.utc).isoformat(),
        "service_urls": {
            "spark_master_ui": master_url,
            "spark_history_server": history_url,
        },
        "artifacts": artifacts,
    }
    (evidence_path / "run_manifest.json").write_text(
        json.dumps(manifest, indent=2, sort_keys=True),
        encoding="utf-8",
    )
    return manifest
