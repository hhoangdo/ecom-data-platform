from __future__ import annotations

import json
import subprocess
from collections.abc import Callable
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import requests


GetJson = Callable[[str], Any]
RunCommand = Callable[[list[str]], str]


def _get_json(url: str) -> Any:
    response = requests.get(url, timeout=30)
    response.raise_for_status()
    return response.json()


def _run_command(command: list[str]) -> str:
    completed = subprocess.run(command, check=True, text=True, capture_output=True)
    return completed.stdout


def capture_evidence(
    *,
    evidence_root: str | Path = "evidence/06_flink_streaming",
    flink_api_url: str = "http://localhost:8086",
    get_json: GetJson = _get_json,
    run_command: RunCommand = _run_command,
) -> dict[str, Any]:
    evidence_path = Path(evidence_root)
    evidence_path.mkdir(parents=True, exist_ok=True)

    overview = get_json(f"{flink_api_url.rstrip('/')}/overview")
    jobs = get_json(f"{flink_api_url.rstrip('/')}/jobs")
    taskmanagers = get_json(f"{flink_api_url.rstrip('/')}/taskmanagers")

    topic_samples = {
        "realtime_commerce_metrics_1m": _consume_topic_sample("realtime_commerce_metrics_1m", run_command),
        "realtime_ops_alerts": _consume_topic_sample("realtime_ops_alerts", run_command),
        "realtime_metric_corrections": _consume_topic_sample("realtime_metric_corrections", run_command),
    }
    checkpoint_listing = run_command(
        [
            "docker",
            "compose",
            "run",
            "--rm",
            "--no-deps",
            "--entrypoint",
            "/bin/sh",
            "minio-init",
            "-c",
            'mc alias set ALIAS http://minio:9000 "${MINIO_ROOT_USER}" "${MINIO_ROOT_PASSWORD}" >/dev/null && mc ls --recursive ALIAS/checkpoints/flink',
        ]
    )
    curated_listing = run_command(
        [
            "docker",
            "compose",
            "run",
            "--rm",
            "--no-deps",
            "--entrypoint",
            "/bin/sh",
            "minio-init",
            "-c",
            'mc alias set ALIAS http://minio:9000 "${MINIO_ROOT_USER}" "${MINIO_ROOT_PASSWORD}" >/dev/null && mc ls --recursive ALIAS/evidence/streaming_curated',
        ]
    )

    (evidence_path / "flink_overview.json").write_text(json.dumps(overview, indent=2, sort_keys=True), encoding="utf-8")
    (evidence_path / "flink_jobs.json").write_text(json.dumps(jobs, indent=2, sort_keys=True), encoding="utf-8")
    (evidence_path / "flink_taskmanagers.json").write_text(json.dumps(taskmanagers, indent=2, sort_keys=True), encoding="utf-8")
    (evidence_path / "derived_topic_samples.json").write_text(json.dumps(topic_samples, indent=2, sort_keys=True), encoding="utf-8")
    (evidence_path / "checkpoint_listing.txt").write_text(checkpoint_listing, encoding="utf-8")
    (evidence_path / "curated_output_listing.txt").write_text(curated_listing, encoding="utf-8")

    version_matrix = {
        "flink": "flink:1.19.2-scala_2.12-java17",
        "python": "3.11",
        "pyflink": "1.19.2",
        "kafka_connector": "flink-sql-connector-kafka-3.2.0-1.19",
    }
    (evidence_path / "version_matrix.json").write_text(json.dumps(version_matrix, indent=2, sort_keys=True), encoding="utf-8")

    manifest = {
        "captured_at": datetime.now(timezone.utc).isoformat(),
        "service_urls": {
            "flink_api": flink_api_url,
        },
        "artifacts": [
            "flink_overview.json",
            "flink_jobs.json",
            "flink_taskmanagers.json",
            "derived_topic_samples.json",
            "checkpoint_listing.txt",
            "curated_output_listing.txt",
            "version_matrix.json",
            "run_manifest.json",
        ],
    }
    (evidence_path / "run_manifest.json").write_text(json.dumps(manifest, indent=2, sort_keys=True), encoding="utf-8")
    return manifest


def _consume_topic_sample(topic: str, run_command: RunCommand) -> dict[str, Any]:
    raw_output = run_command(
        [
            "docker",
            "compose",
            "exec",
            "-T",
            "kafka",
            "kafka-console-consumer",
            "--bootstrap-server",
            "kafka:29092",
            "--topic",
            topic,
            "--from-beginning",
            "--max-messages",
            "1",
            "--timeout-ms",
            "10000",
        ]
    ).strip()
    if not raw_output:
        return {"sample": None}
    return json.loads(raw_output.splitlines()[0])
