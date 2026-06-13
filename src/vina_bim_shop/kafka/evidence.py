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


def _write_json(path: Path, payload: Any) -> None:
    path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")


def capture_evidence(
    *,
    evidence_root: str | Path = "evidence/03_kafka_ingestion",
    schema_registry_url: str = "http://localhost:8081",
    kafka_connect_url: str = "http://localhost:8083",
    get_json: GetJson = _get_json,
    run_command: RunCommand = _run_command,
) -> dict[str, Any]:
    evidence_path = Path(evidence_root)
    evidence_path.mkdir(parents=True, exist_ok=True)
    artifacts: list[str] = []
    failures: list[dict[str, str]] = []

    def record_failure(step: str, exc: Exception) -> None:
        failures.append({"step": step, "error": str(exc)})

    try:
        topic_list = run_command(["docker", "compose", "exec", "-T", "kafka", "kafka-topics", "--bootstrap-server", "kafka:29092", "--list"])
        (evidence_path / "topic_list.txt").write_text(topic_list, encoding="utf-8")
        artifacts.append("topic_list.txt")
    except Exception as exc:
        record_failure("topic_list", exc)

    try:
        topic_descriptions = run_command(["docker", "compose", "exec", "-T", "kafka", "kafka-topics", "--bootstrap-server", "kafka:29092", "--describe"])
        (evidence_path / "topic_descriptions.txt").write_text(topic_descriptions, encoding="utf-8")
        artifacts.append("topic_descriptions.txt")
    except Exception as exc:
        record_failure("topic_descriptions", exc)

    try:
        subjects = get_json(f"{schema_registry_url.rstrip('/')}/subjects")
        _write_json(evidence_path / "schema_registry_subjects.json", subjects)
        artifacts.append("schema_registry_subjects.json")
    except Exception as exc:
        record_failure("schema_registry_subjects", exc)

    try:
        connectors = get_json(f"{kafka_connect_url.rstrip('/')}/connectors")
        _write_json(evidence_path / "kafka_connect_status.json", {"connectors": connectors})
        artifacts.append("kafka_connect_status.json")
    except Exception as exc:
        record_failure("kafka_connect", exc)

    version_matrix = {
        "kafka": "confluentinc/cp-kafka:7.8.3",
        "schema_registry": "confluentinc/cp-schema-registry:7.8.3",
        "kafka_connect": "confluentinc/cp-kafka-connect:7.8.3",
        "kafka_ui": "provectuslabs/kafka-ui:v0.7.2",
    }
    _write_json(evidence_path / "version_matrix.json", version_matrix)
    artifacts.append("version_matrix.json")

    manifest = {
        "captured_at": datetime.now(timezone.utc).isoformat(),
        "status": "failed" if failures else "success",
        "failures": failures,
        "service_urls": {
            "schema_registry": schema_registry_url,
            "kafka_connect": kafka_connect_url,
        },
        "artifacts": artifacts,
    }
    _write_json(evidence_path / "run_manifest.json", manifest)
    if failures:
        failure_summary = "; ".join(f"{failure['step']}: {failure['error']}" for failure in failures)
        raise RuntimeError(f"Kafka evidence capture failed: {failure_summary}")
    return manifest
