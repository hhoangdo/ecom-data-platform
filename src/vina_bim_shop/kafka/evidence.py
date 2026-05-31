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
    evidence_root: str | Path = "evidence/03_kafka_ingestion",
    schema_registry_url: str = "http://localhost:8081",
    kafka_connect_url: str = "http://localhost:8083",
    kafka_ui_url: str = "http://localhost:8084",
    get_json: GetJson = _get_json,
    run_command: RunCommand = _run_command,
) -> dict[str, Any]:
    evidence_path = Path(evidence_root)
    evidence_path.mkdir(parents=True, exist_ok=True)

    topic_list = run_command(["docker", "compose", "exec", "-T", "kafka", "kafka-topics", "--bootstrap-server", "kafka:29092", "--list"])
    topic_descriptions = run_command(["docker", "compose", "exec", "-T", "kafka", "kafka-topics", "--bootstrap-server", "kafka:29092", "--describe"])
    subjects = get_json(f"{schema_registry_url.rstrip('/')}/subjects")
    connectors = get_json(f"{kafka_connect_url.rstrip('/')}/connectors")

    (evidence_path / "topic_list.txt").write_text(topic_list, encoding="utf-8")
    (evidence_path / "topic_descriptions.txt").write_text(topic_descriptions, encoding="utf-8")
    (evidence_path / "schema_registry_subjects.json").write_text(json.dumps(subjects, indent=2, sort_keys=True), encoding="utf-8")
    (evidence_path / "kafka_connect_status.json").write_text(json.dumps({"connectors": connectors}, indent=2, sort_keys=True), encoding="utf-8")

    version_matrix = {
        "kafka": "confluentinc/cp-kafka:7.8.3",
        "schema_registry": "confluentinc/cp-schema-registry:7.8.3",
        "kafka_connect": "confluentinc/cp-kafka-connect:7.8.3",
        "kafka_ui": "provectuslabs/kafka-ui:v0.7.2",
    }
    (evidence_path / "version_matrix.json").write_text(json.dumps(version_matrix, indent=2, sort_keys=True), encoding="utf-8")

    screenshots_path = evidence_path / "screenshots"
    screenshots_path.mkdir(exist_ok=True)
    (screenshots_path / "README.md").write_text(
        "Capture required screenshots here: kafka_ui_topics.png, kafka_ui_message_sample.png, schema_registry_subjects.png.\n",
        encoding="utf-8",
    )

    manifest = {
        "captured_at": datetime.now(timezone.utc).isoformat(),
        "service_urls": {
            "schema_registry": schema_registry_url,
            "kafka_connect": kafka_connect_url,
            "kafka_ui": kafka_ui_url,
        },
        "artifacts": [
            "topic_list.txt",
            "topic_descriptions.txt",
            "schema_registry_subjects.json",
            "kafka_connect_status.json",
            "version_matrix.json",
            "screenshots/README.md",
            "screenshots/kafka_ui_topics.png",
            "screenshots/kafka_ui_message_sample.png",
            "screenshots/schema_registry_subjects.png",
        ],
    }
    (evidence_path / "run_manifest.json").write_text(json.dumps(manifest, indent=2, sort_keys=True), encoding="utf-8")
    return manifest
