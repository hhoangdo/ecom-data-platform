from __future__ import annotations

import json
import os
import shutil
import subprocess
from collections.abc import Callable
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Protocol

import requests


GetJson = Callable[[str], Any]
RunCommand = Callable[[list[str]], str]


class ScreenshotCapturer(Protocol):
    def __call__(self, *, kafka_ui_url: str, screenshots_path: Path) -> dict[str, str]: ...


def _get_json(url: str) -> Any:
    response = requests.get(url, timeout=30)
    response.raise_for_status()
    return response.json()


def _run_command(command: list[str]) -> str:
    completed = subprocess.run(command, check=True, text=True, capture_output=True)
    return completed.stdout


def _resolve_command(command: str) -> str:
    candidates = [command]
    if os.name == "nt":
        candidates = [f"{command}.cmd", f"{command}.exe", command]
    for candidate in candidates:
        resolved = shutil.which(candidate)
        if resolved:
            return resolved
    raise FileNotFoundError(f"Required command not found on PATH: {command}")


def _screenshot_jobs(kafka_ui_url: str, screenshots_path: Path) -> list[tuple[str, Path, str]]:
    base_url = kafka_ui_url.rstrip("/")
    return [
        (
            f"{base_url}/ui/clusters/vina-bim-shop-local/all-topics?perPage=25",
            screenshots_path / "kafka_ui_topics.png",
            "text=commerce_events",
        ),
        (
            f"{base_url}/ui/clusters/vina-bim-shop-local/all-topics/commerce_events/messages",
            screenshots_path / "kafka_ui_message_sample.png",
            "text=search_performed",
        ),
        (
            f"{base_url}/ui/clusters/vina-bim-shop-local/schemas",
            screenshots_path / "schema_registry_subjects.png",
            "text=commerce_events-value",
        ),
    ]


def _capture_screenshots(kafka_ui_url: str, screenshots_path: Path) -> dict[str, str]:
    screenshots_path.mkdir(parents=True, exist_ok=True)
    for filename in ["kafka_ui_topics.png", "kafka_ui_message_sample.png", "schema_registry_subjects.png"]:
        (screenshots_path / filename).unlink(missing_ok=True)
    env = os.environ.copy()
    env.setdefault("PLAYWRIGHT_BROWSERS_PATH", "0")
    npx = _resolve_command("npx")
    subprocess.run([npx, "playwright", "install", "chromium"], check=True, env=env)

    for url, destination, selector in _screenshot_jobs(kafka_ui_url, screenshots_path):
        subprocess.run(
            [
                npx,
                "playwright",
                "screenshot",
                "--browser",
                "chromium",
                "--full-page",
                "--viewport-size",
                "1440,1400",
                "--timeout",
                "30000",
                "--wait-for-selector",
                selector,
                url,
                str(destination.resolve()),
            ],
            check=True,
            env=env,
        )
    return {
        "topics": str((screenshots_path / "kafka_ui_topics.png").resolve()),
        "message_sample": str((screenshots_path / "kafka_ui_message_sample.png").resolve()),
        "schema_registry": str((screenshots_path / "schema_registry_subjects.png").resolve()),
    }


def capture_evidence(
    *,
    evidence_root: str | Path = "evidence/03_kafka_ingestion",
    schema_registry_url: str = "http://localhost:8081",
    kafka_connect_url: str = "http://localhost:8083",
    kafka_ui_url: str = "http://localhost:8084",
    get_json: GetJson = _get_json,
    run_command: RunCommand = _run_command,
    screenshot_capturer: ScreenshotCapturer = _capture_screenshots,
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
    screenshot_capturer(kafka_ui_url=kafka_ui_url, screenshots_path=screenshots_path)

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
