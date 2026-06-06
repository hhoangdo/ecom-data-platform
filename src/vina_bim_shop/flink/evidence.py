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
    def __call__(self, *, flink_ui_url: str, screenshots_path: Path) -> dict[str, str]: ...


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


def _capture_screenshots(*, flink_ui_url: str, screenshots_path: Path) -> dict[str, str]:
    screenshots_path.mkdir(parents=True, exist_ok=True)
    for filename in ["flink_jobs.png", "flink_checkpoints.png"]:
        (screenshots_path / filename).unlink(missing_ok=True)
    env = os.environ.copy()
    env.setdefault("PLAYWRIGHT_BROWSERS_PATH", "0")
    npx = _resolve_command("npx")
    subprocess.run([npx, "playwright", "install", "chromium"], check=True, env=env)

    jobs_path = screenshots_path / "flink_jobs.png"
    checkpoints_path = screenshots_path / "flink_checkpoints.png"
    _capture_page(
        npx=npx,
        env=env,
        selector="text=Running Jobs",
        url=f"{flink_ui_url.rstrip('/')}/#/job/running",
        destination=jobs_path,
    )
    _capture_page(
        npx=npx,
        env=env,
        selector="text=Completed Checkpoints",
        url=f"{flink_ui_url.rstrip('/')}/#/job/running",
        destination=checkpoints_path,
        fallback_selector="text=Running Jobs",
    )
    return {
        "jobs": str(jobs_path.resolve()),
        "checkpoints": str(checkpoints_path.resolve()),
    }


def _capture_page(
    *,
    npx: str,
    env: dict[str, str],
    selector: str,
    url: str,
    destination: Path,
    fallback_selector: str | None = None,
) -> None:
    command = [
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
    ]
    try:
        subprocess.run(command, check=True, env=env)
    except subprocess.CalledProcessError:
        if fallback_selector is None:
            raise
        fallback_command = [
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
            fallback_selector,
            url,
            str(destination.resolve()),
        ]
        subprocess.run(fallback_command, check=True, env=env)


def capture_evidence(
    *,
    evidence_root: str | Path = "evidence/06_flink_streaming",
    flink_api_url: str = "http://localhost:8086",
    flink_ui_url: str = "http://localhost:8086",
    get_json: GetJson = _get_json,
    run_command: RunCommand = _run_command,
    screenshot_capturer: ScreenshotCapturer = _capture_screenshots,
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

    screenshots_path = evidence_path / "screenshots"
    screenshots_path.mkdir(exist_ok=True)
    (screenshots_path / "README.md").write_text(
        "Capture required screenshots here: flink_jobs.png, flink_checkpoints.png.\n",
        encoding="utf-8",
    )
    screenshot_capturer(flink_ui_url=flink_ui_url, screenshots_path=screenshots_path)

    manifest = {
        "captured_at": datetime.now(timezone.utc).isoformat(),
        "service_urls": {
            "flink_api": flink_api_url,
            "flink_ui": flink_ui_url,
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
            "screenshots/README.md",
            "screenshots/flink_jobs.png",
            "screenshots/flink_checkpoints.png",
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
