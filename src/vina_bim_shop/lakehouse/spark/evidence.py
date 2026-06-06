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


DEFAULT_EVIDENCE_ROOT = Path("evidence/05_spark_batch")


class ScreenshotCapturer(Protocol):
    def __call__(self, *, master_url: str, history_url: str, screenshots_path: Path) -> dict[str, str]: ...


def _resolve_command(command: str) -> str:
    candidates = [command]
    if os.name == "nt":
        candidates = [f"{command}.cmd", f"{command}.exe", command]
    for candidate in candidates:
        resolved = shutil.which(candidate)
        if resolved:
            return resolved
    raise FileNotFoundError(f"Required command not found on PATH: {command}")


def _screenshot_jobs(master_url: str, history_url: str, screenshots_path: Path) -> list[tuple[str, Path, str]]:
    return [
        (
            master_url.rstrip("/"),
            screenshots_path / "spark_master_ui.png",
            "text=Spark Master",
        ),
        (
            history_url.rstrip("/"),
            screenshots_path / "spark_history_server.png",
            "text=Applications",
        ),
    ]


def _capture_screenshots(*, master_url: str, history_url: str, screenshots_path: Path) -> dict[str, str]:
    screenshots_path.mkdir(parents=True, exist_ok=True)
    for filename in ["spark_master_ui.png", "spark_history_server.png"]:
        (screenshots_path / filename).unlink(missing_ok=True)
    env = os.environ.copy()
    env.setdefault("PLAYWRIGHT_BROWSERS_PATH", "0")
    npx = _resolve_command("npx")
    subprocess.run([npx, "playwright", "install", "chromium"], check=True, env=env)

    for url, destination, selector in _screenshot_jobs(master_url, history_url, screenshots_path):
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
        "spark_master_ui": str((screenshots_path / "spark_master_ui.png").resolve()),
        "spark_history_server": str((screenshots_path / "spark_history_server.png").resolve()),
    }


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
    screenshot_capturer: ScreenshotCapturer = _capture_screenshots,
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

    screenshots_path = evidence_path / "screenshots"
    screenshots_path.mkdir(exist_ok=True)
    (screenshots_path / "README.md").write_text(
        "Capture required screenshots here: spark_master_ui.png, spark_history_server.png.\n",
        encoding="utf-8",
    )
    screenshot_capturer(master_url=master_url, history_url=history_url, screenshots_path=screenshots_path)

    manifest = {
        "captured_at": datetime.now(timezone.utc).isoformat(),
        "service_urls": {
            "spark_master_ui": master_url,
            "spark_history_server": history_url,
        },
        "artifacts": [
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
            "screenshots/README.md",
            "screenshots/spark_master_ui.png",
            "screenshots/spark_history_server.png",
        ],
    }
    (evidence_path / "run_manifest.json").write_text(
        json.dumps(manifest, indent=2, sort_keys=True),
        encoding="utf-8",
    )
    return manifest
