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


DEFAULT_EVIDENCE_ROOT = Path("evidence/07_pinot_serving")


class ScreenshotCapturer(Protocol):
    def __call__(self, *, controller_url: str, screenshots_path: Path) -> dict[str, str]: ...


def _resolve_command(command: str) -> str:
    candidates = [command]
    if os.name == "nt":
        candidates = [f"{command}.cmd", f"{command}.exe", command]
    for candidate in candidates:
        resolved = shutil.which(candidate)
        if resolved:
            return resolved
    raise FileNotFoundError(f"Required command not found on PATH: {command}")


def _capture_screenshots(*, controller_url: str, screenshots_path: Path) -> dict[str, str]:
    screenshots_path.mkdir(parents=True, exist_ok=True)
    for filename in ["pinot_tables.png", "pinot_query_console.png"]:
        (screenshots_path / filename).unlink(missing_ok=True)
    env = os.environ.copy()
    env.setdefault("PLAYWRIGHT_BROWSERS_PATH", "0")
    npx = _resolve_command("npx")
    subprocess.run([npx, "playwright", "install", "chromium"], check=True, env=env)

    jobs = [
        (controller_url.rstrip("/"), screenshots_path / "pinot_tables.png", "text=Tables"),
        (f"{controller_url.rstrip('/')}/#/query", screenshots_path / "pinot_query_console.png", "text=Query Console"),
    ]
    for url, destination, selector in jobs:
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
        "tables": str((screenshots_path / "pinot_tables.png").resolve()),
        "query_console": str((screenshots_path / "pinot_query_console.png").resolve()),
    }


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
    screenshot_capturer: ScreenshotCapturer = _capture_screenshots,
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

    screenshots_path = evidence_path / "screenshots"
    screenshots_path.mkdir(exist_ok=True)
    (screenshots_path / "README.md").write_text(
        "Capture required screenshots here: pinot_tables.png, pinot_query_console.png.\n",
        encoding="utf-8",
    )
    screenshot_capturer(controller_url=controller_url, screenshots_path=screenshots_path)

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
            "screenshots/README.md",
            "screenshots/pinot_tables.png",
            "screenshots/pinot_query_console.png",
            *query_output_artifacts,
        ],
    }
    (evidence_path / "run_manifest.json").write_text(json.dumps(manifest, indent=2, sort_keys=True), encoding="utf-8")
    return manifest
