from __future__ import annotations

import json
import subprocess
from collections.abc import Callable
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import requests

from vina_bim_shop.lakehouse.smoke import run_smoke_sql


DEFAULT_EVIDENCE_ROOT = Path("evidence/04_lakehouse")
REQUIRED_BUCKETS = ("bronze", "silver", "gold", "checkpoints", "evidence")

GetJson = Callable[[str], Any]
RunCommand = Callable[[list[str]], str]


def bronze_layout_examples() -> dict[str, str]:
    return {
        "batch": "bronze/batch/customers/snapshot_date=2026-06-01/part-000.parquet",
        "events": "bronze/events/commerce_events/ingest_date=2026-06-01/000000.jsonl",
    }


def _get_json(url: str) -> Any:
    response = requests.get(url, timeout=30)
    response.raise_for_status()
    return response.json()


def get_http_artifact(url: str, *, get: Callable[..., Any] = requests.get) -> dict[str, Any]:
    response = get(url, timeout=30)
    response.raise_for_status()
    try:
        body: Any = response.json()
    except ValueError:
        body = response.text
    return {"url": url, "status_code": response.status_code, "body": body}


def _run_command(command: list[str]) -> str:
    completed = subprocess.run(command, check=True, text=True, capture_output=True)
    return completed.stdout


def _write_json(path: Path, payload: Any) -> None:
    path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")


def parse_bucket_listing(listing: str) -> list[str]:
    buckets = []
    for line in listing.splitlines():
        stripped = line.strip()
        if not stripped or not stripped.endswith("/"):
            continue
        name = stripped.rsplit(" ", 1)[-1].rstrip("/")
        if name:
            buckets.append(name)
    return buckets


def capture_bronze_landing_evidence(
    *,
    evidence_root: str | Path,
    listing_text: str | None = None,
    listing_path: str | Path | None = None,
    run_command: RunCommand | None = None,
) -> dict[str, Any]:
    if listing_text is None:
        if listing_path is not None:
            listing_text = Path(listing_path).read_text(encoding="utf-8")
        elif run_command is not None:
            listing_text = run_command([])
        else:
            raise ValueError("Expected listing_text, listing_path, or run_command.")

    examples = {"batch": [], "events": []}
    for line in listing_text.splitlines():
        for prefix in ("bronze/batch/", "bronze/events/", "batch/", "events/"):
            start = line.find(prefix)
            if start == -1:
                continue
            entry = line[start:].strip()
            if not entry.startswith("bronze/"):
                entry = f"bronze/{entry}"
            if "/batch/" in entry:
                examples["batch"].append(entry)
            else:
                examples["events"].append(entry)
            break

    evidence_path = Path(evidence_root)
    evidence_path.mkdir(parents=True, exist_ok=True)
    artifact_name = "bronze_landing_examples.json"
    _write_json(evidence_path / artifact_name, examples)
    return {"artifacts": [artifact_name]}


def capture_evidence(
    *,
    evidence_root: str | Path = DEFAULT_EVIDENCE_ROOT,
    minio_endpoint: str = "http://localhost:9000",
    minio_console_url: str = "http://localhost:9001",
    trino_url: str = "http://localhost:8080",
    get_json: GetJson = _get_json,
    get_http: Callable[[str], dict[str, Any]] = get_http_artifact,
    run_command: RunCommand = _run_command,
) -> dict[str, Any]:
    evidence_path = Path(evidence_root)
    evidence_path.mkdir(parents=True, exist_ok=True)

    minio_health = get_http(f"{minio_endpoint.rstrip('/')}/minio/health/ready")
    trino_info = get_json(f"{trino_url.rstrip('/')}/v1/info")
    bucket_listing = run_command(
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
            'mc alias set ALIAS http://minio:9000 "${MINIO_ROOT_USER}" "${MINIO_ROOT_PASSWORD}" >/dev/null && mc ls ALIAS',
        ]
    )
    postgres_health = run_command(["docker", "compose", "exec", "-T", "lakehouse-postgres", "pg_isready", "-U", "vina_platform", "-d", "platform"])
    hive_health = run_command(["docker", "compose", "ps", "hive-metastore"])
    trino_catalogs = run_command(["docker", "compose", "exec", "-T", "trino", "trino", "--execute", "SHOW CATALOGS"])
    trino_schemas = run_command(["docker", "compose", "exec", "-T", "trino", "trino", "--execute", "SHOW SCHEMAS FROM iceberg"])
    trino_smoke = run_smoke_sql(run_command=run_command)

    bucket_names = parse_bucket_listing(bucket_listing)
    missing_buckets = [bucket for bucket in REQUIRED_BUCKETS if bucket not in bucket_names]

    _write_json(evidence_path / "minio_health.json", minio_health)
    _write_json(
        evidence_path / "minio_buckets.json",
        {
            "required_buckets": list(REQUIRED_BUCKETS),
            "observed_buckets": bucket_names,
            "missing_buckets": missing_buckets,
            "raw_listing": bucket_listing,
        },
    )
    (evidence_path / "postgres_health.txt").write_text(postgres_health, encoding="utf-8")
    (evidence_path / "hive_metastore_health.txt").write_text(hive_health, encoding="utf-8")
    _write_json(evidence_path / "trino_info.json", trino_info)
    (evidence_path / "trino_catalogs.txt").write_text(trino_catalogs, encoding="utf-8")
    (evidence_path / "trino_schemas.txt").write_text(trino_schemas, encoding="utf-8")
    (evidence_path / "trino_smoke_query.txt").write_text(trino_smoke, encoding="utf-8")

    version_matrix = {
        "minio": "minio/minio:RELEASE.2025-04-22T22-12-26Z",
        "minio_client": "minio/mc:RELEASE.2025-04-16T18-13-26Z",
        "postgres": "postgres:16.4",
        "hive_metastore": "vina-bim-shop/hive-metastore:4.1.0-postgres",
        "trino": "trinodb/trino:476",
    }
    _write_json(evidence_path / "version_matrix.json", version_matrix)

    screenshots_path = evidence_path / "screenshots"
    screenshots_path.mkdir(exist_ok=True)
    (screenshots_path / "README.md").write_text(
        "Capture required screenshots here: minio_buckets.png, trino_query_history.png, trino_sample_query.png.\n",
        encoding="utf-8",
    )

    manifest = {
        "captured_at": datetime.now(timezone.utc).isoformat(),
        "service_urls": {
            "minio": minio_endpoint,
            "minio_console": minio_console_url,
            "trino": trino_url,
        },
        "artifacts": [
            "minio_health.json",
            "minio_buckets.json",
            "postgres_health.txt",
            "hive_metastore_health.txt",
            "trino_info.json",
            "trino_catalogs.txt",
            "trino_schemas.txt",
            "trino_smoke_query.txt",
            "version_matrix.json",
            "screenshots/README.md",
            "screenshots/minio_buckets.png",
            "screenshots/trino_query_history.png",
            "screenshots/trino_sample_query.png",
        ],
    }
    _write_json(evidence_path / "run_manifest.json", manifest)

    if missing_buckets:
        raise RuntimeError(f"Missing required MinIO buckets: {', '.join(missing_buckets)}")

    return manifest
