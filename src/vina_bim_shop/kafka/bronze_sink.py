from __future__ import annotations

import json
from collections.abc import Callable
from pathlib import Path
from typing import Any

import requests


PostCallable = Callable[..., Any]
PutCallable = Callable[..., Any]


def _resolve_template_path(template_path: str | Path) -> Path:
    candidate = Path(template_path)
    if candidate.is_file():
        return candidate

    parts = candidate.parts
    if "infra" in parts:
        infra_index = parts.index("infra")
        repo_relative = Path(*parts[infra_index:])
        repo_root = Path(__file__).resolve().parents[3]
        fallback = repo_root / repo_relative
        if fallback.is_file():
            return fallback

    return candidate


def register_bronze_sink(
    *,
    connect_url: str,
    template_path: str | Path,
    connector_name: str,
    bronze_bucket: str,
    minio_endpoint: str,
    minio_region: str,
    minio_access_key: str,
    minio_secret_key: str,
    post: PostCallable = requests.post,
    put: PutCallable = requests.put,
) -> Any:
    payload = json.loads(_resolve_template_path(template_path).read_text(encoding="utf-8"))
    payload["name"] = connector_name

    config = payload.get("config", {})
    replacements = {
        "${BRONZE_BUCKET}": bronze_bucket,
        "${MINIO_ENDPOINT}": minio_endpoint,
        "${MINIO_REGION}": minio_region,
        "${MINIO_ACCESS_KEY}": minio_access_key,
        "${MINIO_SECRET_KEY}": minio_secret_key,
    }
    for key, value in list(config.items()):
        if isinstance(value, str):
            for placeholder, replacement in replacements.items():
                value = value.replace(placeholder, replacement)
            config[key] = value

    response = post(f"{connect_url.rstrip('/')}/connectors", json=payload)
    if getattr(response, "status_code", None) == 409:
        response = put(
            f"{connect_url.rstrip('/')}/connectors/{connector_name}/config",
            json=config,
        )
    if hasattr(response, "raise_for_status"):
        response.raise_for_status()
    return response.json() if hasattr(response, "json") else response
