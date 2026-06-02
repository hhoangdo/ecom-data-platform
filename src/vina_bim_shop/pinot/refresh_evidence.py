from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import requests

from vina_bim_shop.pinot.bootstrap import DEFAULT_EVIDENCE_ROOT, apply_assets
from vina_bim_shop.pinot.evidence import capture_evidence
from vina_bim_shop.pinot.query_examples import run_query_examples


def _payload_is_healthy(payload: Any) -> bool:
    if isinstance(payload, dict):
        candidates = [
            payload.get("status", ""),
            payload.get("text", ""),
            payload.get("body", ""),
        ]
        return any(str(candidate).strip().upper() in {"GOOD", "HEALTHY", "OK"} for candidate in candidates)
    return str(payload).strip().upper() in {"GOOD", "HEALTHY", "OK"}


def _require_http_json(url: str, service_name: str) -> dict[str, Any]:
    try:
        response = requests.get(url, timeout=30)
        response.raise_for_status()
    except requests.RequestException as exc:
        raise RuntimeError(f"{service_name} is unavailable at {url}: {exc}") from exc
    if not response.content:
        return {}
    if "application/json" not in response.headers.get("content-type", ""):
        payload = {"text": response.text}
        if service_name.startswith("Pinot") and not _payload_is_healthy(payload):
            raise RuntimeError(f"{service_name} is unhealthy at {url}: {response.text.strip()}")
        return payload
    try:
        payload = response.json()
    except ValueError as exc:
        raise RuntimeError(f"{service_name} did not return JSON at {url}.") from exc
    if service_name.startswith("Pinot") and not _payload_is_healthy(payload):
        raise RuntimeError(f"{service_name} is unhealthy at {url}: {payload}")
    return payload


def refresh_evidence(
    *,
    controller_url: str = "http://localhost:9003",
    broker_url: str = "http://localhost:8000",
    trino_url: str = "http://localhost:8080",
    trino_user: str = "vina_analyst",
    evidence_root: str | Path = DEFAULT_EVIDENCE_ROOT,
    start_ts: str | None = None,
    end_ts: str | None = None,
) -> dict[str, Any]:
    evidence_path = Path(evidence_root)
    evidence_path.mkdir(parents=True, exist_ok=True)

    _require_http_json(f"{controller_url.rstrip('/')}/health", "Pinot controller")
    _require_http_json(f"{broker_url.rstrip('/')}/health", "Pinot broker")
    _require_http_json(f"{trino_url.rstrip('/')}/v1/info", "Trino")

    bootstrap_manifest = apply_assets(
        controller_url=controller_url,
        evidence_root=evidence_path,
    )
    query_manifest = run_query_examples(
        broker_url=broker_url,
        trino_url=trino_url,
        trino_user=trino_user,
        evidence_root=evidence_path,
        start_ts=start_ts,
        end_ts=end_ts,
    )
    evidence_manifest = capture_evidence(
        controller_url=controller_url,
        broker_url=broker_url,
        evidence_root=evidence_path,
    )

    manifest = {
        "captured_at": datetime.now(timezone.utc).isoformat(),
        "controller_url": controller_url,
        "broker_url": broker_url,
        "trino_url": trino_url,
        "tables": bootstrap_manifest.get("tables", []),
        "bootstrap_manifest": "pinot_bootstrap_manifest.json",
        "query_manifest": "query_examples_manifest.json",
        "run_manifest": "run_manifest.json",
        "artifacts": sorted(
            {
                *query_manifest.get("artifacts", []),
                *evidence_manifest.get("artifacts", []),
                "refresh_evidence_manifest.json",
            }
        ),
    }
    (evidence_path / "refresh_evidence_manifest.json").write_text(
        json.dumps(manifest, indent=2, sort_keys=True),
        encoding="utf-8",
    )
    return manifest
