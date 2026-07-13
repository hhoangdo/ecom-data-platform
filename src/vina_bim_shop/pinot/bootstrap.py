"""Apply the deployable Pinot schema and table assets and record their status."""

from __future__ import annotations

import json
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable

import requests


REPO_ROOT = Path(__file__).resolve().parents[3]
SCHEMA_DIR = REPO_ROOT / "infra" / "pinot" / "schemas"
TABLE_DIR = REPO_ROOT / "infra" / "pinot" / "tables"
DEFAULT_EVIDENCE_ROOT = Path("evidence/07_pinot_serving")


def _request_factory(*, controller_url: str) -> Callable[[str, str], Any]:
    base_url = controller_url.rstrip("/")

    def _request(method: str, path: str, *, payload: dict[str, Any] | None = None) -> Any:
        response = requests.request(method, f"{base_url}{path}", json=payload, timeout=30)
        response.raise_for_status()
        if not response.content:
            return {}
        if "application/json" in response.headers.get("content-type", ""):
            return response.json()
        return {"text": response.text}

    return _request


def _load_json_assets(directory: Path, key: str) -> list[tuple[str, dict[str, Any]]]:
    assets: list[tuple[str, dict[str, Any]]] = []
    for path in sorted(directory.glob("*.json")):
        payload = json.loads(path.read_text(encoding="utf-8"))
        assets.append((str(payload[key]), payload))
    return assets


def _table_listing(payload: Any) -> set[str]:
    if isinstance(payload, dict):
        tables = payload.get("tables", [])
    else:
        tables = payload
    normalized: set[str] = set()
    for table_name in tables or []:
        normalized.add(str(table_name))
        if str(table_name).endswith("_REALTIME"):
            normalized.add(str(table_name).removesuffix("_REALTIME"))
    return normalized


def _schema_listing(payload: Any) -> set[str]:
    if isinstance(payload, dict):
        schemas = payload.get("schemas", [])
    else:
        schemas = payload
    return {str(schema_name) for schema_name in schemas or []}


def _wait_for_table_status(table_name: str, request: Callable[[str, str], Any], retries: int = 15) -> dict[str, Any]:
    last_payload: dict[str, Any] = {}

    for _ in range(retries):
        try:
            payload = request("GET", f"/debug/tables/{table_name}")
            if isinstance(payload, list):
                first_item = payload[0] if payload else {}
                last_payload = {"table_debug": payload}
                ingestion_state = str(first_item.get("ingestionStatus", {}).get("ingestionState", "")).upper()
                if ingestion_state in {"HEALTHY", "GOOD"}:
                    return {"table_debug": payload}
            elif isinstance(payload, dict):
                last_payload = payload
                ingestion_state = str(payload.get("ingestionStatus", {}).get("ingestionState", "")).upper()
                if ingestion_state in {"HEALTHY", "GOOD"}:
                    return payload
        except requests.HTTPError:
            try:
                payload = request("GET", f"/tables/{table_name}_REALTIME/status")
                if isinstance(payload, dict):
                    last_payload = payload
                    if str(payload.get("status", "")).upper() in {"ONLINE", "GOOD"}:
                        return payload
            except requests.HTTPError:
                last_payload = {
                    "status": "unverified",
                    "reason": "Pinot controller status/debug endpoints were unavailable during bootstrap.",
                }
        time.sleep(2)
    return last_payload


def apply_assets(
    *,
    controller_url: str = "http://localhost:9003",
    evidence_root: str | Path = DEFAULT_EVIDENCE_ROOT,
    request: Callable[[str, str], Any] | None = None,
) -> dict[str, Any]:
    """Create or update Pinot assets and return the captured bootstrap manifest.

    Inputs are the controller endpoint, evidence destination, and optional request
    adapter. HTTP and asset-validation failures propagate instead of producing a
    successful bootstrap record.
    """

    request_fn = request or _request_factory(controller_url=controller_url)
    evidence_path = Path(evidence_root)
    evidence_path.mkdir(parents=True, exist_ok=True)

    schemas = _load_json_assets(SCHEMA_DIR, "schemaName")
    tables = _load_json_assets(TABLE_DIR, "tableName")
    existing_schemas = _schema_listing(request_fn("GET", "/schemas"))
    existing_tables = _table_listing(request_fn("GET", "/tables"))

    schema_actions: list[dict[str, str]] = []
    for schema_name, payload in schemas:
        if schema_name in existing_schemas:
            request_fn("PUT", f"/schemas/{schema_name}", payload=payload)
            schema_actions.append({"schema": schema_name, "action": "updated"})
        else:
            request_fn("POST", "/schemas", payload=payload)
            schema_actions.append({"schema": schema_name, "action": "created"})

    table_actions: list[dict[str, str]] = []
    table_status: dict[str, Any] = {}
    for table_name, payload in tables:
        if table_name in existing_tables:
            request_fn("PUT", f"/tables/{table_name}", payload=payload)
            table_actions.append({"table": table_name, "action": "updated"})
        else:
            request_fn("POST", "/tables", payload=payload)
            table_actions.append({"table": table_name, "action": "created"})
        table_status[table_name] = _wait_for_table_status(table_name, request_fn)

    manifest = {
        "captured_at": datetime.now(timezone.utc).isoformat(),
        "controller_url": controller_url,
        "schemas": [name for name, _payload in schemas],
        "tables": [name for name, _payload in tables],
        "schema_actions": schema_actions,
        "table_actions": table_actions,
        "table_status": table_status,
    }
    (evidence_path / "pinot_bootstrap_manifest.json").write_text(
        json.dumps(manifest, indent=2, sort_keys=True),
        encoding="utf-8",
    )
    return manifest
