from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

import requests

REPO_ROOT = Path(__file__).resolve().parents[2]
EVIDENCE_ROOT = REPO_ROOT / "evidence" / "09_datahub_governance"
GMS_URL = "http://localhost:8087"


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _write_json(path: Path, payload: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")


def capture_gms_health() -> dict:
    try:
        resp = requests.get(f"{GMS_URL}/health", timeout=10)
        return resp.json()
    except Exception as exc:
        return {"error": str(exc)}


def capture_entities(entity_type: str) -> dict:
    try:
        resp = requests.post(
            f"{GMS_URL}/entities?action=search",
            json={"input": "*", "systemMetadata": False, "entity": entity_type, "start": 0, "count": 500},
            timeout=30,
            headers={"Content-Type": "application/json"},
        )
        return {"status": resp.status_code, "count": len(resp.json().get("entities", []))}
    except Exception as exc:
        return {"error": str(exc)}


def main() -> None:
    health = capture_gms_health()
    _write_json(EVIDENCE_ROOT / "datahub_health.json", health)

    datasets = capture_entities("dataset")
    _write_json(EVIDENCE_ROOT / "dataset_count.json", datasets)

    tags_entities = capture_entities("tag")
    _write_json(EVIDENCE_ROOT / "tag_count.json", tags_entities)

    manifest = {
        "captured_at": _utc_now(),
        "gms_url": GMS_URL,
        "artifacts": [
            "datahub_health.json",
            "dataset_count.json",
            "tag_count.json",
        ],
    }
    _write_json(EVIDENCE_ROOT / "run_manifest.json", manifest)
    print(json.dumps(manifest, indent=2))


if __name__ == "__main__":
    main()
