from __future__ import annotations

import json
from collections.abc import Callable
from pathlib import Path
from typing import Any

import requests


SOURCE_SCHEMA_SUBJECTS = [
    "commerce_events-value",
    "catalog_events-value",
    "fulfillment_events-value",
    "ops_events-value",
    "dead_letter_events-value",
]


PostCallable = Callable[..., Any]


def register_schema_subjects(
    *,
    registry_url: str,
    schemas_dir: str | Path,
    evidence_root: str | Path,
    post: PostCallable = requests.post,
) -> dict[str, Any]:
    schemas_path = Path(schemas_dir)
    responses: dict[str, Any] = {}

    for subject in SOURCE_SCHEMA_SUBJECTS:
        schema_text = (schemas_path / f"{subject}.schema.json").read_text(encoding="utf-8")
        response = post(
            f"{registry_url.rstrip('/')}/subjects/{subject}/versions",
            json={"subject": subject, "schemaType": "JSON", "schema": schema_text},
            headers={"Content-Type": "application/vnd.schemaregistry.v1+json"},
            timeout=30,
        )
        response.raise_for_status()
        responses[subject] = response.json()

    evidence_path = Path(evidence_root)
    evidence_path.mkdir(parents=True, exist_ok=True)
    (evidence_path / "schema_registry_subjects.json").write_text(
        json.dumps(responses, indent=2, sort_keys=True),
        encoding="utf-8",
    )
    return responses
