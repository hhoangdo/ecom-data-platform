from __future__ import annotations

import json
import re
from datetime import datetime, timezone
from pathlib import Path

import requests

REPO_ROOT = Path(__file__).resolve().parents[2]
EVIDENCE_ROOT = REPO_ROOT / "evidence" / "09_datahub_governance"
DATAHUB_RUNS_ROOT = REPO_ROOT / "evidence" / "08_airflow_gx" / "runs" / "datahub_ingestion"
GMS_URL = "http://localhost:8087"

GRAPHQL_DATASET_QUERY = """
query GetDataset($urn: String!) {
  dataset(urn: $urn) {
    urn
    name
    platform {
      name
    }
    globalTags {
      tags {
        tag {
          urn
          name
        }
      }
    }
  }
}
""".strip()

GRAPHQL_TAG_QUERY = """
query GetTag($urn: String!) {
  tag(urn: $urn) {
    urn
    name
  }
}
""".strip()

REPRESENTATIVE_DATASET_URNS = {
    "iceberg_fact_order": "urn:li:dataset:(urn:li:dataPlatform:iceberg,vina_bim_shop.fact_order,PROD)",
    "kafka_commerce_events": "urn:li:dataset:(urn:li:dataPlatform:kafka,commerce_events,PROD)",
    "pinot_realtime_commerce_metrics_1m": "urn:li:dataset:(urn:li:dataPlatform:pinot,realtime_commerce_metrics_1m,PROD)",
    "s3_spark_event_log_prefix": "urn:li:dataset:(urn:li:dataPlatform:s3,checkpoints.spark-events,PROD)",
}

TAG_URNS = [
    "urn:li:tag:bronze",
    "urn:li:tag:silver",
    "urn:li:tag:gold",
    "urn:li:tag:official",
    "urn:li:tag:provisional",
    "urn:li:tag:quality_gate",
]

DATASET_COUNT_RECIPES = (
    "kafka_topics",
    "minio_storage",
    "trino_tables",
    "dbt_legacy",
)

DATASET_COUNT_PATTERN = re.compile(r"'datasetProperties':\s*(\d+)")


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _write_json(path: Path, payload: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")


def _graphql(query: str, variables: dict[str, str]) -> dict:
    response = requests.post(
        f"{GMS_URL}/api/graphql",
        json={"query": query, "variables": variables},
        timeout=20,
        headers={"Content-Type": "application/json"},
    )
    response.raise_for_status()
    return response.json()


def _load_latest_successful_ingestion_manifest() -> tuple[str, dict] | None:
    manifests = sorted(DATAHUB_RUNS_ROOT.glob("*/run_manifest.json"), reverse=True)
    for manifest_path in manifests:
        payload = json.loads(manifest_path.read_text(encoding="utf-8"))
        if payload.get("status") == "success":
            return manifest_path.parent.name, payload
    return None


def capture_gms_health() -> dict:
    try:
        response = requests.get(f"{GMS_URL}/health", timeout=10)
        body = response.text.strip()
        payload: dict[str, object] = {
            "healthy": response.ok,
            "status_code": response.status_code,
            "url": f"{GMS_URL}/health",
        }
        if body:
            try:
                payload["body"] = response.json()
            except ValueError:
                payload["body"] = body
        else:
            payload["body"] = ""
        return payload
    except Exception as exc:
        return {"healthy": False, "error": str(exc), "url": f"{GMS_URL}/health"}


def _extract_dataset_counts(ingestion_manifest: dict) -> dict[str, int]:
    counts: dict[str, int] = {}
    ingestion_results = ingestion_manifest.get("ingestion_results", {})

    for recipe_name in DATASET_COUNT_RECIPES:
        recipe_result = ingestion_results.get(recipe_name, {})
        output = recipe_result.get("output", "")
        match = DATASET_COUNT_PATTERN.search(output)
        if match:
            counts[recipe_name] = int(match.group(1))

    return counts


def _capture_representative_datasets() -> dict[str, dict]:
    verified: dict[str, dict] = {}
    for label, urn in REPRESENTATIVE_DATASET_URNS.items():
        try:
            payload = _graphql(GRAPHQL_DATASET_QUERY, {"urn": urn})
            dataset = payload.get("data", {}).get("dataset")
            verified[label] = {
                "verified": dataset is not None,
                "dataset": dataset,
            }
        except Exception as exc:
            verified[label] = {
                "verified": False,
                "error": str(exc),
                "urn": urn,
            }
    return verified


def capture_dataset_evidence() -> dict:
    latest_manifest = _load_latest_successful_ingestion_manifest()
    if latest_manifest is None:
        return {"status": "missing", "error": "No successful datahub_ingestion manifest found"}

    run_id, manifest = latest_manifest
    counts_by_recipe = _extract_dataset_counts(manifest)
    custom_lineage = manifest.get("ingestion_results", {}).get("custom_lineage", {})
    spark_results = custom_lineage.get("spark", {})
    flink_results = custom_lineage.get("flink", {})
    gx_results = custom_lineage.get("gx_assertions", {})

    return {
        "status": "success",
        "latest_successful_run_id": run_id,
        "captured_from_manifest_at": manifest.get("captured_at"),
        "counts_by_recipe": counts_by_recipe,
        "total_datasets_emitted": sum(counts_by_recipe.values()),
        "custom_lineage": {
            "spark_entities": len(spark_results),
            "flink_entities": len(flink_results),
            "gx_assertions_emitted": gx_results.get("assertions_emitted", 0),
        },
        "verified_representative_datasets": _capture_representative_datasets(),
        "note": "Representative entities are verified directly via GMS GraphQL lookups; search indexing is not required for this evidence pass.",
    }


def capture_tag_evidence() -> dict:
    verified_tags: list[dict[str, str]] = []
    failed_tags: list[dict[str, str]] = []

    for urn in TAG_URNS:
        try:
            payload = _graphql(GRAPHQL_TAG_QUERY, {"urn": urn})
            tag = payload.get("data", {}).get("tag")
            if tag is None:
                failed_tags.append({"urn": urn, "error": "Tag not found"})
            else:
                verified_tags.append(tag)
        except Exception as exc:
            failed_tags.append({"urn": urn, "error": str(exc)})

    return {
        "status": "success" if not failed_tags else "partial",
        "verified_tag_count": len(verified_tags),
        "verified_tags": verified_tags,
        "failed_tags": failed_tags,
    }


def main() -> None:
    health = capture_gms_health()
    _write_json(EVIDENCE_ROOT / "datahub_health.json", health)

    datasets = capture_dataset_evidence()
    _write_json(EVIDENCE_ROOT / "dataset_count.json", datasets)

    tags = capture_tag_evidence()
    _write_json(EVIDENCE_ROOT / "tag_count.json", tags)

    manifest = {
        "captured_at": _utc_now(),
        "gms_url": GMS_URL,
        "latest_successful_datahub_ingestion_run": datasets.get("latest_successful_run_id"),
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
