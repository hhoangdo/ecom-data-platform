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
ELASTICSEARCH_URL = "http://localhost:9200"

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

GRAPHQL_SEARCH_QUERY = """
query SearchDatasets($input: SearchInput!) {
  search(input: $input) {
    start
    count
    total
    searchResults {
      entity {
        urn
      }
    }
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


def _capture_elasticsearch_indices() -> dict:
    try:
        health_response = requests.get(f"{ELASTICSEARCH_URL}/_cluster/health", timeout=20)
        health_response.raise_for_status()
        health = health_response.json()
        indices_response = requests.get(f"{ELASTICSEARCH_URL}/_cat/indices?format=json&bytes=b", timeout=20)
        indices_response.raise_for_status()
        indices = indices_response.json()
        populated_indices = [
            index
            for index in indices
            if not str(index.get("index", "")).startswith(".") and int(index.get("docs.count", 0)) > 0
        ]
        if health.get("status") not in {"yellow", "green"}:
            raise RuntimeError(f"Elasticsearch health is {health.get('status')!r}")
        if not populated_indices:
            raise RuntimeError("Elasticsearch has no populated DataHub indices")
        return {"status": "success", "health": health, "indices": indices, "populated_indices": populated_indices}
    except Exception as exc:
        return {"status": "failed", "error": str(exc), "url": ELASTICSEARCH_URL}


def capture_search_evidence() -> dict:
    elasticsearch = _capture_elasticsearch_indices()
    if elasticsearch["status"] != "success":
        return {"status": "failed", "error": str(elasticsearch["error"]), "elasticsearch": elasticsearch}

    results: dict[str, dict] = {}
    failures: list[dict[str, str]] = []
    for label, urn in REPRESENTATIVE_DATASET_URNS.items():
        try:
            query = urn.split(",")[1]
            payload = _graphql(
                GRAPHQL_SEARCH_QUERY,
                {"input": {"type": "DATASET", "query": query, "start": 0, "count": 100}},
            )
            if payload.get("errors"):
                raise RuntimeError(str(payload["errors"]))
            search = payload.get("data", {}).get("search", {})
            found_urns = [
                item.get("entity", {}).get("urn")
                for item in search.get("searchResults", [])
                if item.get("entity", {}).get("urn")
            ]
            results[label] = {"expected_urn": urn, "found_urns": found_urns, "total": search.get("total", 0)}
            if urn not in found_urns:
                failures.append({"label": label, "error": f"expected URN was not indexed: {urn}"})
        except Exception as exc:
            results[label] = {"expected_urn": urn, "error": str(exc)}
            failures.append({"label": label, "error": str(exc)})

    return {
        "status": "success" if not failures else "failed",
        "elasticsearch": elasticsearch,
        "results": results,
        "failures": failures,
    }


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


def _manifest_status(health: dict, datasets: dict, tags: dict, search: dict) -> tuple[str, list[dict[str, str]]]:
    failures: list[dict[str, str]] = []
    partial = False

    if not health.get("healthy"):
        failures.append({"step": "gms_health", "error": str(health.get("error") or health.get("body") or "GMS health check failed")})

    dataset_status = datasets.get("status")
    if dataset_status != "success":
        failures.append({"step": "dataset_evidence", "error": str(datasets.get("error") or f"dataset evidence status is {dataset_status}")})

    if tags.get("status") == "partial":
        partial = True
        failed_count = len(tags.get("failed_tags", []))
        failures.append({"step": "tag_evidence", "error": f"{failed_count} tag lookups failed"})
    elif tags.get("status") not in {"success", None}:
        failures.append({"step": "tag_evidence", "error": str(tags.get("error") or f"tag evidence status is {tags.get('status')}")})

    if search.get("status") != "success":
        failures.append({"step": "search_evidence", "error": str(search.get("error") or search.get("failures") or "indexed search failed")})

    if any(failure["step"] in {"gms_health", "dataset_evidence", "search_evidence"} for failure in failures):
        return "failed", failures
    if partial or failures:
        return "partial", failures
    return "success", failures


def capture_evidence() -> dict:
    health = capture_gms_health()
    _write_json(EVIDENCE_ROOT / "datahub_health.json", health)

    datasets = capture_dataset_evidence()
    _write_json(EVIDENCE_ROOT / "dataset_count.json", datasets)

    tags = capture_tag_evidence()
    _write_json(EVIDENCE_ROOT / "tag_count.json", tags)

    search = capture_search_evidence()
    _write_json(EVIDENCE_ROOT / "search_results.json", search)

    status, failures = _manifest_status(health, datasets, tags, search)
    manifest = {
        "captured_at": _utc_now(),
        "status": status,
        "failures": failures,
        "gms_url": GMS_URL,
        "latest_successful_datahub_ingestion_run": datasets.get("latest_successful_run_id"),
        "artifacts": [
            "datahub_health.json",
            "dataset_count.json",
            "tag_count.json",
            "search_results.json",
        ],
    }
    _write_json(EVIDENCE_ROOT / "run_manifest.json", manifest)
    return manifest


def main() -> None:
    manifest = capture_evidence()
    print(json.dumps(manifest, indent=2))
    if manifest["status"] == "failed":
        raise SystemExit(1)


if __name__ == "__main__":
    main()
