from __future__ import annotations

import json
import hashlib
import re
from datetime import datetime, timezone
from pathlib import Path

import requests
from PIL import Image

from vina_bim_shop.datahub_lineage.coursework_pipelines import (
    ASSERTION_TARGETS,
    COURSEWORK_DATAFLOW_URN,
    DATAFLOW_ID,
    datajob_urn,
    coursework_pipeline_entities,
    FEATURE_TABLES,
    ice_urn,
    s3_urn,
)

REPO_ROOT = Path(__file__).resolve().parents[2]
EVIDENCE_ROOT = REPO_ROOT / "evidence" / "09_datahub_governance"
DATAHUB_RUNS_ROOT = REPO_ROOT / "evidence" / "08_airflow_gx" / "runs" / "datahub_ingestion"
COURSEWORK_PIPELINE_EVIDENCE_ROOT = EVIDENCE_ROOT / "coursework_pipeline"
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

GRAPHQL_DATAFLOW_QUERY = """
query GetDataFlow($urn: String!) {
  dataFlow(urn: $urn) {
    urn
    properties {
      name
      description
    }
  }
}
""".strip()

GRAPHQL_DATAJOB_QUERY = """
query GetDataJob($urn: String!) {
  dataJob(urn: $urn) {
    urn
    properties {
      name
      description
    }
    dataFlow {
      urn
    }
    inputOutput {
      inputDatasets {
        urn
      }
      outputDatasets {
        urn
      }
    }
  }
}
""".strip()

GRAPHQL_DATASET_SCHEMA_QUERY = """
query GetDatasetSchema($urn: String!) {
  dataset(urn: $urn) {
    urn
    schemaMetadata {
      fields {
        fieldPath
        nativeDataType
      }
    }
  }
}
""".strip()

GRAPHQL_ASSERTION_QUERY = """
query GetAssertion($urn: String!) {
  assertion(urn: $urn) {
    urn
    info {
      datasetAssertion {
        datasetUrn
      }
    }
    runEvents(limit: 1) {
      total
      succeeded
      runEvents {
        asserteeUrn
        result {
          type
        }
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

COURSEWORK_SCREENSHOT_TARGETS = (
    {
        "id": "dp1_lineage",
        "path": "../screenshots/datahub_dp1_lineage.png",
        "entity_urn": datajob_urn(DATAFLOW_ID, "dp1_raw_to_bronze"),
        "url": "http://localhost:9002/",
    },
    {
        "id": "dp1_contract",
        "path": "../screenshots/datahub_dp1_contract.png",
        "entity_urn": s3_urn("bronze.batch"),
        "url": "http://localhost:9002/",
    },
    {
        "id": "dp2_lineage",
        "path": "../screenshots/datahub_dp2_lineage.png",
        "entity_urn": datajob_urn(DATAFLOW_ID, "dp2_bronze_to_silver_gold"),
        "url": "http://localhost:9002/",
    },
    {
        "id": "dp2_contract",
        "path": "../screenshots/datahub_dp2_contract.png",
        "entity_urn": ice_urn("fact_order"),
        "url": "http://localhost:9002/",
    },
    {
        "id": "dp3_lineage",
        "path": "../screenshots/datahub_dp3_lineage.png",
        "entity_urn": datajob_urn(DATAFLOW_ID, "dp3_offline_features"),
        "url": "http://localhost:9002/",
    },
    {
        "id": "dp3_contract",
        "path": "../screenshots/datahub_dp3_contract.png",
        "entity_urn": ice_urn("feat_customer_unified"),
        "url": "http://localhost:9002/",
    },
)

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


def validate_coursework_screenshot_manifest(root: Path = COURSEWORK_PIPELINE_EVIDENCE_ROOT) -> dict:
    manifest_path = root / "ui_screenshot_manifest.json"
    if not manifest_path.is_file():
        return {"status": "failed", "failures": [{"id": "manifest", "error": "screenshot manifest is missing"}]}
    try:
        entries = json.loads(manifest_path.read_text(encoding="utf-8")).get("screenshots", [])
    except (OSError, ValueError) as exc:
        return {"status": "failed", "failures": [{"id": "manifest", "error": str(exc)}]}
    by_id = {entry.get("id"): entry for entry in entries if isinstance(entry, dict)}
    failures: list[dict[str, str]] = []
    verified: list[dict[str, object]] = []
    for target in COURSEWORK_SCREENSHOT_TARGETS:
        entry = by_id.get(target["id"])
        if entry is None:
            failures.append({"id": target["id"], "error": "screenshot entry is missing"})
            continue
        if entry.get("path") != target["path"] or entry.get("entity_urn") != target["entity_urn"]:
            failures.append({"id": target["id"], "error": "path or entity URN does not match the required target"})
            continue
        if not isinstance(entry.get("url"), str) or not entry["url"].startswith("http://localhost:9002/"):
            failures.append({"id": target["id"], "error": "DataHub page URL is missing"})
            continue
        if not entry.get("captured_at") or entry.get("reloaded") is not True:
            failures.append({"id": target["id"], "error": "capture timestamp or reload confirmation is missing"})
            continue
        image_path = (root / str(entry["path"])).resolve()
        if not image_path.is_file():
            failures.append({"id": target["id"], "error": "PNG file is missing"})
            continue
        try:
            with Image.open(image_path) as image:
                width, height = image.size
        except Exception as exc:
            failures.append({"id": target["id"], "error": f"PNG cannot be opened: {exc}"})
            continue
        digest = hashlib.sha256(image_path.read_bytes()).hexdigest()
        if width <= 0 or height <= 0 or entry.get("width") != width or entry.get("height") != height:
            failures.append({"id": target["id"], "error": "PNG dimensions are invalid or do not match the manifest"})
            continue
        if entry.get("sha256") != digest:
            failures.append({"id": target["id"], "error": "PNG SHA-256 does not match the manifest"})
            continue
        verified.append({"id": target["id"], "path": target["path"], "width": width, "height": height, "sha256": digest})
    return {"status": "success" if not failures else "failed", "screenshots": verified, "failures": failures}


def coursework_gate_status(machine: dict, screenshots: dict) -> tuple[str, list[dict[str, str]]]:
    failures: list[dict[str, str]] = []
    if machine.get("status") != "success":
        failures.append({"step": "coursework_machine_evidence", "error": str(machine.get("error") or "coursework machine evidence failed")})
    if machine.get("indexed_search", {}).get("status") != "success":
        failures.append({"step": "coursework_indexed_search", "error": str(machine.get("indexed_search", {}).get("error") or "coursework entities are not indexed")})
    if screenshots.get("status") != "success":
        failures.append({"step": "coursework_screenshots", "error": str(screenshots.get("failures") or "coursework screenshots are invalid")})
    return ("success" if not failures else "failed", failures)


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


def _indexed_entity_search(entity_type: str, expected_urn: str) -> dict:
    try:
        query = expected_urn.rsplit(",", 1)[-1].rstrip(")") if entity_type == "DATASET" else expected_urn.rsplit(",", 1)[-1].rstrip(")")
        if entity_type == "DATA_FLOW":
            query = DATAFLOW_ID
        elif entity_type == "DATA_JOB":
            query = expected_urn.rsplit(",", 1)[-1].rstrip(")")
        elif entity_type == "ASSERTION":
            query = expected_urn.rsplit(":", 1)[-1]
        payload = _graphql(
            GRAPHQL_SEARCH_QUERY,
            {"input": {"type": entity_type, "query": query, "start": 0, "count": 100}},
        )
        if payload.get("errors"):
            raise RuntimeError(str(payload["errors"]))
        search = payload.get("data", {}).get("search", {})
        found_urns = [
            item.get("entity", {}).get("urn")
            for item in search.get("searchResults", [])
            if item.get("entity", {}).get("urn")
        ]
        return {
            "status": "success" if expected_urn in found_urns else "failed",
            "expected_urn": expected_urn,
            "found_urns": found_urns,
            "total": search.get("total", 0),
            "error": None if expected_urn in found_urns else f"expected URN was not indexed: {expected_urn}",
        }
    except Exception as exc:
        return {"status": "failed", "expected_urn": expected_urn, "error": str(exc)}


def _capture_coursework_job(job: dict[str, object]) -> dict:
    job_urn = str(job["urn"])
    try:
        payload = _graphql(GRAPHQL_DATAJOB_QUERY, {"urn": job_urn})
        if payload.get("errors"):
            raise RuntimeError(str(payload["errors"]))
        data_job = payload.get("data", {}).get("dataJob")
        if data_job is None:
            raise RuntimeError("DataJob was not found")
        input_output = data_job.get("inputOutput") or {}
        actual_inputs = sorted(item["urn"] for item in input_output.get("inputDatasets") or [])
        actual_outputs = sorted(item["urn"] for item in input_output.get("outputDatasets") or [])
        expected_inputs = sorted(job["inputs"])
        expected_outputs = sorted(job["outputs"])
        flow_urn = (data_job.get("dataFlow") or {}).get("urn")
        return {
            "status": "success" if flow_urn == COURSEWORK_DATAFLOW_URN and actual_inputs == expected_inputs and actual_outputs == expected_outputs else "failed",
            "urn": job_urn,
            "dataflow_urn": flow_urn,
            "expected_inputs": expected_inputs,
            "actual_inputs": actual_inputs,
            "expected_outputs": expected_outputs,
            "actual_outputs": actual_outputs,
            "error": None if flow_urn == COURSEWORK_DATAFLOW_URN and actual_inputs == expected_inputs and actual_outputs == expected_outputs else "DataJob flow or I/O sets differ from the contract",
        }
    except Exception as exc:
        return {"status": "failed", "urn": job_urn, "error": str(exc)}


def _capture_dataset_schema(dataset_urn: str) -> dict:
    try:
        payload = _graphql(GRAPHQL_DATASET_SCHEMA_QUERY, {"urn": dataset_urn})
        if payload.get("errors"):
            raise RuntimeError(str(payload["errors"]))
        dataset = payload.get("data", {}).get("dataset")
        fields = (dataset or {}).get("schemaMetadata", {}).get("fields") or []
        return {"status": "success" if dataset and fields else "failed", "urn": dataset_urn, "fields": fields, "error": None if dataset and fields else "dataset schema is missing"}
    except Exception as exc:
        return {"status": "failed", "urn": dataset_urn, "fields": [], "error": str(exc)}


def _capture_assertion(assertion_id: str) -> dict:
    assertion_urn = f"urn:li:assertion:{assertion_id}"
    try:
        payload = _graphql(GRAPHQL_ASSERTION_QUERY, {"urn": assertion_urn})
        if payload.get("errors"):
            raise RuntimeError(str(payload["errors"]))
        assertion = payload.get("data", {}).get("assertion")
        run_events = ((assertion or {}).get("runEvents") or {}).get("runEvents") or []
        latest_event = run_events[0] if run_events else {}
        result_type = (latest_event.get("result") or {}).get("type")
        assertee_urn = latest_event.get("asserteeUrn") or (((assertion or {}).get("info") or {}).get("datasetAssertion") or {}).get("datasetUrn")
        return {
            "status": "success" if assertion and result_type == "SUCCESS" and assertee_urn else "failed",
            "urn": assertion_urn,
            "assertee_urn": assertee_urn,
            "result": result_type,
            "error": None if assertion and result_type == "SUCCESS" and assertee_urn else "assertion is missing, unlinked, or not passing",
        }
    except Exception as exc:
        return {"status": "failed", "urn": assertion_urn, "error": str(exc)}


def _expected_assertion_datasets(job_id: str, target: dict[str, object]) -> set[str]:
    if job_id == "dp1_raw_to_bronze":
        return {s3_urn("bronze.batch"), s3_urn("bronze.events")}
    if job_id == "dp3_offline_features":
        return {ice_urn(table_name) for table_name in FEATURE_TABLES}
    return {str(target["representative_output"])}


def capture_coursework_pipeline_evidence() -> dict:
    entities = coursework_pipeline_entities()
    data_flow = entities["data_flow"]
    data_jobs = entities["data_jobs"]
    assert isinstance(data_flow, dict) and isinstance(data_jobs, list)
    try:
        flow_payload = _graphql(GRAPHQL_DATAFLOW_QUERY, {"urn": COURSEWORK_DATAFLOW_URN})
        if flow_payload.get("errors"):
            raise RuntimeError(str(flow_payload["errors"]))
        flow = flow_payload.get("data", {}).get("dataFlow")
        dataflow = {"status": "success" if flow and flow.get("urn") == COURSEWORK_DATAFLOW_URN else "failed", "expected_urn": COURSEWORK_DATAFLOW_URN, "dataflow": flow}
    except Exception as exc:
        dataflow = {"status": "failed", "expected_urn": COURSEWORK_DATAFLOW_URN, "error": str(exc)}

    indexed_search = {
        "dataflow": _indexed_entity_search("DATA_FLOW", COURSEWORK_DATAFLOW_URN),
        "datajobs": {str(job["id"]): _indexed_entity_search("DATA_JOB", str(job["urn"])) for job in data_jobs},
    }
    indexed_search["status"] = "success" if indexed_search["dataflow"]["status"] == "success" and all(item["status"] == "success" for item in indexed_search["datajobs"].values()) else "failed"
    indexed_search["error"] = None if indexed_search["status"] == "success" else "DataFlow or one or more DataJobs are absent from indexed search"

    jobs = {str(job["id"]): _capture_coursework_job(job) for job in data_jobs}
    verifications: dict[str, dict] = {}
    for job_id, target in ASSERTION_TARGETS.items():
        schema_urns = [str(target["representative_output"])]
        if job_id == "dp3_offline_features":
            schema_urns = [ice_urn(table_name) for table_name in FEATURE_TABLES]
        schemas = {urn: _capture_dataset_schema(urn) for urn in schema_urns}
        required = set(target["required_schema_fields"])
        forbidden = set(target["forbidden_schema_fields"])
        schema_ok = all(
            schema["status"] == "success"
            and required.issubset({field.get("fieldPath") for field in schema.get("fields", [])})
            and not forbidden.intersection({field.get("fieldPath") for field in schema.get("fields", [])})
            for schema in schemas.values()
        )
        assertions = {assertion_id: _capture_assertion(assertion_id) for assertion_id in target["assertions"]}
        expected_assertion_datasets = _expected_assertion_datasets(job_id, target)
        assertion_ok = all(assertion["status"] == "success" and assertion.get("assertee_urn") in expected_assertion_datasets for assertion in assertions.values())
        verifications[job_id] = {
            "status": "success" if jobs[job_id]["status"] == "success" and schema_ok and assertion_ok else "failed",
            "job": jobs[job_id],
            "schemas": schemas,
            "assertions": assertions,
            "schema_ok": schema_ok,
            "assertion_ok": assertion_ok,
        }

    status = "success" if dataflow["status"] == "success" and indexed_search["status"] == "success" and all(item["status"] == "success" for item in verifications.values()) else "failed"
    payload = {"status": status, "dataflow": dataflow, "indexed_search": indexed_search, "verifications": verifications}
    _write_json(COURSEWORK_PIPELINE_EVIDENCE_ROOT / "coursework_pipeline_entities.json", {"dataflow": dataflow, "indexed_search": indexed_search, "datajobs": jobs})
    for job_id, verification in verifications.items():
        _write_json(COURSEWORK_PIPELINE_EVIDENCE_ROOT / f"{job_id.replace('_raw_to_bronze', '').replace('_bronze_to_silver_gold', '').replace('_offline_features', '')}_verification.json", verification)
    return payload


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

    coursework = capture_coursework_pipeline_evidence()
    screenshots = validate_coursework_screenshot_manifest()
    coursework_status, coursework_failures = coursework_gate_status(coursework, screenshots)
    _write_json(
        COURSEWORK_PIPELINE_EVIDENCE_ROOT / "run_manifest.json",
        {
            "captured_at": _utc_now(),
            "status": coursework_status,
            "machine_evidence": coursework,
            "screenshots": screenshots,
            "failures": coursework_failures,
        },
    )

    status, failures = _manifest_status(health, datasets, tags, search)
    failures.extend(coursework_failures)
    if coursework_status == "failed":
        status = "failed"
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
            "coursework_pipeline/run_manifest.json",
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
