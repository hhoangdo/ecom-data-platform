from __future__ import annotations

import argparse
import json
import re
import time
from datetime import datetime, timezone
from pathlib import Path

import requests


REPO_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_EVIDENCE_ROOT = REPO_ROOT / "evidence" / "09_datahub_governance" / "runtime_recovery"
DEFAULT_GMS_URL = "http://localhost:8087"
DEFAULT_ELASTICSEARCH_URL = "http://localhost:9200"
RESTORE_PATH = "/operations?action=restoreIndices"
ROWS_MIGRATED_PATTERN = re.compile(r"\browsMigrated=(\d+)")
LAST_URN_PATTERN = re.compile(r"\blastUrn=([^,)}]+)")
SEARCH_RETRY_ATTEMPTS = 180
SEARCH_RETRY_INTERVAL_SECONDS = 2

REPRESENTATIVE_DATASET_URNS = {
    "iceberg_fact_order": "urn:li:dataset:(urn:li:dataPlatform:iceberg,vina_bim_shop.fact_order,PROD)",
    "kafka_commerce_events": "urn:li:dataset:(urn:li:dataPlatform:kafka,commerce_events,PROD)",
    "pinot_realtime_commerce_metrics_1m": "urn:li:dataset:(urn:li:dataPlatform:pinot,realtime_commerce_metrics_1m,PROD)",
    "s3_spark_event_log_prefix": "urn:li:dataset:(urn:li:dataPlatform:s3,checkpoints.spark-events,PROD)",
}

SEARCH_DATASETS_QUERY = """
query SearchDatasets($input: SearchInput!) {
  search(input: $input) {
    start
    count
    total
    searchResults {
      entity {
        urn
        ... on Dataset {
          name
          platform {
            name
          }
        }
      }
    }
  }
}
""".strip()


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _write_json(path: Path, payload: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")


def _restored_rows(payload: dict[str, object]) -> int:
    for key in ("rowsMigrated", "rowsRestored", "restoredRows", "restored"):
        value = payload.get(key)
        if isinstance(value, int) and not isinstance(value, bool):
            return value

    value = payload.get("value")
    if isinstance(value, int) and not isinstance(value, bool):
        return value
    if isinstance(value, dict):
        return _restored_rows(value)
    if isinstance(value, str):
        match = ROWS_MIGRATED_PATTERN.search(value)
        if match:
            return int(match.group(1))

    raise RuntimeError(f"restore response did not include a numeric restored-row count: {payload}")


def _response_cursor(payload: dict[str, object]) -> str | None:
    for key in ("lastUrn", "nextCursor", "nextStart"):
        value = payload.get(key)
        if value is not None:
            return f"{key}:{value}"
    value = payload.get("value")
    if isinstance(value, str):
        match = LAST_URN_PATTERN.search(value)
        if match:
            return f"lastUrn:{match.group(1)}"
    return None


def _dataset_search_term(urn: str) -> str:
    return urn.split(",")[1]


def capture_search_evidence(gms_url: str) -> dict[str, object]:
    endpoint = f"{gms_url.rstrip('/')}/api/graphql"
    results: dict[str, object] = {}
    failures: list[dict[str, str]] = []

    for label, urn in REPRESENTATIVE_DATASET_URNS.items():
        try:
            response = requests.post(
                endpoint,
                json={
                    "query": SEARCH_DATASETS_QUERY,
                    "variables": {
                        "input": {
                            "type": "DATASET",
                            "query": _dataset_search_term(urn),
                            "start": 0,
                            "count": 100,
                        }
                    },
                },
                timeout=30,
                headers={"Content-Type": "application/json"},
            )
            response.raise_for_status()
            payload = response.json()
            if payload.get("errors"):
                raise RuntimeError(str(payload["errors"]))
            search = payload.get("data", {}).get("search", {})
            found_urns = [
                item.get("entity", {}).get("urn")
                for item in search.get("searchResults", [])
                if item.get("entity", {}).get("urn")
            ]
            results[label] = {
                "expected_urn": urn,
                "found_urns": found_urns,
                "total": search.get("total", 0),
            }
            if urn not in found_urns:
                failures.append({"label": label, "error": f"expected URN was not indexed: {urn}"})
        except Exception as exc:
            results[label] = {"expected_urn": urn, "error": str(exc)}
            failures.append({"label": label, "error": str(exc)})

    return {
        "status": "success" if not failures else "failed",
        "gms_url": gms_url,
        "results": results,
        "failures": failures,
    }


def wait_for_indexed_search(gms_url: str) -> dict[str, object]:
    search: dict[str, object] = {}
    for attempt in range(1, SEARCH_RETRY_ATTEMPTS + 1):
        search = capture_search_evidence(gms_url)
        search["attempt"] = attempt
        if search["status"] == "success":
            return search
        if attempt < SEARCH_RETRY_ATTEMPTS:
            time.sleep(SEARCH_RETRY_INTERVAL_SECONDS)
    return search


def capture_elasticsearch_indices(elasticsearch_url: str) -> dict[str, object]:
    try:
        health_response = requests.get(f"{elasticsearch_url.rstrip('/')}/_cluster/health", timeout=20)
        health_response.raise_for_status()
        health = health_response.json()
        indices_response = requests.get(
            f"{elasticsearch_url.rstrip('/')}/_cat/indices?format=json&bytes=b",
            timeout=20,
        )
        indices_response.raise_for_status()
        indices = indices_response.json()
        populated_indices = [
            index
            for index in indices
            if not str(index.get("index", "")).startswith(".") and int(index.get("docs.count", 0)) > 0
        ]
        status = str(health.get("status", ""))
        if status not in {"yellow", "green"}:
            raise RuntimeError(f"Elasticsearch health is {status or 'missing'}")
        if not populated_indices:
            raise RuntimeError("Elasticsearch has no populated DataHub indices")
        return {"status": "success", "health": health, "indices": indices, "populated_indices": populated_indices}
    except Exception as exc:
        return {"status": "failed", "error": str(exc), "elasticsearch_url": elasticsearch_url}


def _load_source_metadata_count(evidence_root: Path) -> int:
    preflight = json.loads((evidence_root / "preflight.json").read_text(encoding="utf-8"))
    value = preflight["postgres"]["metadata_aspect_v2_count"]
    if not isinstance(value, int):
        raise RuntimeError("preflight metadata aspect count must be an integer")
    return value


def restore_indices(
    gms_url: str,
    urn_like: str,
    batch_size: int,
    evidence_root: Path,
    *,
    source_metadata_count: int | None = None,
) -> dict[str, object]:
    if batch_size <= 0:
        raise ValueError("batch_size must be positive")

    source_metadata_count = source_metadata_count if source_metadata_count is not None else _load_source_metadata_count(evidence_root)
    endpoint = f"{gms_url.rstrip('/')}{RESTORE_PATH}"
    result: dict[str, object] = {
        "captured_at": _utc_now(),
        "endpoint": endpoint,
        "urn_like": urn_like,
        "batch_size": batch_size,
        "source_metadata_count": source_metadata_count,
        "requests": [],
        "responses": [],
        "restored_rows": 0,
    }
    seen_cursors: set[str] = set()
    start = 0

    try:
        while True:
            request_payload = {"urnLike": urn_like, "start": start, "batchSize": batch_size}
            response = requests.post(
                endpoint,
                json=request_payload,
                timeout=60,
                headers={"Content-Type": "application/json"},
            )
            response.raise_for_status()
            response_payload = response.json()
            if not isinstance(response_payload, dict):
                raise RuntimeError(f"restore response must be an object: {response_payload}")

            result["requests"].append(request_payload)
            result["responses"].append(response_payload)

            restored_rows = _restored_rows(response_payload)
            cursor = _response_cursor(response_payload)
            if restored_rows == batch_size and cursor is not None and cursor in seen_cursors:
                raise RuntimeError(f"repeated restore response cursor: {cursor}")
            if cursor is not None:
                seen_cursors.add(cursor)

            result["restored_rows"] = int(result["restored_rows"]) + restored_rows

            if restored_rows == 0:
                if source_metadata_count > 0:
                    raise RuntimeError("restore operation restored zero rows while PostgreSQL contains metadata")
                break
            if restored_rows < batch_size:
                break
            start += batch_size

        if source_metadata_count > 0 and int(result["restored_rows"]) == 0:
            raise RuntimeError("restore operation restored zero rows while PostgreSQL contains metadata")

        search = wait_for_indexed_search(gms_url)
        result["search"] = search
        if search["status"] != "success":
            raise RuntimeError(f"post-restore indexed search failed: {search.get('failures') or search.get('error')}")
        result["status"] = "success"
        return result
    except Exception as exc:
        result["status"] = "failed"
        result["error"] = str(exc)
        raise
    finally:
        _write_json(evidence_root / "restore_indices.json", result)
        if "search" in result:
            _write_json(evidence_root / "search_results.json", result["search"])


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Restore persisted DataHub metadata into Elasticsearch indices.")
    parser.add_argument("--gms-url", default=DEFAULT_GMS_URL)
    parser.add_argument("--elasticsearch-url", default=DEFAULT_ELASTICSEARCH_URL)
    parser.add_argument("--urn-like", default="urn:li:%")
    parser.add_argument("--batch-size", type=int, default=1000)
    parser.add_argument("--evidence-root", type=Path, default=DEFAULT_EVIDENCE_ROOT)
    parser.add_argument("--source-metadata-count", type=int)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    try:
        result = restore_indices(
            args.gms_url,
            args.urn_like,
            args.batch_size,
            args.evidence_root,
            source_metadata_count=args.source_metadata_count,
        )
        elasticsearch = capture_elasticsearch_indices(args.elasticsearch_url)
        result["elasticsearch"] = elasticsearch
        if elasticsearch["status"] != "success":
            result["status"] = "failed"
            result["error"] = str(elasticsearch["error"])
            raise RuntimeError(result["error"])
        _write_json(args.evidence_root / "restore_indices.json", result)
        _write_json(args.evidence_root / "elasticsearch_indices.json", elasticsearch)
        print(json.dumps(result, indent=2))
    except Exception as exc:
        print(json.dumps({"status": "failed", "error": str(exc)}, indent=2))
        raise SystemExit(1) from exc


if __name__ == "__main__":
    main()
