from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from ._constants import DERIVED_TOPICS, EXPECTED_ADR04_COUNTS
from ._probes import _has_checkpoint_metadata


def build_pre_publish_state(
    *,
    topic_counts: dict[str, int],
    checkpoint_listing: str,
    curated_listings: dict[str, str],
    running_jobs: list[str],
) -> dict[str, Any]:
    failures: list[str] = []
    for topic in DERIVED_TOPICS:
        if int(topic_counts.get(topic, 0)) != 0:
            failures.append(f"{topic} must be empty before publish.")
    if checkpoint_listing.strip():
        failures.append("checkpoints/flink/ must be empty before publish.")
    if curated_listings.get("realtime_metric_corrections", "").strip():
        failures.append("evidence/streaming_curated/realtime_metric_corrections must be empty before publish.")
    if curated_listings.get("realtime_ops_alerts", "").strip():
        failures.append("evidence/streaming_curated/realtime_ops_alerts must be empty before publish.")
    if running_jobs:
        failures.append("No ADR 04 Flink jobs should be running before clean-room replay.")
    return {
        "captured_at": datetime.now(timezone.utc).isoformat(),
        "is_clean": not failures,
        "topic_counts": topic_counts,
        "checkpoint_listing": checkpoint_listing,
        "curated_listings": curated_listings,
        "running_jobs": running_jobs,
        "failures": failures,
    }


def evaluate_adr04_assertions(
    *,
    topic_counts: dict[str, int],
    metric_rows: list[dict[str, Any]],
    correction_rows: list[dict[str, Any]],
    checkpoint_listing: str,
    curated_listings: dict[str, str],
) -> dict[str, Any]:
    for topic, expected_count in EXPECTED_ADR04_COUNTS.items():
        actual_count = int(topic_counts.get(topic, 0))
        if actual_count != expected_count:
            if topic == "realtime_metric_corrections":
                raise ValueError(f"Expected exactly 1 correction row in {topic}, found {actual_count}.")
            raise ValueError(f"Expected {expected_count} rows in {topic}, found {actual_count}.")

    if len(correction_rows) != 1:
        raise ValueError(f"Expected exactly 1 correction row in realtime_metric_corrections, found {len(correction_rows)}.")

    correction = correction_rows[0]
    if correction.get("correction_reason") != "late_event":
        raise ValueError("Correction row must use correction_reason=late_event.")
    if int(correction.get("correction_version", -1)) != 1:
        raise ValueError("Correction row must use correction_version=1.")
    if correction.get("target_topic") != "realtime_commerce_metrics_1m":
        raise ValueError("Correction row must target realtime_commerce_metrics_1m.")
    if not str(correction.get("dimension_hash", "")).strip():
        raise ValueError("Correction row must include a non-empty dimension_hash.")

    snapshot = dict(correction.get("metric_snapshot", {}))
    metric_keys = {str(row.get("metric_key")) for row in metric_rows}
    if str(snapshot.get("metric_key")) not in metric_keys:
        raise ValueError("Correction metric_snapshot.metric_key must match one initial realtime_commerce_metrics_1m row.")

    expected_snapshot = {
        "window_start_ts": "2026-05-01T10:00:00+00:00",
        "order_count": 2,
        "order_placed_count": 2,
        "revenue_amount": 125000.0,
        "gmv_proxy_amount": 155000.0,
        "duplicate_event_count": 1,
    }
    for field, expected_value in expected_snapshot.items():
        if snapshot.get(field) != expected_value:
            raise ValueError(f"Correction metric_snapshot.{field} must equal {expected_value!r}.")

    if not _has_checkpoint_metadata(checkpoint_listing, job_prefix="commerce_metrics"):
        raise ValueError("At least one checkpoint object must exist under checkpoints/flink/commerce_metrics/.")
    if not curated_listings.get("realtime_metric_corrections", "").strip():
        raise ValueError("Curated realtime_metric_corrections JSONL output must exist in MinIO.")
    if not curated_listings.get("realtime_ops_alerts", "").strip():
        raise ValueError("Curated realtime_ops_alerts JSONL output must exist in MinIO.")

    return {
        "captured_at": datetime.now(timezone.utc).isoformat(),
        "passed": True,
        "topic_counts": topic_counts,
        "correction_row": correction,
    }


def evaluate_pinot_gate(
    *,
    controller_health: dict[str, Any],
    broker_health: dict[str, Any],
    row_counts: dict[str, int],
) -> dict[str, Any]:
    if not _health_is_good(controller_health):
        raise ValueError("Pinot controller health is not GOOD.")
    if not _health_is_good(broker_health):
        raise ValueError("Pinot broker health is not GOOD.")
    for table_name in [
        "pinot_realtime_commerce_metrics_1m",
        "pinot_realtime_metric_corrections",
        "pinot_realtime_ops_alerts",
    ]:
        if int(row_counts.get(table_name, 0)) <= 0:
            raise ValueError(f"{table_name} must contain live rows after Pinot bootstrap.")
    return {
        "captured_at": datetime.now(timezone.utc).isoformat(),
        "passed": True,
        "row_counts": row_counts,
    }


def _health_is_good(payload: dict[str, Any]) -> bool:
    status_candidates = [
        payload.get("status", ""),
        payload.get("text", ""),
        payload.get("body", ""),
    ]
    return any(str(candidate).strip().upper() in {"GOOD", "HEALTHY", "OK"} for candidate in status_candidates)
