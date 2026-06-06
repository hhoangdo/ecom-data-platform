from __future__ import annotations

from datetime import datetime, timezone
from typing import Any


OPS_ALERT_TYPE_MAP = {
    "traffic_burst_detected": "traffic_burst",
    "late_arrival_observed": "late_arrival",
    "duplicate_event_observed": "duplicate_spike",
    "inventory_low_stock": "inventory_low_stock",
    "shipment_delayed": "shipment_delayed",
    "shipment_blocked_payment_failed": "shipment_blocked_payment_failed",
}


def normalize_source_alert_event(event: dict[str, Any]) -> dict[str, Any]:
    event_type = str(event["event_type"])
    payload = dict(event.get("payload", {}))
    event_time = _normalize_ts(str(event["event_timestamp"]))
    return {
        "alert_id": str(event["event_id"]),
        "alert_type": OPS_ALERT_TYPE_MAP.get(event_type, event_type),
        "source_topic": str(event["event_topic"]),
        "event_time": event_time,
        "window_start_ts": event_time,
        "window_end_ts": event_time,
        "severity": _source_severity(event_type, payload),
        "dimensions": {
            "event_type": event_type,
            "schema_version": int(event.get("schema_version", 0)),
        },
        "metrics": payload,
        "schema_version": int(event.get("schema_version", 0)),
        "emitted_ts": _normalize_ts(str(event.get("created_ts", event["event_timestamp"]))),
    }


def build_payment_failure_alert(
    *,
    snapshot: dict[str, Any],
    count_threshold: int,
    rate_threshold: float,
    emitted_ts: str,
) -> dict[str, Any]:
    failure_count = int(snapshot["payment_failure_count"])
    order_count = int(snapshot["order_count"])
    failure_rate = (failure_count / order_count) if order_count else 0.0
    if failure_count < count_threshold and failure_rate < rate_threshold:
        raise ValueError("Snapshot does not meet payment failure alert threshold.")
    severity = "high" if failure_count >= count_threshold and failure_rate >= rate_threshold else "medium"
    return {
        "alert_id": f"{snapshot['metric_key']}|payment_failure_spike",
        "alert_type": "payment_failure_spike",
        "source_topic": "commerce_events",
        "event_time": snapshot["window_end_ts"],
        "window_start_ts": snapshot["window_start_ts"],
        "window_end_ts": snapshot["window_end_ts"],
        "severity": severity,
        "dimensions": {
            "primary_category": snapshot.get("primary_category"),
            "source": snapshot.get("source"),
            "device_type": snapshot.get("device_type"),
            "payment_method": snapshot.get("payment_method"),
            "order_status": snapshot.get("order_status"),
        },
        "metrics": {
            "payment_failure_count": failure_count,
            "order_count": order_count,
            "payment_failure_rate": round(failure_rate, 6),
        },
        "schema_version": int(snapshot.get("schema_version", 0)),
        "emitted_ts": _normalize_ts(emitted_ts),
    }


def _source_severity(event_type: str, payload: dict[str, Any]) -> str:
    if event_type == "traffic_burst_detected" and int(payload.get("burst_event_count", 0)) >= 100:
        return "high"
    if event_type in {"shipment_delayed", "shipment_blocked_payment_failed", "duplicate_event_observed"}:
        return "high"
    return "medium"


def _normalize_ts(value: str) -> str:
    normalized = value.replace("Z", "+00:00")
    parsed = datetime.fromisoformat(normalized)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc).isoformat()
