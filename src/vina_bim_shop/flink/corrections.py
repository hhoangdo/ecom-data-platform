from __future__ import annotations

from datetime import datetime, timezone
from hashlib import sha1
from typing import Any


def next_correction_version(*, previous_version: int) -> int:
    return previous_version + 1


def build_correction_record(
    *,
    target_topic: str,
    metric_snapshot: dict[str, Any],
    correction_version: int,
    correction_reason: str,
    created_ts: str,
) -> dict[str, Any]:
    dimensions = {
        "primary_category": metric_snapshot.get("primary_category"),
        "source": metric_snapshot.get("source"),
        "device_type": metric_snapshot.get("device_type"),
        "payment_method": metric_snapshot.get("payment_method"),
        "order_status": metric_snapshot.get("order_status"),
        "schema_version": metric_snapshot.get("schema_version"),
    }
    dimension_hash = sha1(str(sorted(dimensions.items())).encode("utf-8")).hexdigest()
    return {
        "correction_id": f"{metric_snapshot['metric_key']}|v{correction_version}",
        "target_topic": target_topic,
        "window_start_ts": metric_snapshot["window_start_ts"],
        "window_end_ts": metric_snapshot["window_end_ts"],
        "dimension_hash": dimension_hash,
        "dimensions": dimensions,
        "metric_snapshot": metric_snapshot,
        "correction_version": correction_version,
        "correction_reason": correction_reason,
        "created_ts": _normalize_ts(created_ts),
    }


def _normalize_ts(value: str) -> str:
    normalized = value.replace("Z", "+00:00")
    parsed = datetime.fromisoformat(normalized)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc).isoformat()
