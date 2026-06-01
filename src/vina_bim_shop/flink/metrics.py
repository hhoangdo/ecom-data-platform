from __future__ import annotations

from collections.abc import Iterable
from datetime import datetime, timezone
from hashlib import sha1
from typing import Any


DIMENSION_FIELDS = (
    "primary_category",
    "source",
    "device_type",
    "payment_method",
    "order_status",
    "schema_version",
)


def dedupe_events(events: Iterable[dict[str, Any]]) -> tuple[list[dict[str, Any]], int]:
    deduped: dict[str, dict[str, Any]] = {}
    duplicate_count = 0
    for event in events:
        event_id = str(event["event_id"])
        if event_id in deduped:
            duplicate_count += 1
            continue
        deduped[event_id] = event
    return list(deduped.values()), duplicate_count


def build_metric_snapshot(
    *,
    events: Iterable[dict[str, Any]],
    window_start_ts: str,
    window_end_ts: str,
    watermark_ts: str,
    allowed_lateness_seconds: int = 300,
    correction_version: int = 0,
) -> dict[str, Any]:
    deduped_events, duplicate_count = dedupe_events(events)
    dimension_seed = _dimension_values(deduped_events[0]) if deduped_events else _empty_dimensions()
    metric_key = _metric_key(window_start_ts, dimension_seed)
    late_event_count = sum(
        1
        for event in deduped_events
        if _is_late_event(
            created_ts=str(event["created_ts"]),
            window_end_ts=window_end_ts,
            allowed_lateness_seconds=allowed_lateness_seconds,
        )
    )

    checkout_started_count = sum(1 for event in deduped_events if event["event_type"] == "checkout_started")
    order_placed_count = sum(1 for event in deduped_events if event["event_type"] == "order_placed")
    payment_failure_count = sum(1 for event in deduped_events if event["event_type"] == "payment_failed")
    revenue_amount = round(
        sum(float(event.get("payload", {}).get("amount") or 0.0) for event in deduped_events if event["event_type"] == "payment_succeeded"),
        2,
    )
    gmv_proxy_amount = round(
        sum(float(event.get("payload", {}).get("order_net_amount") or 0.0) for event in deduped_events if event["event_type"] == "order_placed"),
        2,
    )
    checkout_conversion_rate = round(order_placed_count / checkout_started_count, 6) if checkout_started_count else 0.0

    return {
        "metric_key": metric_key,
        "metric_minute": window_start_ts,
        "window_start_ts": window_start_ts,
        "window_end_ts": window_end_ts,
        **dimension_seed,
        "order_count": order_placed_count,
        "order_placed_count": order_placed_count,
        "checkout_started_count": checkout_started_count,
        "payment_failure_count": payment_failure_count,
        "revenue_amount": revenue_amount,
        "gmv_proxy_amount": gmv_proxy_amount,
        "checkout_conversion_rate": checkout_conversion_rate,
        "late_event_count": late_event_count,
        "duplicate_event_count": duplicate_count,
        "correction_version": correction_version,
        "emitted_ts": _normalize_iso(datetime.now(timezone.utc)),
        "watermark_ts": watermark_ts,
    }


def _metric_key(window_start_ts: str, dimensions: dict[str, Any]) -> str:
    dimension_key = "|".join(str(dimensions[field]) for field in DIMENSION_FIELDS)
    digest = sha1(f"{window_start_ts}|{dimension_key}".encode("utf-8")).hexdigest()[:12]
    return f"commerce|{window_start_ts}|{digest}"


def _dimension_values(event: dict[str, Any]) -> dict[str, Any]:
    payload = dict(event.get("payload", {}))
    return {
        "primary_category": payload.get("primary_category"),
        "source": payload.get("source"),
        "device_type": payload.get("device_type"),
        "payment_method": payload.get("payment_method"),
        "order_status": payload.get("order_status"),
        "schema_version": int(event.get("schema_version", 0)),
    }


def _empty_dimensions() -> dict[str, Any]:
    return {
        "primary_category": None,
        "source": None,
        "device_type": None,
        "payment_method": None,
        "order_status": None,
        "schema_version": 0,
    }


def _is_late_event(*, created_ts: str, window_end_ts: str, allowed_lateness_seconds: int) -> bool:
    created = _parse_ts(created_ts)
    window_end = _parse_ts(window_end_ts)
    return (created - window_end).total_seconds() > allowed_lateness_seconds


def _parse_ts(value: str) -> datetime:
    normalized = value.replace("Z", "+00:00")
    parsed = datetime.fromisoformat(normalized)
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _normalize_iso(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat()
