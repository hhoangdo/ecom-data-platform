from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable

import pandas as pd

from vina_bim_shop.kafka.publisher import publish_topic_events as _publish_topic_events


PublishTopicEvents = Callable[..., dict[str, int]]


def run_streaming_smoke_publish(
    *,
    bootstrap_servers: str = "localhost:9092",
    evidence_root: str | Path = "evidence/06_flink_streaming",
    publish_topic_events: PublishTopicEvents = _publish_topic_events,
) -> dict[str, Any]:
    topic_events = build_streaming_smoke_topic_events()
    published_counts = publish_topic_events(
        topic_events=topic_events,
        bootstrap_servers=bootstrap_servers,
    )
    summary = {
        "kafka_bootstrap_servers": bootstrap_servers,
        "published_counts": published_counts,
        "expected_outputs": {
            "commerce_metrics_topic": "realtime_commerce_metrics_1m",
            "ops_alerts_topic": "realtime_ops_alerts",
            "metric_corrections_topic": "realtime_metric_corrections",
        },
    }
    evidence_path = Path(evidence_root)
    evidence_path.mkdir(parents=True, exist_ok=True)
    (evidence_path / "flink_smoke_publish_summary.json").write_text(json.dumps(summary, indent=2, sort_keys=True), encoding="utf-8")
    return summary


def build_streaming_smoke_topic_events(*, base: datetime | None = None) -> dict[str, pd.DataFrame]:
    base_time = base or datetime(2026, 5, 1, 10, 0, 0, tzinfo=timezone.utc)
    return {
        "commerce_events": pd.DataFrame(_commerce_events(base_time)),
        "catalog_events": pd.DataFrame(_catalog_events(base_time)),
        "fulfillment_events": pd.DataFrame(_fulfillment_events(base_time)),
        "ops_events": pd.DataFrame(_ops_events(base_time)),
    }


def build_cleanroom_smoke_phases(*, base: datetime | None = None) -> list[dict[str, Any]]:
    topic_events = build_streaming_smoke_topic_events(base=base)
    commerce_records = topic_events["commerce_events"].to_dict("records")
    initial_records = [record for record in commerce_records if record["event_id"] != "evt-8"]
    late_records = [record for record in commerce_records if record["event_id"] == "evt-8"]

    return [
        {
            "name": "initial_business_events",
            "topic_events": {
                "commerce_events": pd.DataFrame(initial_records),
                "catalog_events": topic_events["catalog_events"],
                "fulfillment_events": topic_events["fulfillment_events"],
                "ops_events": topic_events["ops_events"],
            },
        },
        {
            "name": "watermark_control_event",
            "topic_events": {
                "commerce_events": pd.DataFrame([_control_commerce_event(base or datetime(2026, 5, 1, 10, 0, 0, tzinfo=timezone.utc))]),
            },
        },
        {
            "name": "late_correction_event",
            "topic_events": {
                "commerce_events": pd.DataFrame(late_records),
            },
        },
    ]


def _commerce_events(base: datetime) -> list[dict[str, Any]]:
    late_base = base + timedelta(minutes=6)
    duplicate_payload = _commerce_payload(order_status="paid", payment_method="wallet", order_net_amount=125000.0, amount=125000.0)
    duplicate = _commerce_envelope("evt-3", "payment_succeeded", base + timedelta(seconds=20), base + timedelta(seconds=25), duplicate_payload)
    return [
        _commerce_envelope("evt-1", "checkout_started", base + timedelta(seconds=5), base + timedelta(seconds=5), _commerce_payload(order_status="checkout_started", payment_method=None)),
        _commerce_envelope("evt-2", "order_placed", base + timedelta(seconds=15), base + timedelta(seconds=15), _commerce_payload()),
        duplicate,
        dict(duplicate, created_ts=(base + timedelta(seconds=26)).replace(tzinfo=None).isoformat()),
        _commerce_envelope("evt-4", "payment_failed", base + timedelta(seconds=30), base + timedelta(seconds=30), _commerce_payload(order_status="payment_failed", payment_method="wallet", amount=0.0)),
        _commerce_envelope("evt-5", "payment_failed", base + timedelta(seconds=31), base + timedelta(seconds=31), _commerce_payload(order_status="payment_failed", payment_method="wallet", amount=0.0)),
        _commerce_envelope("evt-6", "payment_failed", base + timedelta(seconds=32), base + timedelta(seconds=32), _commerce_payload(order_status="payment_failed", payment_method="wallet", amount=0.0)),
        _commerce_envelope("evt-7", "payment_failed", base + timedelta(seconds=33), base + timedelta(seconds=33), _commerce_payload(order_status="payment_failed", payment_method="wallet", amount=0.0)),
        _commerce_envelope("evt-8", "order_placed", base + timedelta(seconds=40), late_base + timedelta(seconds=10), _commerce_payload(order_net_amount=30000.0)),
    ]


def _control_commerce_event(base: datetime) -> dict[str, Any]:
    control_event_ts = base + timedelta(minutes=1, seconds=10)
    return _commerce_envelope(
        "ctrl-1",
        "checkout_started",
        control_event_ts,
        control_event_ts,
        _commerce_payload(
            primary_category="CONTROL",
            source="cleanroom",
            device_type="control_device",
            payment_method=None,
            order_status="checkout_started",
            order_net_amount=0.0,
            amount=0.0,
        ),
    )


def _catalog_events(base: datetime) -> list[dict[str, Any]]:
    return [
        {
            "event_id": "cat-1",
            "event_type": "inventory_low_stock",
            "event_topic": "catalog_events",
            "schema_version": 1,
            "event_timestamp": (base + timedelta(minutes=1)).replace(tzinfo=None).isoformat(),
            "created_ts": (base + timedelta(minutes=1)).replace(tzinfo=None).isoformat(),
            "producer": "vina_bim_shop.synthetic_source",
            "correlation_ids": {"product_id": "PRD-1"},
            "payload": {"primary_category": "FMCG", "inventory_level": 2},
        }
    ]


def _fulfillment_events(base: datetime) -> list[dict[str, Any]]:
    return [
        {
            "event_id": "ful-1",
            "event_type": "shipment_delayed",
            "event_topic": "fulfillment_events",
            "schema_version": 1,
            "event_timestamp": (base + timedelta(minutes=2)).replace(tzinfo=None).isoformat(),
            "created_ts": (base + timedelta(minutes=2)).replace(tzinfo=None).isoformat(),
            "producer": "vina_bim_shop.synthetic_source",
            "correlation_ids": {"shipment_id": "SHP-1", "order_id": "ORD-1"},
            "payload": {"shipping_city": "Ho Chi Minh City", "shipping_method": "standard"},
        },
        {
            "event_id": "ful-2",
            "event_type": "shipment_blocked_payment_failed",
            "event_topic": "fulfillment_events",
            "schema_version": 1,
            "event_timestamp": (base + timedelta(minutes=2, seconds=10)).replace(tzinfo=None).isoformat(),
            "created_ts": (base + timedelta(minutes=2, seconds=10)).replace(tzinfo=None).isoformat(),
            "producer": "vina_bim_shop.synthetic_source",
            "correlation_ids": {"shipment_id": "SHP-2", "order_id": "ORD-2"},
            "payload": {"block_reason": "payment_failed", "shipment_status": "blocked_payment_failed"},
        },
    ]


def _ops_events(base: datetime) -> list[dict[str, Any]]:
    return [
        {
            "event_id": "ops-1",
            "event_type": "traffic_burst_detected",
            "event_topic": "ops_events",
            "schema_version": 1,
            "event_timestamp": (base + timedelta(minutes=3)).replace(tzinfo=None).isoformat(),
            "created_ts": (base + timedelta(minutes=3)).replace(tzinfo=None).isoformat(),
            "producer": "vina_bim_shop.synthetic_source",
            "correlation_ids": {},
            "payload": {"burst_event_count": 120, "burst_windows": ["12:00-12:20"]},
        },
        {
            "event_id": "ops-2",
            "event_type": "late_arrival_observed",
            "event_topic": "ops_events",
            "schema_version": 1,
            "event_timestamp": (base + timedelta(minutes=3, seconds=10)).replace(tzinfo=None).isoformat(),
            "created_ts": (base + timedelta(minutes=3, seconds=10)).replace(tzinfo=None).isoformat(),
            "producer": "vina_bim_shop.synthetic_source",
            "correlation_ids": {},
            "payload": {"late_event_count": 1, "allowed_lateness_seconds": 300},
        },
        {
            "event_id": "ops-3",
            "event_type": "duplicate_event_observed",
            "event_topic": "ops_events",
            "schema_version": 1,
            "event_timestamp": (base + timedelta(minutes=3, seconds=20)).replace(tzinfo=None).isoformat(),
            "created_ts": (base + timedelta(minutes=3, seconds=20)).replace(tzinfo=None).isoformat(),
            "producer": "vina_bim_shop.synthetic_source",
            "correlation_ids": {},
            "payload": {"duplicate_event_count": 1, "dedup_key": ["event_id", "created_ts"]},
        },
    ]


def _commerce_envelope(event_id: str, event_type: str, event_timestamp: datetime, created_ts: datetime, payload: dict[str, Any]) -> dict[str, Any]:
    return {
        "event_id": event_id,
        "event_type": event_type,
        "event_topic": "commerce_events",
        "schema_version": 1,
        "event_timestamp": event_timestamp.replace(tzinfo=None).isoformat(),
        "created_ts": created_ts.replace(tzinfo=None).isoformat(),
        "producer": "vina_bim_shop.synthetic_source",
        "correlation_ids": {"order_id": "ORD-1", "customer_id": "CUS-1", "session_id": "SES-1"},
        "payload": payload,
    }


def _commerce_payload(
    *,
    primary_category: str = "FMCG",
    source: str = "app",
    device_type: str = "app_android",
    payment_method: str | None = "wallet",
    order_status: str = "paid",
    order_net_amount: float = 125000.0,
    amount: float = 125000.0,
) -> dict[str, Any]:
    return {
        "primary_category": primary_category,
        "source": source,
        "device_type": device_type,
        "payment_method": payment_method,
        "order_status": order_status,
        "order_net_amount": order_net_amount,
        "amount": amount,
    }
