from __future__ import annotations

import json
from copy import deepcopy


def _commerce_event(
    *,
    event_id: str = "evt-1",
    event_type: str = "order_placed",
    event_timestamp: str = "2026-05-01T10:00:15",
    created_ts: str = "2026-05-01T10:00:20",
    payload: dict | None = None,
) -> dict:
    return {
        "event_id": event_id,
        "event_type": event_type,
        "event_topic": "commerce_events",
        "schema_version": 1,
        "event_timestamp": event_timestamp,
        "created_ts": created_ts,
        "producer": "vina_bim_shop.synthetic_source",
        "correlation_ids": {"order_id": "ORD-1", "customer_id": "CUS-1"},
        "payload": {
            "primary_category": "FMCG",
            "source": "app",
            "device_type": "app_android",
            "payment_method": "cod",
            "order_status": "paid",
            "order_net_amount": 125000.0,
            "amount": 125000.0,
            **(payload or {}),
        },
    }


def test_build_metric_snapshot_uses_event_fields_and_metric_formulas() -> None:
    from vina_bim_shop.flink.metrics import build_metric_snapshot

    snapshot = build_metric_snapshot(
        events=[
            _commerce_event(event_type="checkout_started", payload={"order_status": "checkout_started", "payment_method": None}),
            _commerce_event(event_id="evt-2", event_type="order_placed"),
            _commerce_event(event_id="evt-3", event_type="payment_succeeded", payload={"amount": 99000.0, "order_net_amount": 125000.0}),
            _commerce_event(event_id="evt-4", event_type="payment_failed", payload={"order_status": "payment_failed", "payment_method": "wallet"}),
        ],
        window_start_ts="2026-05-01T10:00:00+00:00",
        window_end_ts="2026-05-01T10:01:00+00:00",
        watermark_ts="2026-05-01T10:01:05+00:00",
    )

    assert snapshot["metric_minute"] == "2026-05-01T10:00:00+00:00"
    assert snapshot["primary_category"] == "FMCG"
    assert snapshot["source"] == "app"
    assert snapshot["device_type"] == "app_android"
    assert snapshot["schema_version"] == 1
    assert snapshot["checkout_started_count"] == 1
    assert snapshot["order_placed_count"] == 1
    assert snapshot["order_count"] == 1
    assert snapshot["payment_failure_count"] == 1
    assert snapshot["revenue_amount"] == 99000.0
    assert snapshot["gmv_proxy_amount"] == 125000.0
    assert snapshot["checkout_conversion_rate"] == 1.0
    assert snapshot["correction_version"] == 0


def test_dedupe_events_tracks_duplicates_without_double_counting() -> None:
    from vina_bim_shop.flink.metrics import dedupe_events

    original = _commerce_event(event_id="evt-1", event_type="payment_succeeded", payload={"amount": 80000.0, "order_net_amount": 100000.0})
    duplicate = deepcopy(original)
    duplicate["created_ts"] = "2026-05-01T10:00:25"

    deduped, duplicate_count = dedupe_events([original, duplicate])

    assert duplicate_count == 1
    assert len(deduped) == 1
    assert deduped[0]["event_id"] == "evt-1"


def test_late_event_count_uses_created_ts_minus_window_close() -> None:
    from vina_bim_shop.flink.metrics import build_metric_snapshot

    snapshot = build_metric_snapshot(
        events=[
            _commerce_event(event_id="evt-10", event_type="order_placed", created_ts="2026-05-01T10:06:10"),
            _commerce_event(event_id="evt-11", event_type="payment_succeeded", created_ts="2026-05-01T10:00:25", payload={"amount": 50000.0}),
        ],
        window_start_ts="2026-05-01T10:00:00+00:00",
        window_end_ts="2026-05-01T10:01:00+00:00",
        watermark_ts="2026-05-01T10:06:15+00:00",
        allowed_lateness_seconds=300,
    )

    assert snapshot["late_event_count"] == 1


def test_build_correction_record_emits_full_snapshot_with_incremented_version() -> None:
    from vina_bim_shop.flink.corrections import build_correction_record, next_correction_version

    snapshot = {
        "metric_key": "commerce|2026-05-01T10:00:00+00:00|abc123",
        "window_start_ts": "2026-05-01T10:00:00+00:00",
        "window_end_ts": "2026-05-01T10:01:00+00:00",
        "primary_category": "FMCG",
        "source": "app",
        "device_type": "app_android",
        "payment_method": "cod",
        "order_status": "paid",
        "schema_version": 1,
        "order_count": 1,
        "revenue_amount": 99000.0,
        "gmv_proxy_amount": 125000.0,
    }

    version = next_correction_version(previous_version=2)
    record = build_correction_record(
        target_topic="realtime_commerce_metrics_1m",
        metric_snapshot=snapshot,
        correction_version=version,
        correction_reason="late_event",
        created_ts="2026-05-01T10:06:10+00:00",
    )

    assert version == 3
    assert record["target_topic"] == "realtime_commerce_metrics_1m"
    assert record["correction_version"] == 3
    assert record["correction_reason"] == "late_event"
    assert record["dimension_hash"]
    assert record["dimensions"] == {
        "primary_category": "FMCG",
        "source": "app",
        "device_type": "app_android",
        "payment_method": "cod",
        "order_status": "paid",
        "schema_version": 1,
    }
    assert record["metric_snapshot"] == snapshot


def test_event_timestamp_millis_accepts_raw_json_and_parsed_dict() -> None:
    from vina_bim_shop.flink.runtime import event_timestamp_millis

    event = _commerce_event(event_timestamp="2026-05-01T10:00:15")

    parsed_millis = event_timestamp_millis(event)
    raw_millis = event_timestamp_millis(json.dumps(event))

    assert parsed_millis == raw_millis == 1777629615000


def test_normalize_ops_and_payment_failure_alerts_share_one_contract() -> None:
    from vina_bim_shop.flink.alerts import (
        build_payment_failure_alert,
        normalize_source_alert_event,
    )

    ops_alert = normalize_source_alert_event(
        {
            "event_id": "ops-1",
            "event_type": "traffic_burst_detected",
            "event_topic": "ops_events",
            "schema_version": 1,
            "event_timestamp": "2026-05-01T12:05:00",
            "created_ts": "2026-05-01T12:05:00",
            "producer": "vina_bim_shop.synthetic_source",
            "correlation_ids": {},
            "payload": {"burst_event_count": 120, "burst_windows": ["12:00-12:20"]},
        }
    )
    payment_alert = build_payment_failure_alert(
        snapshot={
            "metric_key": "m-1",
            "window_start_ts": "2026-05-01T10:00:00+00:00",
            "window_end_ts": "2026-05-01T10:01:00+00:00",
            "payment_failure_count": 4,
            "order_count": 20,
            "primary_category": "FMCG",
            "source": "app",
            "device_type": "app_android",
            "payment_method": "wallet",
            "order_status": "payment_failed",
            "schema_version": 1,
        },
        count_threshold=3,
        rate_threshold=0.05,
        emitted_ts="2026-05-01T10:01:05+00:00",
    )

    for alert in [ops_alert, payment_alert]:
        assert set(alert) >= {
            "alert_id",
            "alert_type",
            "source_topic",
            "event_time",
            "window_start_ts",
            "window_end_ts",
            "severity",
            "dimensions",
            "metrics",
            "schema_version",
            "emitted_ts",
        }

    assert ops_alert["alert_type"] == "traffic_burst"
    assert payment_alert["alert_type"] == "payment_failure_spike"
    assert payment_alert["severity"] == "high"
