import json
from pathlib import Path

import pandas as pd

from vina_bim_shop.generators.runner import run_generation


EXPECTED_EVENTS = {
    "commerce_events": {
        "session_started",
        "search_performed",
        "product_viewed",
        "add_to_cart",
        "remove_from_cart",
        "checkout_started",
        "checkout_abandoned",
        "coupon_applied",
        "order_placed",
        "order_cancelled",
        "payment_succeeded",
        "payment_failed",
    },
    "catalog_events": {
        "product_created",
        "product_updated",
        "price_changed",
        "inventory_snapshot",
        "inventory_low_stock",
        "promotion_created",
        "promotion_activated",
    },
    "fulfillment_events": {
        "shipment_created",
        "shipment_handoff",
        "shipment_delayed",
        "shipment_delivered",
        "shipment_blocked_payment_failed",
    },
    "ops_events": {
        "source_heartbeat",
        "traffic_burst_detected",
        "late_arrival_observed",
        "duplicate_event_observed",
        "schema_version_changed",
    },
}

EXPECTED_DLQ_REASONS = {
    "missing_required_key",
    "invalid_json",
    "invalid_timestamp",
    "unknown_schema_version",
}

ENVELOPE_FIELDS = {
    "event_id",
    "event_type",
    "event_topic",
    "schema_version",
    "event_timestamp",
    "created_ts",
    "producer",
    "correlation_ids",
    "payload",
}


def test_full_generation_writes_kafka_topic_jsonl_with_common_envelope(tmp_path: Path) -> None:
    repo_root = Path(__file__).resolve().parents[2]
    raw_root = tmp_path / "raw"
    evidence_root = tmp_path / "evidence"

    result = run_generation(
        config_path=repo_root / "configs" / "generator" / "base.yaml",
        scale="smoke",
        mode="full",
        raw_root=raw_root,
        evidence_root=evidence_root,
        clean=True,
        seed=123,
    )

    assert "kafka_topics" in result.row_counts
    assert "bad_snapshots" in result.row_counts

    topic_root = raw_root / "kafka_topics"
    observed_counts: dict[str, int] = {}

    for topic, expected_event_types in EXPECTED_EVENTS.items():
        topic_path = topic_root / topic / "events.jsonl"
        assert topic_path.is_file()

        events = [json.loads(line) for line in topic_path.read_text(encoding="utf-8").splitlines() if line]
        assert events
        observed_counts[topic] = len(events)

        assert expected_event_types.issubset({event["event_type"] for event in events})
        for event in events:
            assert ENVELOPE_FIELDS.issubset(event)
            assert event["event_topic"] == topic
            assert isinstance(event["correlation_ids"], dict)
            assert isinstance(event["payload"], dict)

    dlq_path = topic_root / "dead_letter_events" / "events.jsonl"
    assert dlq_path.is_file()
    dlq_events = [json.loads(line) for line in dlq_path.read_text(encoding="utf-8").splitlines() if line]
    assert EXPECTED_DLQ_REASONS.issubset({event["error_reason"] for event in dlq_events})
    for event in dlq_events:
        assert event["event_topic"] == "dead_letter_events"
        assert event["source_topic"]
        assert event["raw_payload"]
    observed_counts["dead_letter_events"] = len(dlq_events)

    bad_snapshot_path = raw_root / "bad_snapshots" / "bad_snapshots.jsonl"
    assert bad_snapshot_path.is_file()
    bad_snapshots = [json.loads(line) for line in bad_snapshot_path.read_text(encoding="utf-8").splitlines() if line]
    assert EXPECTED_DLQ_REASONS.issubset({record["error_reason"] for record in bad_snapshots})
    for record in bad_snapshots:
        assert record["source_dataset"]
        assert record["raw_record"]

    topic_counts = pd.read_csv(evidence_root / "event_topic_row_counts.csv")
    assert set(topic_counts["event_topic"]) == set(EXPECTED_EVENTS) | {"dead_letter_events"}
    assert topic_counts.set_index("event_topic")["row_count"].to_dict() == observed_counts

    schema_versions = pd.read_csv(evidence_root / "schema_version_summary.csv")
    assert set(schema_versions["event_topic"]) == set(EXPECTED_EVENTS) | {"dead_letter_events"}
    assert schema_versions["schema_version"].notna().all()
