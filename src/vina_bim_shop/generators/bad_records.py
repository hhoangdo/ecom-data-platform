from __future__ import annotations

import json
from typing import Any


def build_dead_letter_events(config: Any) -> Any:
    import pandas as pd

    ingest_ts = (pd.Timestamp(config.end_date) + pd.Timedelta(days=1, minutes=5)).isoformat()
    raw_payloads = [
        {
            "dlq_id": "DLQ-EVENT-0001",
            "source_topic": "commerce_events",
            "error_reason": "missing_required_key",
            "raw_payload": json.dumps(
                {
                    "event_type": "order_placed",
                    "event_topic": "commerce_events",
                    "schema_version": 1,
                    "event_timestamp": f"{config.end_date}T10:00:00",
                    "created_ts": f"{config.end_date}T10:00:03",
                    "producer": config.kafka["producer"],
                    "payload": {"order_id": "ORD-MISSING-EVENT-ID"},
                },
                separators=(",", ":"),
            ),
        },
        {
            "dlq_id": "DLQ-EVENT-0002",
            "source_topic": "commerce_events",
            "error_reason": "invalid_json",
            "raw_payload": '{"event_id":"BROKEN","event_type":"order_placed",',
        },
        {
            "dlq_id": "DLQ-EVENT-0003",
            "source_topic": "fulfillment_events",
            "error_reason": "invalid_timestamp",
            "raw_payload": json.dumps(
                {
                    "event_id": "BAD-TS-0001",
                    "event_type": "shipment_handoff",
                    "event_topic": "fulfillment_events",
                    "schema_version": 1,
                    "event_timestamp": "not-a-timestamp",
                    "created_ts": f"{config.end_date}T12:00:00",
                    "producer": config.kafka["producer"],
                    "correlation_ids": {"shipment_id": "SHP-BAD-TS"},
                    "payload": {"shipment_status": "handoff"},
                },
                separators=(",", ":"),
            ),
        },
        {
            "dlq_id": "DLQ-EVENT-0004",
            "source_topic": "catalog_events",
            "error_reason": "unknown_schema_version",
            "raw_payload": json.dumps(
                {
                    "event_id": "BAD-SCHEMA-0001",
                    "event_type": "product_updated",
                    "event_topic": "catalog_events",
                    "schema_version": 99,
                    "event_timestamp": f"{config.end_date}T13:00:00",
                    "created_ts": f"{config.end_date}T13:00:02",
                    "producer": config.kafka["producer"],
                    "correlation_ids": {"product_id": "PRD-UNKNOWN-SCHEMA"},
                    "payload": {"field": "category_attributes"},
                },
                separators=(",", ":"),
            ),
        },
    ]
    rows = []
    for payload in raw_payloads:
        rows.append(
            {
                **payload,
                "event_topic": "dead_letter_events",
                "schema_version": 1,
                "ingest_ts": ingest_ts,
            }
        )
    return pd.DataFrame(rows)


def build_bad_snapshots(config: Any) -> Any:
    import pandas as pd

    ingest_ts = (pd.Timestamp(config.end_date) + pd.Timedelta(days=1, minutes=10)).isoformat()
    records = [
        {
            "bad_record_id": "BAD-SNAPSHOT-0001",
            "source_dataset": "orders",
            "error_reason": "missing_required_key",
            "raw_record": json.dumps(
                {
                    "customer_id": "CUS-MISSING-ORDER-ID",
                    "order_timestamp": f"{config.end_date}T09:00:00",
                    "status": "paid",
                },
                separators=(",", ":"),
            ),
        },
        {
            "bad_record_id": "BAD-SNAPSHOT-0002",
            "source_dataset": "payments",
            "error_reason": "invalid_json",
            "raw_record": '{"payment_id":"PAY-BROKEN","order_id":"ORD-BROKEN",',
        },
        {
            "bad_record_id": "BAD-SNAPSHOT-0003",
            "source_dataset": "shipments",
            "error_reason": "invalid_timestamp",
            "raw_record": json.dumps(
                {
                    "shipment_id": "SHP-BAD-TS",
                    "order_id": "ORD-BAD-TS",
                    "handoff_ts": "tomorrow-ish",
                    "shipment_status": "handoff",
                },
                separators=(",", ":"),
            ),
        },
        {
            "bad_record_id": "BAD-SNAPSHOT-0004",
            "source_dataset": "products",
            "error_reason": "unknown_schema_version",
            "raw_record": json.dumps(
                {
                    "schema_version": 99,
                    "product_id": "PRD-UNKNOWN-SCHEMA",
                    "primary_category": "FMCG",
                    "category_attributes": {"unexpected": "shape"},
                },
                separators=(",", ":"),
            ),
        },
    ]
    return pd.DataFrame([{**record, "ingest_ts": ingest_ts} for record in records])
