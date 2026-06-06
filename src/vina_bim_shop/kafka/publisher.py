from __future__ import annotations

import json
from typing import Any

import pandas as pd


def _json_default(value: Any) -> Any:
    if pd.isna(value):
        return None
    if hasattr(value, "isoformat"):
        return value.isoformat()
    return value


def publish_topic_events(
    *,
    topic_events: dict[str, pd.DataFrame],
    bootstrap_servers: str,
    flush_timeout_seconds: float = 30.0,
) -> dict[str, int]:
    from confluent_kafka import Producer

    producer = Producer({"bootstrap.servers": bootstrap_servers})
    publish_counts: dict[str, int] = {}

    for topic, frame in sorted(topic_events.items()):
        publish_counts[topic] = 0
        for record in frame.to_dict("records"):
            key = record.get("event_id") or record.get("dlq_id")
            producer.produce(
                topic,
                key=str(key).encode("utf-8") if key else None,
                value=json.dumps(record, default=_json_default, separators=(",", ":")).encode("utf-8"),
            )
            producer.poll(0)
            publish_counts[topic] += 1

    producer.flush(flush_timeout_seconds)
    return publish_counts
