from __future__ import annotations

import json
from typing import Any

import numpy as np
import pandas as pd

from vina_bim_shop.generators.config import GeneratorConfig


def _iso(value: Any) -> str:
    timestamp = pd.Timestamp(value)
    if pd.isna(timestamp):
        timestamp = pd.Timestamp.utcnow()
    return timestamp.isoformat()


def _json_value(value: Any) -> Any:
    if value is None:
        return None
    try:
        if pd.isna(value):
            return None
    except (TypeError, ValueError):
        pass
    if isinstance(value, pd.Timestamp):
        return _iso(value)
    if isinstance(value, np.integer):
        return int(value)
    if isinstance(value, np.floating):
        return float(value)
    if isinstance(value, np.bool_):
        return bool(value)
    return value


def _payload_dict(raw_payload: Any) -> dict[str, Any]:
    if isinstance(raw_payload, dict):
        return raw_payload
    if raw_payload is None:
        return {}
    try:
        if pd.isna(raw_payload):
            return {}
    except (TypeError, ValueError):
        pass
    if isinstance(raw_payload, str):
        try:
            parsed = json.loads(raw_payload)
        except json.JSONDecodeError:
            return {"raw_payload": raw_payload}
        return parsed if isinstance(parsed, dict) else {"value": parsed}
    return {"value": _json_value(raw_payload)}


def _event_id(topic: str, event_type: str, sequence: int, timestamp: Any) -> str:
    topic_hint = topic.replace("_events", "")[:3].upper()
    event_hint = event_type.replace("_", "-").upper()[:24]
    return f"KEVT-{topic_hint}-{event_hint}-{pd.Timestamp(timestamp).strftime('%Y%m%d')}-{sequence:010d}"


def _topic_schema_version(config: GeneratorConfig, topic: str) -> int:
    return int(config.kafka["topics"][topic]["schema_version"])


def _topic_frame(rows: list[dict[str, Any]]) -> pd.DataFrame:
    frame = pd.DataFrame(rows)
    if frame.empty:
        return frame
    return frame.sort_values(["event_timestamp", "event_type", "event_id"]).reset_index(drop=True)


def _envelope(
    config: GeneratorConfig,
    topic: str,
    event_type: str,
    event_id: str,
    event_timestamp: Any,
    created_ts: Any,
    correlation_ids: dict[str, Any],
    payload: dict[str, Any],
) -> dict[str, Any]:
    return {
        "event_id": event_id,
        "event_type": event_type,
        "event_topic": topic,
        "schema_version": _topic_schema_version(config, topic),
        "event_timestamp": _iso(event_timestamp),
        "created_ts": _iso(created_ts),
        "producer": config.kafka["producer"],
        "correlation_ids": {key: _json_value(value) for key, value in correlation_ids.items()},
        "payload": {key: _json_value(value) for key, value in payload.items()},
    }


__all__ = [
    "envelope",
    "topic_frame",
    "topic_schema_version",
    "event_id",
    "payload_dict",
    "json_value",
    "iso",
]


def envelope(
    config: GeneratorConfig,
    topic: str,
    event_type: str,
    event_id: str,
    event_timestamp: Any,
    created_ts: Any,
    correlation_ids: dict[str, Any],
    payload: dict[str, Any],
) -> dict[str, Any]:
    return _envelope(config, topic, event_type, event_id, event_timestamp, created_ts, correlation_ids, payload)


def topic_frame(rows: list[dict[str, Any]]) -> pd.DataFrame:
    return _topic_frame(rows)


def topic_schema_version(config: GeneratorConfig, topic: str) -> int:
    return _topic_schema_version(config, topic)


def event_id(topic: str, event_type: str, sequence: int, timestamp: Any) -> str:
    return _event_id(topic, event_type, sequence, timestamp)


def payload_dict(raw_payload: Any) -> dict[str, Any]:
    return _payload_dict(raw_payload)


def json_value(value: Any) -> Any:
    return _json_value(value)


def iso(value: Any) -> str:
    return _iso(value)
