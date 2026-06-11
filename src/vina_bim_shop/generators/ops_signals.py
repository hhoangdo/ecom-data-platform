from __future__ import annotations

from typing import Any

import pandas as pd

from vina_bim_shop.generators.config import GeneratorConfig
from vina_bim_shop.generators.streaming.envelope import (
    envelope,
    event_id,
    topic_frame,
    topic_schema_version,
)


def _ops_topic_events(config: GeneratorConfig, session_events: pd.DataFrame) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    topic = "ops_events"
    run_ts = pd.Timestamp(config.end_date) + pd.Timedelta(hours=23, minutes=59)

    rows.append(
        envelope(
            config,
            topic,
            "source_heartbeat",
            event_id(topic, "source_heartbeat", 1, run_ts),
            run_ts,
            run_ts,
            {"producer": config.kafka["producer"]},
            {"status": "ok", "topics": list(config.kafka["topics"])},
        )
    )

    burst_count = int(session_events["is_burst_window"].sum())
    burst_ts = session_events.loc[session_events["is_burst_window"], "event_timestamp"].min() if burst_count else run_ts
    rows.append(
        envelope(
            config,
            topic,
            "traffic_burst_detected",
            event_id(topic, "traffic_burst_detected", 1, burst_ts),
            burst_ts,
            burst_ts,
            {},
            {"burst_event_count": burst_count, "burst_windows": config.streaming["burst_windows"]},
        )
    )

    late_count = int(session_events["is_late_arrival"].sum())
    late_ts = session_events.loc[session_events["is_late_arrival"], "created_ts"].min() if late_count else run_ts
    rows.append(
        envelope(
            config,
            topic,
            "late_arrival_observed",
            event_id(topic, "late_arrival_observed", 1, late_ts),
            late_ts,
            late_ts,
            {},
            {
                "late_event_count": late_count,
                "allowed_lateness_seconds": config.kafka["watermark"]["allowed_lateness_seconds"],
            },
        )
    )

    duplicate_count = int(session_events["event_id"].duplicated().sum())
    rows.append(
        envelope(
            config,
            topic,
            "duplicate_event_observed",
            event_id(topic, "duplicate_event_observed", 1, run_ts),
            run_ts,
            run_ts,
            {},
            {"duplicate_event_count": duplicate_count, "dedup_key": ["event_id", "created_ts"]},
        )
    )

    cutoff = pd.to_datetime(config.end_date) - pd.Timedelta(
        days=int(config.history_days * (1 - float(config.quality["schema_evolution_cutoff_ratio"])))
    )
    rows.append(
        envelope(
            config,
            topic,
            "schema_version_changed",
            event_id(topic, "schema_version_changed", 1, cutoff),
            cutoff,
            cutoff,
            {},
            {
                "old_schema_version": 0,
                "new_schema_version": topic_schema_version(config, "commerce_events"),
                "changed_fields": ["fulfillment_channel", "device_metadata", "category_attributes", "promotion_funding_detail"],
            },
        )
    )

    return topic_frame(rows)


def ops_topic_events(config: GeneratorConfig, session_events: pd.DataFrame) -> pd.DataFrame:
    return _ops_topic_events(config, session_events)


__all__ = ["ops_topic_events"]
