"""Streaming event generation.

The legacy module exposed a single ``generate_streaming_events`` entry point
backed by 22 private helpers.  The helpers now live in dedicated submodules
grouped by responsibility.  This file remains as a thin public surface and a
backwards-compatible re-export of every symbol that used to be defined here.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import numpy as np
import pandas as pd

from vina_bim_shop.generators.config import GeneratorConfig
from vina_bim_shop.generators.duplicates import inject_stream_duplicates as _inject_stream_duplicates
from vina_bim_shop.generators.lateness import (
    apply_created_ts_and_late_arrivals as _apply_created_ts_and_late_arrivals,
)
from vina_bim_shop.generators.lateness import inject_device_missingness as _inject_device_missingness
from vina_bim_shop.generators.lateness import is_burst_timestamp as _is_burst_timestamp
from vina_bim_shop.generators.ops_signals import ops_topic_events as _ops_topic_events
from vina_bim_shop.generators.streaming.envelope import (
    _envelope,
    _event_id,
    _iso,
    _json_value,
    _payload_dict,
    _topic_frame,
    _topic_schema_version,
)
from vina_bim_shop.generators.streaming.session_events import (
    COMMERCE_EVENT_TYPE_MAP,
    _abandoned_sessions,
    _device_os,
    _events_from_orders,
    _finalize_event_frame,
)
from vina_bim_shop.generators.streaming.topic_catalog import _catalog_topic_events
from vina_bim_shop.generators.streaming.topic_commerce import _commerce_topic_events
from vina_bim_shop.generators.streaming.topic_fulfillment import _fulfillment_topic_events


@dataclass
class StreamingGeneration:
    topic_events: dict[str, pd.DataFrame]
    issue_records: list[dict[str, Any]]


def _build_topic_events(
    config: GeneratorConfig,
    rng: np.random.Generator,
    session_events: pd.DataFrame,
    offline_datasets: dict[str, pd.DataFrame],
) -> dict[str, pd.DataFrame]:
    return {
        "commerce_events": _commerce_topic_events(
            config,
            rng,
            session_events,
            offline_datasets["orders"],
            offline_datasets["payments"],
        ),
        "catalog_events": _catalog_topic_events(
            config,
            offline_datasets["products"],
            offline_datasets["inventory_snapshots"],
            offline_datasets["promotions"],
        ),
        "fulfillment_events": _fulfillment_topic_events(config, offline_datasets["shipments"]),
        "ops_events": _ops_topic_events(config, session_events),
    }


def generate_streaming_events(
    config: GeneratorConfig,
    offline_datasets: dict[str, pd.DataFrame],
) -> StreamingGeneration:
    rng = np.random.default_rng(config.random_seed + 100_000)
    orders = offline_datasets["orders"].copy()
    order_items = offline_datasets["order_items"].drop_duplicates("order_item_id")
    products = offline_datasets["products"]
    customers = offline_datasets["customers"]

    events = _events_from_orders(config, rng, orders, order_items)
    abandoned = _abandoned_sessions(config, rng, customers, products, orders)
    events = pd.concat([events, abandoned], ignore_index=True)
    events = events.sort_values(["event_timestamp", "session_id", "event_type"]).reset_index(drop=True)
    events["event_id"] = _assign_event_ids(events)

    events = _apply_created_ts_and_late_arrivals(config, rng, events)
    events = _inject_device_missingness(config, rng, events)
    events, issues = _inject_stream_duplicates(config, rng, events)
    issues.append(
        {
            "dataset": "commerce_events",
            "issue_type": "missing_device_type",
            "affected_rows": int(events["device_type"].isna().sum()),
            "observed_rate": round(float(events["device_type"].isna().mean()), 5),
        }
    )
    issues.append(
        {
            "dataset": "commerce_events",
            "issue_type": "late_arrival",
            "affected_rows": int(events["is_late_arrival"].sum()),
            "observed_rate": round(float(events["is_late_arrival"].mean()), 5),
        }
    )
    topic_events = _build_topic_events(config, rng, events, offline_datasets)
    return StreamingGeneration(topic_events=topic_events, issue_records=issues)


def _assign_event_ids(events: pd.DataFrame) -> pd.Series:
    from vina_bim_shop.generators.ids import dated_ids

    return dated_ids(
        "EVT",
        events["event_type"].str[:3].str.upper(),
        events["event_timestamp"],
        pd.Series(np.arange(1, len(events) + 1)),
    )


__all__ = [
    "COMMERCE_EVENT_TYPE_MAP",
    "StreamingGeneration",
    "_abandoned_sessions",
    "_apply_created_ts_and_late_arrivals",
    "_build_topic_events",
    "_catalog_topic_events",
    "_commerce_topic_events",
    "_device_os",
    "_envelope",
    "_events_from_orders",
    "_event_id",
    "_finalize_event_frame",
    "_fulfillment_topic_events",
    "_inject_device_missingness",
    "_inject_stream_duplicates",
    "_is_burst_timestamp",
    "_iso",
    "_json_value",
    "_ops_topic_events",
    "_payload_dict",
    "_topic_frame",
    "_topic_schema_version",
    "generate_streaming_events",
]
