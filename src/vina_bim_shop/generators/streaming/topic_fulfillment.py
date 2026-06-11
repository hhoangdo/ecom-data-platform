from __future__ import annotations

from typing import Any

import pandas as pd

from vina_bim_shop.generators.config import GeneratorConfig
from vina_bim_shop.generators.streaming.envelope import (
    envelope,
    event_id,
    iso,
    json_value,
    topic_frame,
)


def _fulfillment_topic_events(config: GeneratorConfig, shipments: pd.DataFrame) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    topic = "fulfillment_events"

    for seq, shipment in enumerate(shipments.itertuples(index=False), start=1):
        rows.append(
            envelope(
                config,
                topic,
                "shipment_created",
                event_id(topic, "shipment_created", seq, shipment.created_ts),
                shipment.created_ts,
                shipment.created_ts,
                {"shipment_id": shipment.shipment_id, "order_id": shipment.order_id, "customer_id": shipment.customer_id},
                {"shipping_city": json_value(shipment.shipping_city), "shipping_method": json_value(shipment.shipping_method)},
            )
        )

    handed_off = shipments[shipments["handoff_ts"].notna()]
    for seq, shipment in enumerate(handed_off.itertuples(index=False), start=1):
        rows.append(
            envelope(
                config,
                topic,
                "shipment_handoff",
                event_id(topic, "shipment_handoff", seq, shipment.handoff_ts),
                shipment.handoff_ts,
                shipment.handoff_ts,
                {"shipment_id": shipment.shipment_id, "order_id": shipment.order_id, "customer_id": shipment.customer_id},
                {"shipment_status": json_value(shipment.shipment_status)},
            )
        )

    delayed = shipments[shipments["shipment_status"].eq("delayed")]
    if delayed.empty:
        delayed = handed_off.head(1)
    for seq, shipment in enumerate(delayed.itertuples(index=False), start=1):
        delayed_ts = pd.Timestamp(shipment.handoff_ts) + pd.Timedelta(hours=12)
        rows.append(
            envelope(
                config,
                topic,
                "shipment_delayed",
                event_id(topic, "shipment_delayed", seq, delayed_ts),
                delayed_ts,
                delayed_ts,
                {"shipment_id": shipment.shipment_id, "order_id": shipment.order_id, "customer_id": shipment.customer_id},
                {"delay_reason": "carrier_capacity_or_weather", "shipping_region": json_value(shipment.shipping_region)},
            )
        )

    delivered = shipments[shipments["shipment_status"].eq("delivered")]
    if delivered.empty:
        delivered = handed_off.head(1)
    for seq, shipment in enumerate(delivered.itertuples(index=False), start=1):
        delivery_ts = pd.Timestamp(shipment.estimated_delivery_ts)
        rows.append(
            envelope(
                config,
                topic,
                "shipment_delivered",
                event_id(topic, "shipment_delivered", seq, delivery_ts),
                delivery_ts,
                delivery_ts,
                {"shipment_id": shipment.shipment_id, "order_id": shipment.order_id, "customer_id": shipment.customer_id},
                {"shipment_status": json_value(shipment.shipment_status)},
            )
        )

    blocked = shipments[shipments["shipment_status"].eq("blocked_payment_failed")]
    if blocked.empty:
        blocked = shipments.head(1)
    for seq, shipment in enumerate(blocked.itertuples(index=False), start=1):
        rows.append(
            envelope(
                config,
                topic,
                "shipment_blocked_payment_failed",
                event_id(topic, "shipment_blocked_payment_failed", seq, shipment.created_ts),
                shipment.created_ts,
                shipment.created_ts,
                {"shipment_id": shipment.shipment_id, "order_id": shipment.order_id, "customer_id": shipment.customer_id},
                {"block_reason": "payment_failed", "shipment_status": json_value(shipment.shipment_status)},
            )
        )

    return topic_frame(rows)


def fulfillment_topic_events(config: GeneratorConfig, shipments: pd.DataFrame) -> pd.DataFrame:
    return _fulfillment_topic_events(config, shipments)


__all__ = ["fulfillment_topic_events"]
