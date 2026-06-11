from __future__ import annotations

from typing import Any

import numpy as np
import pandas as pd

from vina_bim_shop.generators.config import GeneratorConfig
from vina_bim_shop.generators.streaming.envelope import (
    envelope,
    event_id,
    json_value,
    payload_dict,
    topic_frame,
)


def _commerce_topic_events(
    config: GeneratorConfig,
    rng: np.random.Generator,
    session_events: pd.DataFrame,
    orders: pd.DataFrame,
    payments: pd.DataFrame,
) -> pd.DataFrame:
    from vina_bim_shop.generators.streaming.session_events import COMMERCE_EVENT_TYPE_MAP

    rows: list[dict[str, Any]] = []
    topic = "commerce_events"
    ordered_events = session_events.sort_values(["event_timestamp", "session_id", "event_type"]).reset_index(drop=True)
    orders_by_id = orders.drop_duplicates("order_id").set_index("order_id").to_dict("index")

    for event in ordered_events.itertuples(index=False):
        event_type = COMMERCE_EVENT_TYPE_MAP.get(str(event.event_type), str(event.event_type))
        order_context = orders_by_id.get(str(event.order_id), {}) if json_value(event.order_id) else {}
        payload = {
            **payload_dict(event.payload),
            "product_id": json_value(event.product_id),
            "order_id": json_value(event.order_id),
            "primary_category": json_value(event.primary_category),
            "device_type": json_value(event.device_type),
            "source": json_value(event.source),
            "order_status": json_value(order_context.get("status")),
            "order_net_amount": json_value(order_context.get("order_net_amount")),
        }
        rows.append(
            envelope(
                config,
                topic,
                event_type,
                f"KEVT-COM-{event.event_id}",
                event.event_timestamp,
                event.created_ts,
                {
                    "session_id": event.session_id,
                    "anonymous_id": event.anonymous_id,
                    "customer_id": event.customer_id,
                    "product_id": event.product_id,
                    "order_id": event.order_id,
                },
                payload,
            )
        )

    first_session_events = ordered_events.drop_duplicates("session_id")
    for seq, event in enumerate(first_session_events.itertuples(index=False), start=1):
        started_ts = pd.Timestamp(event.event_timestamp) - pd.Timedelta(seconds=1)
        rows.append(
            envelope(
                config,
                topic,
                "session_started",
                event_id(topic, "session_started", seq, started_ts),
                started_ts,
                event.created_ts,
                {"session_id": event.session_id, "anonymous_id": event.anonymous_id, "customer_id": event.customer_id},
                {"device_type": json_value(event.device_type), "source": json_value(event.source)},
            )
        )

    search_events = first_session_events.head(max(1, int(len(first_session_events) * 0.35)))
    for seq, event in enumerate(search_events.itertuples(index=False), start=1):
        search_ts = pd.Timestamp(event.event_timestamp) - pd.Timedelta(seconds=20)
        rows.append(
            envelope(
                config,
                topic,
                "search_performed",
                event_id(topic, "search_performed", seq, search_ts),
                search_ts,
                event.created_ts,
                {"session_id": event.session_id, "anonymous_id": event.anonymous_id, "customer_id": event.customer_id},
                {"query_family": json_value(event.primary_category), "result_count": int(rng.integers(12, 80))},
            )
        )

    cart_events = ordered_events[ordered_events["event_type"].eq("add_to_cart")].drop_duplicates("session_id")
    remove_events = cart_events.head(max(1, int(len(cart_events) * 0.06)))
    for seq, event in enumerate(remove_events.itertuples(index=False), start=1):
        remove_ts = pd.Timestamp(event.event_timestamp) + pd.Timedelta(seconds=45)
        rows.append(
            envelope(
                config,
                topic,
                "remove_from_cart",
                event_id(topic, "remove_from_cart", seq, remove_ts),
                remove_ts,
                remove_ts,
                {"session_id": event.session_id, "customer_id": event.customer_id, "product_id": event.product_id},
                {"cart_action": "remove", "product_id": json_value(event.product_id)},
            )
        )

    session_event_sets = ordered_events.groupby("session_id")["event_type"].agg(lambda values: set(values))
    abandoned_session_ids = [
        session_id
        for session_id, event_types in session_event_sets.items()
        if "checkout_started" in event_types and "order_placed" not in event_types
    ]
    checkout_events = ordered_events[
        ordered_events["session_id"].isin(abandoned_session_ids)
        & ordered_events["event_type"].eq("checkout_started")
    ].drop_duplicates("session_id")
    if checkout_events.empty:
        checkout_events = ordered_events[ordered_events["event_type"].eq("checkout_started")].head(1)
    for seq, event in enumerate(checkout_events.itertuples(index=False), start=1):
        abandoned_ts = pd.Timestamp(event.event_timestamp) + pd.Timedelta(minutes=20)
        rows.append(
            envelope(
                config,
                topic,
                "checkout_abandoned",
                event_id(topic, "checkout_abandoned", seq, abandoned_ts),
                abandoned_ts,
                abandoned_ts,
                {"session_id": event.session_id, "customer_id": event.customer_id, "product_id": event.product_id},
                {"abandonment_stage": "payment_or_review", "primary_category": json_value(event.primary_category)},
            )
        )

    coupon_orders = orders[orders["coupon_code"].notna()].head(max(1, int(len(orders) * 0.08)))
    if coupon_orders.empty:
        coupon_orders = orders.head(1)
    for seq, order in enumerate(coupon_orders.itertuples(index=False), start=1):
        coupon_ts = pd.Timestamp(order.order_timestamp) - pd.Timedelta(minutes=4)
        rows.append(
            envelope(
                config,
                topic,
                "coupon_applied",
                event_id(topic, "coupon_applied", seq, coupon_ts),
                coupon_ts,
                coupon_ts,
                {"session_id": order.session_id, "customer_id": order.customer_id, "order_id": order.order_id},
                {"coupon_code": json_value(order.coupon_code), "promotion_id": json_value(order.promotion_id)},
            )
        )

    cancelled_orders = orders[orders["status"].eq("payment_failed")].head(max(1, int(len(orders) * 0.01)))
    if cancelled_orders.empty:
        cancelled_orders = orders.head(1)
    for seq, order in enumerate(cancelled_orders.itertuples(index=False), start=1):
        cancelled_ts = pd.Timestamp(order.order_timestamp) + pd.Timedelta(minutes=35)
        rows.append(
            envelope(
                config,
                topic,
                "order_cancelled",
                event_id(topic, "order_cancelled", seq, cancelled_ts),
                cancelled_ts,
                cancelled_ts,
                {"session_id": order.session_id, "customer_id": order.customer_id, "order_id": order.order_id},
                {"cancel_reason": "payment_or_customer_cancelled", "order_status": json_value(order.status)},
            )
        )

    for seq, payment in enumerate(payments.itertuples(index=False), start=1):
        payment_type = "payment_succeeded" if payment.payment_status == "success" else "payment_failed"
        rows.append(
            envelope(
                config,
                topic,
                payment_type,
                event_id(topic, payment_type, seq, payment.payment_timestamp),
                payment.payment_timestamp,
                payment.created_ts,
                {"order_id": payment.order_id, "customer_id": payment.customer_id, "payment_id": payment.payment_id},
                {
                    "payment_method": json_value(payment.payment_method),
                    "amount": json_value(payment.amount),
                    "failure_reason": json_value(payment.failure_reason),
                },
            )
        )

    return topic_frame(rows)


def commerce_topic_events(
    config: GeneratorConfig,
    rng: np.random.Generator,
    session_events: pd.DataFrame,
    orders: pd.DataFrame,
    payments: pd.DataFrame,
) -> pd.DataFrame:
    return _commerce_topic_events(config, rng, session_events, orders, payments)


__all__ = ["commerce_topic_events"]
