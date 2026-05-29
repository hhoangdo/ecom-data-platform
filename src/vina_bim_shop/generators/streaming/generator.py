from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any

import numpy as np
import pandas as pd

from vina_bim_shop.generators.config import GeneratorConfig
from vina_bim_shop.generators.ids import dated_ids


@dataclass
class StreamingGeneration:
    stream_events: pd.DataFrame
    topic_events: dict[str, pd.DataFrame]
    issue_records: list[dict[str, Any]]


COMMERCE_EVENT_TYPE_MAP = {
    "view": "product_viewed",
    "add_to_cart": "add_to_cart",
    "checkout_started": "checkout_started",
    "order_placed": "order_placed",
    "payment_failed": "payment_failed",
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
    events["event_id"] = dated_ids("EVT", events["event_type"].str[:3].str.upper(), events["event_timestamp"], pd.Series(np.arange(1, len(events) + 1)))

    events = _apply_created_ts_and_late_arrivals(config, rng, events)
    events = _inject_device_missingness(config, rng, events)
    events, issues = _inject_stream_duplicates(config, rng, events)
    issues.append(
        {
            "dataset": "stream_events",
            "issue_type": "missing_device_type",
            "affected_rows": int(events["device_type"].isna().sum()),
            "observed_rate": round(float(events["device_type"].isna().mean()), 5),
        }
    )
    issues.append(
        {
            "dataset": "stream_events",
            "issue_type": "late_arrival",
            "affected_rows": int(events["is_late_arrival"].sum()),
            "observed_rate": round(float(events["is_late_arrival"].mean()), 5),
        }
    )
    topic_events = _build_topic_events(config, rng, events, offline_datasets)
    return StreamingGeneration(stream_events=events, topic_events=topic_events, issue_records=issues)


def _events_from_orders(
    config: GeneratorConfig,
    rng: np.random.Generator,
    orders: pd.DataFrame,
    order_items: pd.DataFrame,
) -> pd.DataFrame:
    item_lookup = order_items.groupby("order_id")["product_id"].apply(list).to_dict()
    rows: list[dict[str, Any]] = []
    device_labels = list(config.streaming["device_types"])
    device_weights = list(config.streaming["device_types"].values())
    source_labels = list(config.streaming["sources"])
    source_weights = list(config.streaming["sources"].values())
    for order in orders.itertuples(index=False):
        order_ts = pd.Timestamp(order.order_timestamp)
        product_ids = item_lookup.get(order.order_id, [])
        if not product_ids:
            continue
        selected_products = list(dict.fromkeys(product_ids[: max(1, min(4, len(product_ids)))]))
        if rng.random() < float(config.streaming["repeated_view_rate"]):
            selected_products.append(str(rng.choice(selected_products)))
        device_type = str(rng.choice(device_labels, p=np.asarray(device_weights, dtype=float) / sum(device_weights)))
        source = str(rng.choice(source_labels, p=np.asarray(source_weights, dtype=float) / sum(source_weights)))
        anonymous_first = rng.random() < float(config.streaming["anonymous_first_event_rate"])
        base_context = {
            "session_id": order.session_id,
            "anonymous_id": order.anonymous_id,
            "device_type": device_type,
            "source": source,
            "order_id": None,
            "quantity": None,
            "price": None,
            "primary_category": order.primary_category,
        }
        for idx, product_id in enumerate(selected_products):
            rows.append(
                {
                    **base_context,
                    "event_type": "view",
                    "event_timestamp": order_ts - pd.Timedelta(minutes=int(rng.integers(18, 90))) + pd.Timedelta(seconds=int(idx * 17)),
                    "customer_id": None if anonymous_first and idx == 0 else order.customer_id,
                    "product_id": product_id,
                    "payload": "{\"surface\":\"search_or_feed\"}",
                }
            )
        cart_product = selected_products[0]
        rows.append(
            {
                **base_context,
                "event_type": "add_to_cart",
                "event_timestamp": order_ts - pd.Timedelta(minutes=int(rng.integers(10, 40))),
                "customer_id": order.customer_id,
                "product_id": cart_product,
                "payload": "{\"cart_action\":\"add\"}",
            }
        )
        rows.append(
            {
                **base_context,
                "event_type": "checkout_started",
                "event_timestamp": order_ts - pd.Timedelta(minutes=int(rng.integers(2, 10))),
                "customer_id": order.customer_id,
                "product_id": cart_product,
                "payload": "{\"checkout_step\":\"shipping\"}",
            }
        )
        rows.append(
            {
                **base_context,
                "event_type": "order_placed",
                "event_timestamp": order_ts,
                "customer_id": order.customer_id,
                "product_id": cart_product,
                "order_id": order.order_id,
                "payload": "{\"order_source\":\"stream_derived_from_offline\"}",
            }
        )
        if order.status == "payment_failed":
            rows.append(
                {
                    **base_context,
                    "event_type": "payment_failed",
                    "event_timestamp": order_ts + pd.Timedelta(minutes=int(rng.integers(1, 25))),
                    "customer_id": order.customer_id,
                    "product_id": cart_product,
                    "order_id": order.order_id,
                    "payload": "{\"failure_surface\":\"payment_provider\"}",
                }
            )
    return _finalize_event_frame(config, pd.DataFrame(rows))


def _abandoned_sessions(
    config: GeneratorConfig,
    rng: np.random.Generator,
    customers: pd.DataFrame,
    products: pd.DataFrame,
    orders: pd.DataFrame,
) -> pd.DataFrame:
    n = max(1, int(len(orders) * float(config.streaming["abandoned_session_ratio"])))
    selected_customers = customers.iloc[rng.choice(customers.index.to_numpy(), size=n)].reset_index(drop=True)
    selected_products = products.iloc[rng.choice(products.index.to_numpy(), size=n)].reset_index(drop=True)
    reference_ts = pd.to_datetime(orders.iloc[rng.choice(orders.index.to_numpy(), size=n)]["order_timestamp"]).reset_index(drop=True)
    session_id = dated_ids("SES", selected_customers["city_code"], reference_ts, pd.Series(np.arange(1, n + 1) + 7_000_000))
    rows = []
    for i in range(n):
        device_type = str(rng.choice(list(config.streaming["device_types"]), p=np.asarray(list(config.streaming["device_types"].values()), dtype=float) / sum(config.streaming["device_types"].values())))
        source = str(rng.choice(list(config.streaming["sources"]), p=np.asarray(list(config.streaming["sources"].values()), dtype=float) / sum(config.streaming["sources"].values())))
        rows.append(
            {
                "session_id": session_id.iloc[i],
                "anonymous_id": selected_customers["anonymous_id"].iloc[i],
                "customer_id": None if rng.random() < 0.4 else selected_customers["customer_id"].iloc[i],
                "device_type": device_type,
                "source": source,
                "event_type": "view",
                "event_timestamp": reference_ts.iloc[i] - pd.Timedelta(minutes=int(rng.integers(5, 45))),
                "product_id": selected_products["product_id"].iloc[i],
                "order_id": None,
                "quantity": None,
                "price": None,
                "primary_category": selected_products["primary_category"].iloc[i],
                "payload": "{\"surface\":\"abandoned_session\"}",
            }
        )
        if rng.random() < 0.55:
            rows.append(
                {
                    **rows[-1],
                    "event_type": "add_to_cart",
                    "event_timestamp": reference_ts.iloc[i] - pd.Timedelta(minutes=int(rng.integers(2, 15))),
                    "customer_id": selected_customers["customer_id"].iloc[i],
                    "payload": "{\"cart_action\":\"add_abandoned\"}",
                }
            )
        if rng.random() < 0.22:
            rows.append(
                {
                    **rows[-1],
                    "event_type": "checkout_started",
                    "event_timestamp": reference_ts.iloc[i] - pd.Timedelta(minutes=int(rng.integers(1, 8))),
                    "customer_id": selected_customers["customer_id"].iloc[i],
                    "payload": "{\"checkout_step\":\"abandoned\"}",
                }
            )
    return _finalize_event_frame(config, pd.DataFrame(rows))


def _finalize_event_frame(config: GeneratorConfig, events: pd.DataFrame) -> pd.DataFrame:
    if events.empty:
        return events
    events["event_date"] = pd.to_datetime(events["event_timestamp"]).dt.date.astype(str)
    events["event_hour"] = pd.to_datetime(events["event_timestamp"]).dt.hour
    events["is_burst_window"] = events["event_timestamp"].map(_is_burst_timestamp)
    cutoff = pd.to_datetime(config.end_date) - pd.Timedelta(days=int(config.history_days * (1 - float(config.quality["schema_evolution_cutoff_ratio"]))))
    events["device_os"] = np.where(pd.to_datetime(events["event_timestamp"]) >= cutoff, events["device_type"].map(_device_os), None)
    events["app_version"] = np.where(pd.to_datetime(events["event_timestamp"]) >= cutoff, "v2." + (events["event_hour"] % 7).astype(str), None)
    return events


def _is_burst_timestamp(value: pd.Timestamp) -> bool:
    timestamp = pd.Timestamp(value)
    return (timestamp.hour == 12 and timestamp.minute <= 20) or (timestamp.hour == 20 and timestamp.minute <= 20)


def _device_os(device_type: str | None) -> str | None:
    if device_type == "app_ios":
        return "ios"
    if device_type == "app_android":
        return "android"
    if device_type in {"mobile_web", "desktop_web"}:
        return "web"
    return None


def _apply_created_ts_and_late_arrivals(
    config: GeneratorConfig,
    rng: np.random.Generator,
    events: pd.DataFrame,
) -> pd.DataFrame:
    output = events.copy()
    n = len(output)
    late = rng.random(n) < float(config.quality["late_arrival_rate"])
    normal_delay = np.zeros(n, dtype=int)
    late_delay = rng.integers(
        int(config.quality["late_delay_minutes_min"]) * 60,
        int(config.quality["late_delay_minutes_max"]) * 60,
        n,
    )
    delay = np.where(late, late_delay, normal_delay)
    output["created_ts"] = pd.to_datetime(output["event_timestamp"]) + pd.to_timedelta(delay, unit="s")
    output["is_late_arrival"] = late
    return output


def _inject_device_missingness(
    config: GeneratorConfig,
    rng: np.random.Generator,
    events: pd.DataFrame,
) -> pd.DataFrame:
    output = events.copy()
    missing = rng.random(len(output)) < float(config.quality["missing_device_type_rate"])
    output.loc[missing, ["device_type", "device_os"]] = None
    return output


def _inject_stream_duplicates(
    config: GeneratorConfig,
    rng: np.random.Generator,
    events: pd.DataFrame,
) -> tuple[pd.DataFrame, list[dict[str, Any]]]:
    duplicate_rate = float(config.quality["stream_duplicate_rate"])
    n_dupes = max(1, int(len(events) * duplicate_rate))
    duplicate_rows = events.iloc[rng.choice(events.index.to_numpy(), size=n_dupes, replace=False)].copy()
    output = pd.concat([events, duplicate_rows], ignore_index=True)
    return output, [
        {
            "dataset": "stream_events",
            "issue_type": "exact_duplicate_event_payload",
            "affected_rows": int(n_dupes),
            "observed_rate": round(float(n_dupes / len(output)), 5),
        }
    ]


def _build_topic_events(
    config: GeneratorConfig,
    rng: np.random.Generator,
    stream_events: pd.DataFrame,
    offline_datasets: dict[str, pd.DataFrame],
) -> dict[str, pd.DataFrame]:
    return {
        "commerce_events": _commerce_topic_events(
            config,
            rng,
            stream_events,
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
        "ops_events": _ops_topic_events(config, stream_events),
    }


def _commerce_topic_events(
    config: GeneratorConfig,
    rng: np.random.Generator,
    stream_events: pd.DataFrame,
    orders: pd.DataFrame,
    payments: pd.DataFrame,
) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    topic = "commerce_events"
    ordered_events = stream_events.sort_values(["event_timestamp", "session_id", "event_type"]).reset_index(drop=True)
    orders_by_id = orders.drop_duplicates("order_id").set_index("order_id").to_dict("index")

    for event in ordered_events.itertuples(index=False):
        event_type = COMMERCE_EVENT_TYPE_MAP.get(str(event.event_type), str(event.event_type))
        order_context = orders_by_id.get(str(event.order_id), {}) if _json_value(event.order_id) else {}
        payload = {
            **_payload_dict(event.payload),
            "product_id": _json_value(event.product_id),
            "order_id": _json_value(event.order_id),
            "primary_category": _json_value(event.primary_category),
            "device_type": _json_value(event.device_type),
            "source": _json_value(event.source),
            "order_status": _json_value(order_context.get("status")),
            "order_net_amount": _json_value(order_context.get("order_net_amount")),
        }
        rows.append(
            _envelope(
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
            _envelope(
                config,
                topic,
                "session_started",
                _event_id(topic, "session_started", seq, started_ts),
                started_ts,
                event.created_ts,
                {"session_id": event.session_id, "anonymous_id": event.anonymous_id, "customer_id": event.customer_id},
                {"device_type": _json_value(event.device_type), "source": _json_value(event.source)},
            )
        )

    search_events = first_session_events.head(max(1, int(len(first_session_events) * 0.35)))
    for seq, event in enumerate(search_events.itertuples(index=False), start=1):
        search_ts = pd.Timestamp(event.event_timestamp) - pd.Timedelta(seconds=20)
        rows.append(
            _envelope(
                config,
                topic,
                "search_performed",
                _event_id(topic, "search_performed", seq, search_ts),
                search_ts,
                event.created_ts,
                {"session_id": event.session_id, "anonymous_id": event.anonymous_id, "customer_id": event.customer_id},
                {"query_family": _json_value(event.primary_category), "result_count": int(rng.integers(12, 80))},
            )
        )

    cart_events = ordered_events[ordered_events["event_type"].eq("add_to_cart")].drop_duplicates("session_id")
    remove_events = cart_events.head(max(1, int(len(cart_events) * 0.06)))
    for seq, event in enumerate(remove_events.itertuples(index=False), start=1):
        remove_ts = pd.Timestamp(event.event_timestamp) + pd.Timedelta(seconds=45)
        rows.append(
            _envelope(
                config,
                topic,
                "remove_from_cart",
                _event_id(topic, "remove_from_cart", seq, remove_ts),
                remove_ts,
                remove_ts,
                {"session_id": event.session_id, "customer_id": event.customer_id, "product_id": event.product_id},
                {"cart_action": "remove", "product_id": _json_value(event.product_id)},
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
            _envelope(
                config,
                topic,
                "checkout_abandoned",
                _event_id(topic, "checkout_abandoned", seq, abandoned_ts),
                abandoned_ts,
                abandoned_ts,
                {"session_id": event.session_id, "customer_id": event.customer_id, "product_id": event.product_id},
                {"abandonment_stage": "payment_or_review", "primary_category": _json_value(event.primary_category)},
            )
        )

    coupon_orders = orders[orders["coupon_code"].notna()].head(max(1, int(len(orders) * 0.08)))
    if coupon_orders.empty:
        coupon_orders = orders.head(1)
    for seq, order in enumerate(coupon_orders.itertuples(index=False), start=1):
        coupon_ts = pd.Timestamp(order.order_timestamp) - pd.Timedelta(minutes=4)
        rows.append(
            _envelope(
                config,
                topic,
                "coupon_applied",
                _event_id(topic, "coupon_applied", seq, coupon_ts),
                coupon_ts,
                coupon_ts,
                {"session_id": order.session_id, "customer_id": order.customer_id, "order_id": order.order_id},
                {"coupon_code": _json_value(order.coupon_code), "promotion_id": _json_value(order.promotion_id)},
            )
        )

    cancelled_orders = orders[orders["status"].eq("payment_failed")].head(max(1, int(len(orders) * 0.01)))
    if cancelled_orders.empty:
        cancelled_orders = orders.head(1)
    for seq, order in enumerate(cancelled_orders.itertuples(index=False), start=1):
        cancelled_ts = pd.Timestamp(order.order_timestamp) + pd.Timedelta(minutes=35)
        rows.append(
            _envelope(
                config,
                topic,
                "order_cancelled",
                _event_id(topic, "order_cancelled", seq, cancelled_ts),
                cancelled_ts,
                cancelled_ts,
                {"session_id": order.session_id, "customer_id": order.customer_id, "order_id": order.order_id},
                {"cancel_reason": "payment_or_customer_cancelled", "order_status": _json_value(order.status)},
            )
        )

    for seq, payment in enumerate(payments.itertuples(index=False), start=1):
        payment_type = "payment_succeeded" if payment.payment_status == "success" else "payment_failed"
        rows.append(
            _envelope(
                config,
                topic,
                payment_type,
                _event_id(topic, payment_type, seq, payment.payment_timestamp),
                payment.payment_timestamp,
                payment.created_ts,
                {"order_id": payment.order_id, "customer_id": payment.customer_id, "payment_id": payment.payment_id},
                {
                    "payment_method": _json_value(payment.payment_method),
                    "amount": _json_value(payment.amount),
                    "failure_reason": _json_value(payment.failure_reason),
                },
            )
        )

    return _topic_frame(rows)


def _catalog_topic_events(
    config: GeneratorConfig,
    products: pd.DataFrame,
    inventory_snapshots: pd.DataFrame,
    promotions: pd.DataFrame,
) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    topic = "catalog_events"

    for seq, product in enumerate(products.itertuples(index=False), start=1):
        rows.append(
            _envelope(
                config,
                topic,
                "product_created",
                _event_id(topic, "product_created", seq, product.created_ts),
                product.created_ts,
                product.created_ts,
                {"product_id": product.product_id, "seller_id": product.seller_id},
                {
                    "product_name": _json_value(product.product_name),
                    "primary_category": _json_value(product.primary_category),
                    "primary_subcategory": _json_value(product.primary_subcategory),
                    "brand": _json_value(product.brand),
                    "base_price": _json_value(product.base_price),
                    "category_attributes": _payload_dict(product.category_attributes),
                },
            )
        )

    updated_products = products[products["category_attributes"].notna()].head(max(1, int(len(products) * 0.05)))
    if updated_products.empty:
        updated_products = products.head(1)
    for seq, product in enumerate(updated_products.itertuples(index=False), start=1):
        updated_ts = pd.Timestamp(product.created_ts) + pd.Timedelta(days=1)
        rows.append(
            _envelope(
                config,
                topic,
                "product_updated",
                _event_id(topic, "product_updated", seq, updated_ts),
                updated_ts,
                updated_ts,
                {"product_id": product.product_id, "seller_id": product.seller_id},
                {"changed_fields": ["category_attributes", "fulfillment_channel"], "fulfillment_channel": _json_value(product.fulfillment_channel)},
            )
        )
        rows.append(
            _envelope(
                config,
                topic,
                "price_changed",
                _event_id(topic, "price_changed", seq, updated_ts + pd.Timedelta(minutes=5)),
                updated_ts + pd.Timedelta(minutes=5),
                updated_ts + pd.Timedelta(minutes=5),
                {"product_id": product.product_id, "seller_id": product.seller_id},
                {"old_price": round(float(product.base_price) * 1.05, 2), "new_price": _json_value(product.base_price)},
            )
        )

    for seq, snapshot in enumerate(inventory_snapshots.itertuples(index=False), start=1):
        rows.append(
            _envelope(
                config,
                topic,
                "inventory_snapshot",
                _event_id(topic, "inventory_snapshot", seq, snapshot.snapshot_ts),
                snapshot.snapshot_ts,
                snapshot.snapshot_ts,
                {"snapshot_id": snapshot.snapshot_id, "product_id": snapshot.product_id, "seller_id": snapshot.seller_id},
                {"stock_on_hand": _json_value(snapshot.stock_on_hand), "reserved_stock": _json_value(snapshot.reserved_stock)},
            )
        )

    low_stock_cutoff = inventory_snapshots["stock_on_hand"].quantile(0.08)
    low_stock = inventory_snapshots[inventory_snapshots["stock_on_hand"].le(low_stock_cutoff)].head(max(1, int(len(inventory_snapshots) * 0.04)))
    if low_stock.empty:
        low_stock = inventory_snapshots.nsmallest(1, "stock_on_hand")
    for seq, snapshot in enumerate(low_stock.itertuples(index=False), start=1):
        rows.append(
            _envelope(
                config,
                topic,
                "inventory_low_stock",
                _event_id(topic, "inventory_low_stock", seq, snapshot.snapshot_ts),
                snapshot.snapshot_ts,
                snapshot.snapshot_ts,
                {"snapshot_id": snapshot.snapshot_id, "product_id": snapshot.product_id, "seller_id": snapshot.seller_id},
                {"stock_on_hand": _json_value(snapshot.stock_on_hand), "threshold": _json_value(low_stock_cutoff)},
            )
        )

    for seq, promotion in enumerate(promotions.itertuples(index=False), start=1):
        rows.append(
            _envelope(
                config,
                topic,
                "promotion_created",
                _event_id(topic, "promotion_created", seq, promotion.created_ts),
                promotion.created_ts,
                promotion.created_ts,
                {"promotion_id": promotion.promotion_id, "seller_id": promotion.seller_id},
                {
                    "promotion_name": _json_value(promotion.promotion_name),
                    "funding_type": _json_value(promotion.funding_type),
                    "funding_detail": _payload_dict(promotion.funding_detail),
                    "category": _json_value(promotion.category),
                    "discount_rate": _json_value(promotion.discount_rate),
                },
            )
        )
        rows.append(
            _envelope(
                config,
                topic,
                "promotion_activated",
                _event_id(topic, "promotion_activated", seq, promotion.promotion_start_ts),
                promotion.promotion_start_ts,
                promotion.promotion_start_ts,
                {"promotion_id": promotion.promotion_id, "seller_id": promotion.seller_id},
                {"promotion_end_ts": _iso(promotion.promotion_end_ts), "category": _json_value(promotion.category)},
            )
        )

    return _topic_frame(rows)


def _fulfillment_topic_events(config: GeneratorConfig, shipments: pd.DataFrame) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    topic = "fulfillment_events"

    for seq, shipment in enumerate(shipments.itertuples(index=False), start=1):
        rows.append(
            _envelope(
                config,
                topic,
                "shipment_created",
                _event_id(topic, "shipment_created", seq, shipment.created_ts),
                shipment.created_ts,
                shipment.created_ts,
                {"shipment_id": shipment.shipment_id, "order_id": shipment.order_id, "customer_id": shipment.customer_id},
                {"shipping_city": _json_value(shipment.shipping_city), "shipping_method": _json_value(shipment.shipping_method)},
            )
        )

    handed_off = shipments[shipments["handoff_ts"].notna()]
    for seq, shipment in enumerate(handed_off.itertuples(index=False), start=1):
        rows.append(
            _envelope(
                config,
                topic,
                "shipment_handoff",
                _event_id(topic, "shipment_handoff", seq, shipment.handoff_ts),
                shipment.handoff_ts,
                shipment.handoff_ts,
                {"shipment_id": shipment.shipment_id, "order_id": shipment.order_id, "customer_id": shipment.customer_id},
                {"shipment_status": _json_value(shipment.shipment_status)},
            )
        )

    delayed = shipments[shipments["shipment_status"].eq("delayed")]
    if delayed.empty:
        delayed = handed_off.head(1)
    for seq, shipment in enumerate(delayed.itertuples(index=False), start=1):
        delayed_ts = pd.Timestamp(shipment.handoff_ts) + pd.Timedelta(hours=12)
        rows.append(
            _envelope(
                config,
                topic,
                "shipment_delayed",
                _event_id(topic, "shipment_delayed", seq, delayed_ts),
                delayed_ts,
                delayed_ts,
                {"shipment_id": shipment.shipment_id, "order_id": shipment.order_id, "customer_id": shipment.customer_id},
                {"delay_reason": "carrier_capacity_or_weather", "shipping_region": _json_value(shipment.shipping_region)},
            )
        )

    delivered = shipments[shipments["shipment_status"].eq("delivered")]
    if delivered.empty:
        delivered = handed_off.head(1)
    for seq, shipment in enumerate(delivered.itertuples(index=False), start=1):
        delivery_ts = pd.Timestamp(shipment.estimated_delivery_ts)
        rows.append(
            _envelope(
                config,
                topic,
                "shipment_delivered",
                _event_id(topic, "shipment_delivered", seq, delivery_ts),
                delivery_ts,
                delivery_ts,
                {"shipment_id": shipment.shipment_id, "order_id": shipment.order_id, "customer_id": shipment.customer_id},
                {"shipment_status": _json_value(shipment.shipment_status)},
            )
        )

    blocked = shipments[shipments["shipment_status"].eq("blocked_payment_failed")]
    if blocked.empty:
        blocked = shipments.head(1)
    for seq, shipment in enumerate(blocked.itertuples(index=False), start=1):
        rows.append(
            _envelope(
                config,
                topic,
                "shipment_blocked_payment_failed",
                _event_id(topic, "shipment_blocked_payment_failed", seq, shipment.created_ts),
                shipment.created_ts,
                shipment.created_ts,
                {"shipment_id": shipment.shipment_id, "order_id": shipment.order_id, "customer_id": shipment.customer_id},
                {"block_reason": "payment_failed", "shipment_status": _json_value(shipment.shipment_status)},
            )
        )

    return _topic_frame(rows)


def _ops_topic_events(config: GeneratorConfig, stream_events: pd.DataFrame) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    topic = "ops_events"
    run_ts = pd.Timestamp(config.end_date) + pd.Timedelta(hours=23, minutes=59)

    rows.append(
        _envelope(
            config,
            topic,
            "source_heartbeat",
            _event_id(topic, "source_heartbeat", 1, run_ts),
            run_ts,
            run_ts,
            {"producer": config.kafka["producer"]},
            {"status": "ok", "topics": list(config.kafka["topics"])},
        )
    )

    burst_count = int(stream_events["is_burst_window"].sum())
    burst_ts = stream_events.loc[stream_events["is_burst_window"], "event_timestamp"].min() if burst_count else run_ts
    rows.append(
        _envelope(
            config,
            topic,
            "traffic_burst_detected",
            _event_id(topic, "traffic_burst_detected", 1, burst_ts),
            burst_ts,
            burst_ts,
            {},
            {"burst_event_count": burst_count, "burst_windows": config.streaming["burst_windows"]},
        )
    )

    late_count = int(stream_events["is_late_arrival"].sum())
    late_ts = stream_events.loc[stream_events["is_late_arrival"], "created_ts"].min() if late_count else run_ts
    rows.append(
        _envelope(
            config,
            topic,
            "late_arrival_observed",
            _event_id(topic, "late_arrival_observed", 1, late_ts),
            late_ts,
            late_ts,
            {},
            {
                "late_event_count": late_count,
                "allowed_lateness_seconds": config.kafka["watermark"]["allowed_lateness_seconds"],
            },
        )
    )

    duplicate_count = int(stream_events["event_id"].duplicated().sum())
    rows.append(
        _envelope(
            config,
            topic,
            "duplicate_event_observed",
            _event_id(topic, "duplicate_event_observed", 1, run_ts),
            run_ts,
            run_ts,
            {},
            {"duplicate_event_count": duplicate_count, "dedup_key": ["event_id", "created_ts"]},
        )
    )

    cutoff = pd.to_datetime(config.end_date) - pd.Timedelta(days=int(config.history_days * (1 - float(config.quality["schema_evolution_cutoff_ratio"]))))
    rows.append(
        _envelope(
            config,
            topic,
            "schema_version_changed",
            _event_id(topic, "schema_version_changed", 1, cutoff),
            cutoff,
            cutoff,
            {},
            {
                "old_schema_version": 0,
                "new_schema_version": _topic_schema_version(config, "commerce_events"),
                "changed_fields": ["fulfillment_channel", "device_metadata", "category_attributes", "promotion_funding_detail"],
            },
        )
    )

    return _topic_frame(rows)


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


def _topic_frame(rows: list[dict[str, Any]]) -> pd.DataFrame:
    frame = pd.DataFrame(rows)
    if frame.empty:
        return frame
    return frame.sort_values(["event_timestamp", "event_type", "event_id"]).reset_index(drop=True)


def _topic_schema_version(config: GeneratorConfig, topic: str) -> int:
    return int(config.kafka["topics"][topic]["schema_version"])


def _event_id(topic: str, event_type: str, sequence: int, timestamp: Any) -> str:
    topic_hint = topic.replace("_events", "")[:3].upper()
    event_hint = event_type.replace("_", "-").upper()[:24]
    return f"KEVT-{topic_hint}-{event_hint}-{pd.Timestamp(timestamp).strftime('%Y%m%d')}-{sequence:010d}"


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


def _iso(value: Any) -> str:
    timestamp = pd.Timestamp(value)
    if pd.isna(timestamp):
        timestamp = pd.Timestamp.utcnow()
    return timestamp.isoformat()
