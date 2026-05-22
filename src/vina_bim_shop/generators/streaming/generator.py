from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import numpy as np
import pandas as pd

from vina_bim_shop.generators.config import GeneratorConfig
from vina_bim_shop.generators.ids import dated_ids


@dataclass
class StreamingGeneration:
    stream_events: pd.DataFrame
    issue_records: list[dict[str, Any]]


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
    return StreamingGeneration(stream_events=events, issue_records=issues)


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
