from __future__ import annotations

import numpy as np
import pandas as pd

from vina_bim_shop.generators.config import GeneratorConfig
from vina_bim_shop.generators.ids import dated_ids
from vina_bim_shop.generators.profiles import random_timestamps, weighted_choice
from vina_bim_shop.generators.skew import (
    coupon_affinity,
    customer_order_weights,
    fulfillment_channel_for_orders,
    segment_affinity_to_categories,
    shipping_method,
)


def _generate_orders(
    config: GeneratorConfig,
    rng: np.random.Generator,
    customers: pd.DataFrame,
    products: pd.DataFrame,
    promotions: pd.DataFrame,
    start_ts: pd.Timestamp,
    end_ts: pd.Timestamp,
    cutoff_ts: pd.Timestamp,
) -> pd.DataFrame:
    n = config.entities["orders"]
    customer_weights = customer_order_weights(customers)
    customer_idx = rng.choice(customers.index.to_numpy(), size=n, p=customer_weights / customer_weights.sum())
    selected_customers = customers.loc[customer_idx].reset_index(drop=True)

    order_ts = random_timestamps(rng, start_ts, end_ts, n, evening_bias=True)
    preferred_categories = segment_affinity_to_categories(selected_customers, config)
    category_values = []
    for preferred in preferred_categories:
        if rng.random() < 0.63:
            category_values.append(preferred)
        else:
            category_values.append(rng.choice(list(config.category_weights), p=list(config.category_weights.values())))
    category_values = pd.Series(category_values)

    coupon_affinities = coupon_affinity(selected_customers, config)
    has_coupon = rng.random(n) < (0.18 + 0.35 * coupon_affinities)
    promotion_ids = []
    coupon_codes = []
    for category, timestamp, coupon in zip(category_values, order_ts, has_coupon):
        active = promotions[
            (promotions["category"].eq(category))
            & (promotions["promotion_start_ts"].le(timestamp))
            & (promotions["promotion_end_ts"].ge(timestamp))
        ]
        if coupon and len(active) > 0:
            promotion_id = str(active.sample(n=1, random_state=int(rng.integers(0, 1_000_000)))["promotion_id"].iloc[0])
            promotion_ids.append(promotion_id)
            coupon_codes.append("VBS-" + promotion_id[-8:])
        else:
            promotion_ids.append(None)
            coupon_codes.append(None)

    shipping_methods = shipping_method(rng, n).astype(object)
    missing_shipping = rng.random(n) < float(config.quality["missing_shipping_method_rate"])
    shipping_methods[missing_shipping] = None
    fulfillment_channels = np.where(
        pd.to_datetime(order_ts) >= cutoff_ts,
        fulfillment_channel_for_orders(rng, n),
        None,
    )

    sequence = pd.Series(np.arange(1, n + 1))
    order_id = dated_ids("ORD", selected_customers["city_code"], order_ts, sequence)
    return pd.DataFrame(
        {
            "order_id": order_id,
            "customer_id": selected_customers["customer_id"],
            "session_id": dated_ids("SES", selected_customers["city_code"], order_ts, sequence),
            "anonymous_id": selected_customers["anonymous_id"],
            "order_timestamp": order_ts,
            "created_ts": order_ts + pd.to_timedelta(rng.integers(5, 240, n), unit="s"),
            "order_date": pd.to_datetime(order_ts).dt.date.astype(str),
            "primary_category": category_values,
            "status": "pending",
            "shipping_city": selected_customers["city"],
            "shipping_region": selected_customers["region"],
            "shipping_method": shipping_methods,
            "fulfillment_channel": fulfillment_channels,
            "promotion_id": promotion_ids,
            "coupon_code": coupon_codes,
        }
    )


def _generate_order_items(
    config: GeneratorConfig,
    rng: np.random.Generator,
    orders: pd.DataFrame,
    products: pd.DataFrame,
    promotions: pd.DataFrame,
) -> pd.DataFrame:
    products_by_category = {category: frame.reset_index(drop=True) for category, frame in products.groupby("primary_category")}
    promotion_lookup = promotions.set_index("promotion_id")["discount_rate"].to_dict()
    rows = []
    item_seq = 1
    segment_avg_items = {segment: values["avg_items"] for segment, values in config.customer_segments.items()}
    for order in orders.itertuples(index=False):
        avg_items = 2.4
        if order.primary_category == "FMCG":
            avg_items = 4.3
        elif order.primary_category == "ELHA":
            avg_items = 1.3
        elif order.primary_category == "Fashion":
            avg_items = 2.2
        elif order.primary_category == "Home & Living":
            avg_items = 2.0
        n_items = int(np.clip(rng.poisson(avg_items - 0.5) + 1, 1, 8))
        product_pool = products_by_category[str(order.primary_category)]
        selected = product_pool.iloc[rng.choice(product_pool.index.to_numpy(), size=n_items, replace=len(product_pool) < n_items)]
        for product in selected.itertuples(index=False):
            quantity = int(np.clip(rng.poisson(2 if order.primary_category == "FMCG" else 1) + 1, 1, 12))
            discount_rate = float(promotion_lookup.get(order.promotion_id, 0.0) or 0.0)
            unit_price = float(product.base_price)
            discount_amount = round(unit_price * quantity * discount_rate, 2)
            rows.append(
                {
                    "order_item_id": "ITM-" + str(item_seq).zfill(10),
                    "order_id": order.order_id,
                    "product_id": product.product_id,
                    "seller_id": product.seller_id,
                    "primary_category": product.primary_category,
                    "primary_subcategory": product.primary_subcategory,
                    "quantity": quantity,
                    "unit_price": unit_price,
                    "gross_amount": round(unit_price * quantity, 2),
                    "discount_amount": discount_amount,
                    "net_amount": round(unit_price * quantity - discount_amount, 2),
                    "promotion_id": order.promotion_id,
                    "created_ts": order.created_ts,
                }
            )
            item_seq += 1
    return pd.DataFrame(rows)


def _attach_order_totals(orders: pd.DataFrame, order_items: pd.DataFrame) -> pd.DataFrame:
    totals = order_items.groupby("order_id").agg(
        item_count=("order_item_id", "count"),
        order_gross_amount=("gross_amount", "sum"),
        order_discount_amount=("discount_amount", "sum"),
        order_net_amount=("net_amount", "sum"),
    )
    result = orders.merge(totals, left_on="order_id", right_index=True, how="left")
    result[["item_count", "order_gross_amount", "order_discount_amount", "order_net_amount"]] = result[
        ["item_count", "order_gross_amount", "order_discount_amount", "order_net_amount"]
    ].fillna(0)
    return result


def generate_orders(
    config: GeneratorConfig,
    rng: np.random.Generator,
    customers: pd.DataFrame,
    products: pd.DataFrame,
    promotions: pd.DataFrame,
    start_ts: pd.Timestamp,
    end_ts: pd.Timestamp,
    cutoff_ts: pd.Timestamp,
) -> pd.DataFrame:
    return _generate_orders(config, rng, customers, products, promotions, start_ts, end_ts, cutoff_ts)


def generate_order_items(
    config: GeneratorConfig,
    rng: np.random.Generator,
    orders: pd.DataFrame,
    products: pd.DataFrame,
    promotions: pd.DataFrame,
) -> pd.DataFrame:
    return _generate_order_items(config, rng, orders, products, promotions)


def attach_order_totals(orders: pd.DataFrame, order_items: pd.DataFrame) -> pd.DataFrame:
    return _attach_order_totals(orders, order_items)


__all__ = ["generate_orders", "generate_order_items", "attach_order_totals"]
