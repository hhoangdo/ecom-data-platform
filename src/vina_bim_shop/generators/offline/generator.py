from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import timedelta
from typing import Any

import numpy as np
import pandas as pd

from vina_bim_shop.generators.config import GeneratorConfig
from vina_bim_shop.generators.ids import dated_ids, sequential_ids


CATEGORY_BRANDS = {
    "FMCG": ["BimCare", "Saigon Fresh", "Mekong Pantry", "GlowVina", "PawJoy"],
    "ELHA": ["VinaTech", "BimDigital", "LotusHome", "SaigonSound", "SmartHue"],
    "Fashion": ["AoNha", "StreetLotus", "VinaWear", "ModaBim", "DenimSaigon"],
    "Home & Living": ["BepNha", "CozyVina", "NhaDep", "MekongHome", "LotusLiving"],
}

PRICE_RANGES = {
    "FMCG": (15000, 450000),
    "ELHA": (350000, 18000000),
    "Fashion": (70000, 1800000),
    "Home & Living": (45000, 4500000),
}

PRICE_BAND_MULTIPLIER = {
    "budget": 0.82,
    "value": 0.92,
    "mid": 1.0,
    "premium": 1.18,
}

PAYMENT_METHODS = ["cod", "e_wallet", "domestic_card", "bank_transfer", "installment"]
SHIPPING_METHODS = ["standard", "express", "same_day", "pickup_point"]


@dataclass
class OfflineGeneration:
    datasets: dict[str, pd.DataFrame]
    issue_records: list[dict[str, Any]]


def generate_offline(config: GeneratorConfig) -> OfflineGeneration:
    rng = np.random.default_rng(config.random_seed)
    start_ts, end_ts, cutoff_ts = _time_bounds(config)
    cities = pd.DataFrame(config.geography["cities"])

    customers = _generate_customers(config, rng, cities, start_ts, end_ts)
    sellers = _generate_sellers(config, rng, cities, start_ts)
    products = _generate_products(config, rng, sellers, start_ts, end_ts, cutoff_ts)
    product_category_map = _generate_product_category_map(config, rng, products, start_ts)
    inventory_snapshots = _generate_inventory_snapshots(config, rng, products, start_ts, end_ts)
    promotions = _generate_promotions(config, rng, sellers, start_ts, end_ts, cutoff_ts)
    orders = _generate_orders(config, rng, customers, products, promotions, start_ts, end_ts, cutoff_ts)
    order_items = _generate_order_items(config, rng, orders, products, promotions)
    orders = _attach_order_totals(orders, order_items)
    payments = _generate_payments(config, rng, orders)
    shipments = _generate_shipments(config, rng, orders, sellers)

    issue_records: list[dict[str, Any]] = []
    order_items, item_issues = _inject_order_item_duplicates(config, rng, order_items)
    issue_records.extend(item_issues)

    issue_records.extend(
        [
            _issue_record("products", "missing_brand", int(products["brand"].isna().sum()), float(products["brand"].isna().mean())),
            _issue_record(
                "orders",
                "missing_shipping_method",
                int(orders["shipping_method"].isna().sum()),
                float(orders["shipping_method"].isna().mean()),
            ),
            _issue_record(
                "products",
                "schema_evolution_category_attributes",
                int(products["category_attributes"].isna().sum()),
                float(products["category_attributes"].isna().mean()),
            ),
        ]
    )

    return OfflineGeneration(
        datasets={
            "customers": customers,
            "sellers": sellers,
            "products": products,
            "product_category_map": product_category_map,
            "inventory_snapshots": inventory_snapshots,
            "promotions": promotions,
            "orders": orders,
            "order_items": order_items,
            "payments": payments,
            "shipments": shipments,
        },
        issue_records=issue_records,
    )


def _time_bounds(config: GeneratorConfig) -> tuple[pd.Timestamp, pd.Timestamp, pd.Timestamp]:
    end_ts = pd.Timestamp(config.end_date).normalize() + pd.Timedelta(hours=23, minutes=59)
    start_ts = end_ts - pd.Timedelta(days=config.history_days - 1)
    cutoff_ratio = float(config.quality["schema_evolution_cutoff_ratio"])
    cutoff_ts = start_ts + pd.Timedelta(days=max(1, int(config.history_days * cutoff_ratio)))
    return start_ts, end_ts, cutoff_ts


def _weighted_choice(rng: np.random.Generator, labels: list[str], weights: list[float], size: int) -> np.ndarray:
    weights_array = np.asarray(weights, dtype=float)
    weights_array = weights_array / weights_array.sum()
    return rng.choice(labels, size=size, p=weights_array)


def _random_timestamps(
    rng: np.random.Generator,
    start_ts: pd.Timestamp,
    end_ts: pd.Timestamp,
    size: int,
    *,
    evening_bias: bool = False,
) -> pd.Series:
    days = rng.integers(0, max(1, (end_ts.normalize() - start_ts.normalize()).days + 1), size=size)
    if evening_bias:
        hourly_weights = np.array(
            [0.015, 0.01, 0.008, 0.006, 0.006, 0.01, 0.02, 0.035, 0.04, 0.04, 0.045, 0.05,
             0.07, 0.05, 0.045, 0.045, 0.055, 0.07, 0.085, 0.09, 0.09, 0.065, 0.04, 0.025],
            dtype=float,
        )
        hours = rng.choice(
            np.arange(24),
            size=size,
            p=hourly_weights / hourly_weights.sum(),
        )
    else:
        hours = rng.integers(0, 24, size=size)
    minutes = rng.integers(0, 60, size=size)
    seconds = rng.integers(0, 60, size=size)
    return pd.Series(start_ts + pd.to_timedelta(days, unit="D") + pd.to_timedelta(hours, unit="h") + pd.to_timedelta(minutes, unit="m") + pd.to_timedelta(seconds, unit="s"))


def _generate_customers(
    config: GeneratorConfig,
    rng: np.random.Generator,
    cities: pd.DataFrame,
    start_ts: pd.Timestamp,
    end_ts: pd.Timestamp,
) -> pd.DataFrame:
    n = config.entities["customers"]
    city_weights = cities["weight"].to_numpy(dtype=float)
    city_idx = rng.choice(cities.index.to_numpy(), size=n, p=city_weights / city_weights.sum())
    selected_cities = cities.loc[city_idx].reset_index(drop=True)

    segments = list(config.customer_segments)
    segment_weights = [config.customer_segments[item]["weight"] for item in segments]
    segment_values = _weighted_choice(rng, segments, segment_weights, n)
    signup_offsets = (rng.beta(2.2, 1.4, size=n) * (config.history_days + 365)).astype(int)
    signup_ts = end_ts - pd.to_timedelta(signup_offsets, unit="D") + pd.to_timedelta(rng.integers(0, 86400, n), unit="s")
    created_ts = signup_ts + pd.to_timedelta(rng.integers(0, 3600, n), unit="s")

    sequence = pd.Series(np.arange(1, n + 1))
    customer_id = sequential_ids("CUS", selected_cities["city_code"], sequence)
    return pd.DataFrame(
        {
            "customer_id": customer_id,
            "anonymous_id": "ANON-" + sequence.astype(str).str.zfill(8),
            "signup_ts": signup_ts,
            "created_ts": created_ts,
            "country": "VN",
            "region": selected_cities["region"],
            "city": selected_cities["city"],
            "city_code": selected_cities["city_code"],
            "segment": segment_values,
            "marketing_opt_in": rng.random(n) < 0.62,
            "preferred_device": _weighted_choice(rng, ["app_android", "app_ios", "mobile_web", "desktop_web"], [0.55, 0.23, 0.16, 0.06], n),
            "acquisition_channel": _weighted_choice(rng, ["organic", "ads", "referral", "affiliate"], [0.48, 0.32, 0.13, 0.07], n),
        }
    )


def _generate_sellers(
    config: GeneratorConfig,
    rng: np.random.Generator,
    cities: pd.DataFrame,
    start_ts: pd.Timestamp,
) -> pd.DataFrame:
    n = config.entities["sellers"]
    tiers = list(config.seller_tiers)
    tier_weights = [config.seller_tiers[item]["weight"] for item in tiers]
    tier_values = _weighted_choice(rng, tiers, tier_weights, n)
    city_idx = rng.choice(cities.index.to_numpy(), size=n, p=cities["weight"].to_numpy(dtype=float) / cities["weight"].sum())
    selected_cities = cities.loc[city_idx].reset_index(drop=True)
    category_values = _weighted_choice(rng, list(config.category_weights), list(config.category_weights.values()), n)

    ratings = []
    fulfillment_days = []
    inventory_reliability = []
    price_bands = []
    for tier in tier_values:
        tier_config = config.seller_tiers[str(tier)]
        ratings.append(np.clip(rng.normal(tier_config["rating_mean"], 0.16), 3.2, 5.0))
        fulfillment_days.append(max(1.0, rng.normal(tier_config["fulfillment_days_mean"], 0.5)))
        inventory_reliability.append(np.clip(rng.normal(tier_config["inventory_reliability_mean"], 0.035), 0.55, 0.995))
        price_bands.append(tier_config["price_band"])

    sequence = pd.Series(np.arange(1, n + 1))
    seller_id = sequential_ids("SEL", selected_cities["city_code"] + "-" + pd.Series(tier_values).str[:3].str.upper(), sequence)
    return pd.DataFrame(
        {
            "seller_id": seller_id,
            "seller_name": "Seller " + sequence.astype(str).str.zfill(5),
            "seller_tier": tier_values,
            "primary_category": category_values,
            "city": selected_cities["city"],
            "region": selected_cities["region"],
            "seller_rating": np.round(ratings, 2),
            "fulfillment_speed_days": np.round(fulfillment_days, 2),
            "inventory_reliability": np.round(inventory_reliability, 3),
            "price_band": price_bands,
            "is_official_store": pd.Series(tier_values).eq("official").to_numpy(),
            "created_ts": start_ts - pd.to_timedelta(rng.integers(15, 540, n), unit="D"),
        }
    )


def _generate_products(
    config: GeneratorConfig,
    rng: np.random.Generator,
    sellers: pd.DataFrame,
    start_ts: pd.Timestamp,
    end_ts: pd.Timestamp,
    cutoff_ts: pd.Timestamp,
) -> pd.DataFrame:
    n = config.entities["products"]
    seller_idx = rng.choice(sellers.index.to_numpy(), size=n)
    selected_sellers = sellers.loc[seller_idx].reset_index(drop=True)
    category_values = _weighted_choice(rng, list(config.category_weights), list(config.category_weights.values()), n)
    subcategory_values = [
        rng.choice(config.taxonomy.subcategories[str(category)])
        for category in category_values
    ]
    brands = np.array([rng.choice(CATEGORY_BRANDS[str(category)]) for category in category_values], dtype=object)

    missing_brand_rate = float(config.quality["missing_brand_rate"])
    brands[rng.random(n) < missing_brand_rate] = None

    base_prices = []
    for category, price_band in zip(category_values, selected_sellers["price_band"]):
        low, high = PRICE_RANGES[str(category)]
        value = rng.lognormal(mean=np.log((low + high) / 7), sigma=0.8)
        value = np.clip(value, low, high) * PRICE_BAND_MULTIPLIER[str(price_band)]
        base_prices.append(round(float(value) / 1000) * 1000)

    created_ts = _random_timestamps(rng, start_ts, end_ts, n)
    category_attributes = [
        _category_attributes(rng, str(category), str(subcategory)) if timestamp >= cutoff_ts else None
        for category, subcategory, timestamp in zip(category_values, subcategory_values, created_ts)
    ]
    fulfillment_channel = np.where(
        pd.to_datetime(created_ts) >= cutoff_ts,
        _weighted_choice(rng, ["seller_fulfilled", "platform_fulfilled", "cross_dock"], [0.62, 0.30, 0.08], n),
        None,
    )

    sequence = pd.Series(np.arange(1, n + 1))
    product_id = sequential_ids("PRD", pd.Series(category_values), sequence)
    return pd.DataFrame(
        {
            "product_id": product_id,
            "seller_id": selected_sellers["seller_id"],
            "primary_category": category_values,
            "primary_subcategory": subcategory_values,
            "brand": brands,
            "product_name": pd.Series(subcategory_values).astype(str) + " Item " + sequence.astype(str).str.zfill(6),
            "base_price": pd.Series(base_prices).astype(float),
            "price_band": selected_sellers["price_band"],
            "is_active": rng.random(n) > 0.04,
            "created_ts": created_ts,
            "fulfillment_channel": fulfillment_channel,
            "category_attributes": category_attributes,
        }
    )


def _category_attributes(rng: np.random.Generator, category: str, subcategory: str) -> str:
    if category == "FMCG":
        payload = {
            "pack_size": rng.choice(["single", "bundle_3", "family_pack"]),
            "shelf_life_days": int(rng.integers(90, 720)),
            "variant": rng.choice(["standard", "sensitive", "premium"]),
        }
    elif category == "ELHA":
        payload = {
            "warranty_months": int(rng.choice([6, 12, 18, 24])),
            "energy_rating": rng.choice(["A", "A+", "A++", "not_applicable"]),
            "spec_tier": rng.choice(["entry", "mid", "flagship"]),
        }
    elif category == "Fashion":
        payload = {
            "size_family": rng.choice(["alpha", "numeric", "free_size"]),
            "material": rng.choice(["cotton", "polyester", "denim", "synthetic_leather"]),
            "style": rng.choice(["casual", "office", "streetwear"]),
        }
    else:
        payload = {
            "room_type": rng.choice(["kitchen", "bedroom", "living_room", "bathroom"]),
            "assembly_required": bool(rng.random() < 0.35),
            "material": rng.choice(["wood", "steel", "ceramic", "fabric"]),
        }
    payload["subcategory"] = subcategory
    return json.dumps(payload, sort_keys=True)


def _generate_product_category_map(
    config: GeneratorConfig,
    rng: np.random.Generator,
    products: pd.DataFrame,
    start_ts: pd.Timestamp,
) -> pd.DataFrame:
    rows = []
    for row in products[["product_id", "primary_category", "primary_subcategory"]].itertuples(index=False):
        rows.append(
            {
                "product_id": row.product_id,
                "category": row.primary_category,
                "subcategory": row.primary_subcategory,
                "is_primary": True,
                "assigned_ts": start_ts,
            }
        )
        if rng.random() < 0.22:
            choices = [item for item in config.taxonomy.subcategories[row.primary_category] if item != row.primary_subcategory]
            if choices:
                rows.append(
                    {
                        "product_id": row.product_id,
                        "category": row.primary_category,
                        "subcategory": str(rng.choice(choices)),
                        "is_primary": False,
                        "assigned_ts": start_ts + pd.Timedelta(days=int(rng.integers(0, max(1, config.history_days)))),
                    }
                )
    return pd.DataFrame(rows)


def _generate_inventory_snapshots(
    config: GeneratorConfig,
    rng: np.random.Generator,
    products: pd.DataFrame,
    start_ts: pd.Timestamp,
    end_ts: pd.Timestamp,
) -> pd.DataFrame:
    snapshot_days = pd.date_range(start_ts.normalize(), end_ts.normalize(), freq="7D")
    if len(snapshot_days) == 0 or snapshot_days[-1] != end_ts.normalize():
        snapshot_days = snapshot_days.append(pd.DatetimeIndex([end_ts.normalize()]))
    frames = []
    for snapshot_ts in snapshot_days:
        stock = rng.poisson(lam=np.where(products["primary_category"].eq("FMCG"), 180, 55))
        frames.append(
            pd.DataFrame(
                {
                    "snapshot_id": "INV-" + snapshot_ts.strftime("%Y%m%d") + "-" + pd.Series(np.arange(1, len(products) + 1)).astype(str).str.zfill(8),
                    "product_id": products["product_id"].to_numpy(),
                    "seller_id": products["seller_id"].to_numpy(),
                    "snapshot_ts": snapshot_ts,
                    "stock_on_hand": stock,
                    "reserved_stock": rng.binomial(np.maximum(stock, 1), 0.08),
                }
            )
        )
    return pd.concat(frames, ignore_index=True)


def _generate_promotions(
    config: GeneratorConfig,
    rng: np.random.Generator,
    sellers: pd.DataFrame,
    start_ts: pd.Timestamp,
    end_ts: pd.Timestamp,
    cutoff_ts: pd.Timestamp,
) -> pd.DataFrame:
    n = config.entities["promotions"]
    funding_type = _weighted_choice(rng, ["platform", "seller", "mixed"], [0.42, 0.36, 0.22], n)
    category_values = _weighted_choice(rng, list(config.category_weights), list(config.category_weights.values()), n)
    seller_values = np.where(
        pd.Series(funding_type).isin(["seller", "mixed"]),
        rng.choice(sellers["seller_id"], size=n),
        None,
    )
    start_values = _random_timestamps(rng, start_ts, end_ts - pd.Timedelta(days=2), n)
    duration_days = rng.integers(2, 14, size=n)
    end_values = start_values + pd.to_timedelta(duration_days, unit="D")
    discount_rate = np.round(rng.uniform(0.04, 0.28, size=n), 3)
    funding_detail = [
        json.dumps({"platform_share": 1.0, "seller_share": 0.0})
        if item == "platform"
        else json.dumps({"platform_share": 0.0, "seller_share": 1.0})
        if item == "seller"
        else json.dumps({"platform_share": 0.5, "seller_share": 0.5})
        for item in funding_type
    ]
    funding_detail = [value if start >= cutoff_ts else None for value, start in zip(funding_detail, start_values)]

    sequence = pd.Series(np.arange(1, n + 1))
    return pd.DataFrame(
        {
            "promotion_id": sequential_ids("PRM", pd.Series(category_values), sequence),
            "promotion_name": pd.Series(funding_type).str.title() + " " + pd.Series(category_values) + " Campaign",
            "funding_type": funding_type,
            "funding_detail": funding_detail,
            "seller_id": seller_values,
            "category": category_values,
            "discount_rate": discount_rate,
            "promotion_start_ts": start_values,
            "promotion_end_ts": end_values,
            "created_ts": start_values - pd.to_timedelta(rng.integers(1, 7, n), unit="D"),
        }
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
    customer_weights = customers["segment"].map(
        {
            "budget_shopper": 0.95,
            "loyal_fmcg_repeat_buyer": 1.7,
            "occasional_high_value_elha_buyer": 0.65,
            "fashion_browser": 0.85,
            "home_improver": 0.75,
        }
    ).to_numpy(dtype=float)
    customer_idx = rng.choice(customers.index.to_numpy(), size=n, p=customer_weights / customer_weights.sum())
    selected_customers = customers.loc[customer_idx].reset_index(drop=True)

    order_ts = _random_timestamps(rng, start_ts, end_ts, n, evening_bias=True)
    preferred_categories = selected_customers["segment"].map(
        {segment: config.customer_segments[segment]["preferred_category"] for segment in config.customer_segments}
    )
    category_values = []
    for preferred in preferred_categories:
        if rng.random() < 0.63:
            category_values.append(preferred)
        else:
            category_values.append(rng.choice(list(config.category_weights), p=list(config.category_weights.values())))
    category_values = pd.Series(category_values)

    coupon_affinity = selected_customers["segment"].map(
        {segment: config.customer_segments[segment]["coupon_affinity"] for segment in config.customer_segments}
    ).to_numpy(dtype=float)
    has_coupon = rng.random(n) < (0.18 + 0.35 * coupon_affinity)
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

    shipping_method = _weighted_choice(rng, SHIPPING_METHODS, [0.62, 0.24, 0.08, 0.06], n).astype(object)
    missing_shipping = rng.random(n) < float(config.quality["missing_shipping_method_rate"])
    shipping_method[missing_shipping] = None
    fulfillment_channel = np.where(pd.to_datetime(order_ts) >= cutoff_ts, _weighted_choice(rng, ["seller_fulfilled", "platform_fulfilled", "cross_dock"], [0.64, 0.29, 0.07], n), None)

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
            "shipping_method": shipping_method,
            "fulfillment_channel": fulfillment_channel,
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


def _generate_payments(config: GeneratorConfig, rng: np.random.Generator, orders: pd.DataFrame) -> pd.DataFrame:
    n = len(orders)
    failure_rates = {
        segment: values["payment_failure_rate"]
        for segment, values in config.customer_segments.items()
    }
    # Segment is not on orders yet, so use category-level proxy plus a base rate.
    category_failure = orders["primary_category"].map({"FMCG": 0.026, "ELHA": 0.038, "Fashion": 0.045, "Home & Living": 0.032}).fillna(0.035)
    failed = rng.random(n) < category_failure.to_numpy(dtype=float)
    payment_ts = pd.to_datetime(orders["order_timestamp"]) + pd.to_timedelta(rng.integers(1, 30, n), unit="m")
    payment_status = np.where(failed, "failed", "success")
    order_status = np.where(failed, "payment_failed", "paid")
    orders.loc[:, "status"] = order_status
    return pd.DataFrame(
        {
            "payment_id": dated_ids("PAY", orders["shipping_city"].str[:3].str.upper(), payment_ts, pd.Series(np.arange(1, n + 1))),
            "order_id": orders["order_id"].to_numpy(),
            "customer_id": orders["customer_id"].to_numpy(),
            "payment_timestamp": payment_ts,
            "created_ts": payment_ts + pd.to_timedelta(rng.integers(5, 180, n), unit="s"),
            "payment_method": _weighted_choice(rng, PAYMENT_METHODS, [0.36, 0.34, 0.18, 0.09, 0.03], n),
            "amount": orders["order_net_amount"].round(2).to_numpy(),
            "payment_status": payment_status,
            "failure_reason": np.where(failed, _weighted_choice(rng, ["insufficient_funds", "provider_timeout", "auth_failed"], [0.52, 0.30, 0.18], n), None),
        }
    )


def _generate_shipments(
    config: GeneratorConfig,
    rng: np.random.Generator,
    orders: pd.DataFrame,
    sellers: pd.DataFrame,
) -> pd.DataFrame:
    n = len(orders)
    paid = orders["status"].eq("paid").to_numpy()
    order_ts = pd.to_datetime(orders["order_timestamp"])
    handoff_ts = order_ts + pd.to_timedelta(rng.integers(6, 72, n), unit="h")
    delivery_ts = handoff_ts + pd.to_timedelta(rng.integers(1, 5, n), unit="D")
    shipment_status = np.where(paid, _weighted_choice(rng, ["delivered", "in_transit", "delayed"], [0.82, 0.13, 0.05], n), "blocked_payment_failed")
    return pd.DataFrame(
        {
            "shipment_id": dated_ids("SHP", orders["shipping_city"].str[:3].str.upper(), order_ts, pd.Series(np.arange(1, n + 1))),
            "order_id": orders["order_id"].to_numpy(),
            "customer_id": orders["customer_id"].to_numpy(),
            "shipping_city": orders["shipping_city"].to_numpy(),
            "shipping_region": orders["shipping_region"].to_numpy(),
            "shipping_method": orders["shipping_method"].to_numpy(),
            "shipment_status": shipment_status,
            "handoff_ts": np.where(paid, handoff_ts, pd.NaT),
            "estimated_delivery_ts": np.where(paid, delivery_ts, pd.NaT),
            "created_ts": order_ts + pd.to_timedelta(rng.integers(35, 600, n), unit="s"),
        }
    )


def _inject_order_item_duplicates(
    config: GeneratorConfig,
    rng: np.random.Generator,
    order_items: pd.DataFrame,
) -> tuple[pd.DataFrame, list[dict[str, Any]]]:
    duplicate_rate = float(config.quality["offline_duplicate_rate"])
    n_dupes = max(1, int(len(order_items) * duplicate_rate))
    duplicate_rows = order_items.iloc[rng.choice(order_items.index.to_numpy(), size=n_dupes, replace=False)].copy()
    output = pd.concat([order_items, duplicate_rows], ignore_index=True)
    return output, [_issue_record("order_items", "exact_duplicate_payload", n_dupes, n_dupes / len(output))]


def _issue_record(dataset: str, issue_type: str, affected_rows: int, observed_rate: float) -> dict[str, Any]:
    return {
        "dataset": dataset,
        "issue_type": issue_type,
        "affected_rows": int(affected_rows),
        "observed_rate": round(float(observed_rate), 5),
    }
