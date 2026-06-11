from __future__ import annotations

import numpy as np
import pandas as pd

from vina_bim_shop.generators.config import GeneratorConfig
from vina_bim_shop.generators.ids import sequential_ids
from vina_bim_shop.generators.profiles import (
    CATEGORY_BRANDS,
    PRICE_BAND_MULTIPLIER,
    PRICE_RANGES,
    random_timestamps,
    weighted_choice,
)
from vina_bim_shop.generators.skew import acquisition_channel, preferred_device


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
    segment_values = weighted_choice(rng, segments, segment_weights, n)
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
            "preferred_device": preferred_device(rng, n),
            "acquisition_channel": acquisition_channel(rng, n),
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
    tier_values = weighted_choice(rng, tiers, tier_weights, n)
    city_idx = rng.choice(cities.index.to_numpy(), size=n, p=cities["weight"].to_numpy(dtype=float) / cities["weight"].sum())
    selected_cities = cities.loc[city_idx].reset_index(drop=True)
    category_values = weighted_choice(rng, list(config.category_weights), list(config.category_weights.values()), n)

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
    from vina_bim_shop.generators.schema_evolution import category_attributes
    from vina_bim_shop.generators.skew import fulfillment_channel

    n = config.entities["products"]
    seller_idx = rng.choice(sellers.index.to_numpy(), size=n)
    selected_sellers = sellers.loc[seller_idx].reset_index(drop=True)
    category_values = weighted_choice(rng, list(config.category_weights), list(config.category_weights.values()), n)
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

    created_ts = random_timestamps(rng, start_ts, end_ts, n)
    category_attributes_values = [
        category_attributes(rng, str(category), str(subcategory)) if timestamp >= cutoff_ts else None
        for category, subcategory, timestamp in zip(category_values, subcategory_values, created_ts)
    ]
    fulfillment_channels = np.where(
        pd.to_datetime(created_ts) >= cutoff_ts,
        fulfillment_channel(rng, n),
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
            "fulfillment_channel": fulfillment_channels,
            "category_attributes": category_attributes_values,
        }
    )


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


def generate_customers(
    config: GeneratorConfig,
    rng: np.random.Generator,
    cities: pd.DataFrame,
    start_ts: pd.Timestamp,
    end_ts: pd.Timestamp,
) -> pd.DataFrame:
    return _generate_customers(config, rng, cities, start_ts, end_ts)


def generate_sellers(
    config: GeneratorConfig,
    rng: np.random.Generator,
    cities: pd.DataFrame,
    start_ts: pd.Timestamp,
) -> pd.DataFrame:
    return _generate_sellers(config, rng, cities, start_ts)


def generate_products(
    config: GeneratorConfig,
    rng: np.random.Generator,
    sellers: pd.DataFrame,
    start_ts: pd.Timestamp,
    end_ts: pd.Timestamp,
    cutoff_ts: pd.Timestamp,
) -> pd.DataFrame:
    return _generate_products(config, rng, sellers, start_ts, end_ts, cutoff_ts)


def generate_product_category_map(
    config: GeneratorConfig,
    rng: np.random.Generator,
    products: pd.DataFrame,
    start_ts: pd.Timestamp,
) -> pd.DataFrame:
    return _generate_product_category_map(config, rng, products, start_ts)


__all__ = [
    "generate_customers",
    "generate_sellers",
    "generate_products",
    "generate_product_category_map",
]
