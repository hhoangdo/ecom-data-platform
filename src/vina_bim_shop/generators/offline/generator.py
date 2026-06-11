"""Offline source data generation.

The legacy module exposed a single ``generate_offline`` entry point backed by a
flat list of private helpers.  The helpers now live in dedicated submodules
grouped by responsibility.  This file remains as a thin public surface and a
backwards-compatible re-export of every symbol that used to be defined here, so
that any ``from vina_bim_shop.generators.offline.generator import ...`` import
keeps working.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import numpy as np
import pandas as pd

from vina_bim_shop.generators.config import GeneratorConfig
from vina_bim_shop.generators.duplicates import inject_order_item_duplicates as _inject_order_item_duplicates
from vina_bim_shop.generators.duplicates import issue_record
from vina_bim_shop.generators.offline.entities import (
    _generate_customers,
    _generate_product_category_map,
    _generate_products,
    _generate_sellers,
)
from vina_bim_shop.generators.offline.fulfillment import (
    _generate_payments,
    _generate_shipments,
)
from vina_bim_shop.generators.offline.inventory_promos import (
    _generate_inventory_snapshots,
    _generate_promotions,
)
from vina_bim_shop.generators.offline.orders import (
    _attach_order_totals,
    _generate_order_items,
    _generate_orders,
)
from vina_bim_shop.generators.profiles import (
    CATEGORY_BRANDS,
    COMMERCE_EVENT_TYPE_MAP,
    PAYMENT_METHODS,
    PRICE_BAND_MULTIPLIER,
    PRICE_RANGES,
    SHIPPING_METHODS,
    random_timestamps,
    time_bounds,
    weighted_choice,
)


@dataclass
class OfflineGeneration:
    datasets: dict[str, pd.DataFrame]
    issue_records: list[dict[str, Any]]


def generate_offline(config: GeneratorConfig) -> OfflineGeneration:
    rng = np.random.default_rng(config.random_seed)
    start_ts, end_ts, cutoff_ts = time_bounds(config)
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
            issue_record("products", "missing_brand", int(products["brand"].isna().sum()), float(products["brand"].isna().mean())),
            issue_record(
                "orders",
                "missing_shipping_method",
                int(orders["shipping_method"].isna().sum()),
                float(orders["shipping_method"].isna().mean()),
            ),
            issue_record(
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


__all__ = [
    "CATEGORY_BRANDS",
    "COMMERCE_EVENT_TYPE_MAP",
    "OfflineGeneration",
    "PAYMENT_METHODS",
    "PRICE_BAND_MULTIPLIER",
    "PRICE_RANGES",
    "SHIPPING_METHODS",
    "_attach_order_totals",
    "_generate_customers",
    "_generate_inventory_snapshots",
    "_generate_order_items",
    "_generate_orders",
    "_generate_payments",
    "_generate_product_category_map",
    "_generate_products",
    "_generate_promotions",
    "_generate_sellers",
    "_generate_shipments",
    "_inject_order_item_duplicates",
    "_issue_record",
    "_random_timestamps",
    "_time_bounds",
    "_weighted_choice",
    "generate_offline",
    "issue_record",
    "random_timestamps",
    "time_bounds",
    "weighted_choice",
]


_issue_record = issue_record
_random_timestamps = random_timestamps
_time_bounds = time_bounds
_weighted_choice = weighted_choice
