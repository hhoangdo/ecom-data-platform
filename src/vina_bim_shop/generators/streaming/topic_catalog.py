from __future__ import annotations

from typing import Any

import pandas as pd

from vina_bim_shop.generators.config import GeneratorConfig
from vina_bim_shop.generators.streaming.envelope import (
    envelope,
    event_id,
    json_value,
    payload_dict,
    topic_frame,
)


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
            envelope(
                config,
                topic,
                "product_created",
                event_id(topic, "product_created", seq, product.created_ts),
                product.created_ts,
                product.created_ts,
                {"product_id": product.product_id, "seller_id": product.seller_id},
                {
                    "product_name": json_value(product.product_name),
                    "primary_category": json_value(product.primary_category),
                    "primary_subcategory": json_value(product.primary_subcategory),
                    "brand": json_value(product.brand),
                    "base_price": json_value(product.base_price),
                    "category_attributes": payload_dict(product.category_attributes),
                },
            )
        )

    updated_products = products[products["category_attributes"].notna()].head(max(1, int(len(products) * 0.05)))
    if updated_products.empty:
        updated_products = products.head(1)
    for seq, product in enumerate(updated_products.itertuples(index=False), start=1):
        updated_ts = pd.Timestamp(product.created_ts) + pd.Timedelta(days=1)
        rows.append(
            envelope(
                config,
                topic,
                "product_updated",
                event_id(topic, "product_updated", seq, updated_ts),
                updated_ts,
                updated_ts,
                {"product_id": product.product_id, "seller_id": product.seller_id},
                {"changed_fields": ["category_attributes", "fulfillment_channel"], "fulfillment_channel": json_value(product.fulfillment_channel)},
            )
        )
        rows.append(
            envelope(
                config,
                topic,
                "price_changed",
                event_id(topic, "price_changed", seq, updated_ts + pd.Timedelta(minutes=5)),
                updated_ts + pd.Timedelta(minutes=5),
                updated_ts + pd.Timedelta(minutes=5),
                {"product_id": product.product_id, "seller_id": product.seller_id},
                {"old_price": round(float(product.base_price) * 1.05, 2), "new_price": json_value(product.base_price)},
            )
        )

    for seq, snapshot in enumerate(inventory_snapshots.itertuples(index=False), start=1):
        rows.append(
            envelope(
                config,
                topic,
                "inventory_snapshot",
                event_id(topic, "inventory_snapshot", seq, snapshot.snapshot_ts),
                snapshot.snapshot_ts,
                snapshot.snapshot_ts,
                {"snapshot_id": snapshot.snapshot_id, "product_id": snapshot.product_id, "seller_id": snapshot.seller_id},
                {"stock_on_hand": json_value(snapshot.stock_on_hand), "reserved_stock": json_value(snapshot.reserved_stock)},
            )
        )

    low_stock_cutoff = inventory_snapshots["stock_on_hand"].quantile(0.08)
    low_stock = inventory_snapshots[inventory_snapshots["stock_on_hand"].le(low_stock_cutoff)].head(max(1, int(len(inventory_snapshots) * 0.04)))
    if low_stock.empty:
        low_stock = inventory_snapshots.nsmallest(1, "stock_on_hand")
    for seq, snapshot in enumerate(low_stock.itertuples(index=False), start=1):
        rows.append(
            envelope(
                config,
                topic,
                "inventory_low_stock",
                event_id(topic, "inventory_low_stock", seq, snapshot.snapshot_ts),
                snapshot.snapshot_ts,
                snapshot.snapshot_ts,
                {"snapshot_id": snapshot.snapshot_id, "product_id": snapshot.product_id, "seller_id": snapshot.seller_id},
                {"stock_on_hand": json_value(snapshot.stock_on_hand), "threshold": json_value(low_stock_cutoff)},
            )
        )

    for seq, promotion in enumerate(promotions.itertuples(index=False), start=1):
        rows.append(
            envelope(
                config,
                topic,
                "promotion_created",
                event_id(topic, "promotion_created", seq, promotion.created_ts),
                promotion.created_ts,
                promotion.created_ts,
                {"promotion_id": promotion.promotion_id, "seller_id": promotion.seller_id},
                {
                    "promotion_name": json_value(promotion.promotion_name),
                    "funding_type": json_value(promotion.funding_type),
                    "funding_detail": payload_dict(promotion.funding_detail),
                    "category": json_value(promotion.category),
                    "discount_rate": json_value(promotion.discount_rate),
                },
            )
        )
        rows.append(
            envelope(
                config,
                topic,
                "promotion_activated",
                event_id(topic, "promotion_activated", seq, promotion.promotion_start_ts),
                promotion.promotion_start_ts,
                promotion.promotion_start_ts,
                {"promotion_id": promotion.promotion_id, "seller_id": promotion.seller_id},
                {"promotion_end_ts": json_value(promotion.promotion_end_ts), "category": json_value(promotion.category)},
            )
        )

    return topic_frame(rows)


def catalog_topic_events(
    config: GeneratorConfig,
    products: pd.DataFrame,
    inventory_snapshots: pd.DataFrame,
    promotions: pd.DataFrame,
) -> pd.DataFrame:
    return _catalog_topic_events(config, products, inventory_snapshots, promotions)


__all__ = ["catalog_topic_events"]
