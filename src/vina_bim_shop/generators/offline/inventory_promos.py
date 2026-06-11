from __future__ import annotations

import numpy as np
import pandas as pd

from vina_bim_shop.generators.config import GeneratorConfig
from vina_bim_shop.generators.profiles import random_timestamps, weighted_choice
from vina_bim_shop.generators.skew import promotion_funding_type


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
    import json

    from vina_bim_shop.generators.ids import sequential_ids

    n = config.entities["promotions"]
    funding_type = promotion_funding_type(rng, n)
    category_values = weighted_choice(rng, list(config.category_weights), list(config.category_weights.values()), n)
    seller_values = np.where(
        pd.Series(funding_type).isin(["seller", "mixed"]),
        rng.choice(sellers["seller_id"], size=n),
        None,
    )
    start_values = random_timestamps(rng, start_ts, end_ts - pd.Timedelta(days=2), n)
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


def generate_inventory_snapshots(
    config: GeneratorConfig,
    rng: np.random.Generator,
    products: pd.DataFrame,
    start_ts: pd.Timestamp,
    end_ts: pd.Timestamp,
) -> pd.DataFrame:
    return _generate_inventory_snapshots(config, rng, products, start_ts, end_ts)


def generate_promotions(
    config: GeneratorConfig,
    rng: np.random.Generator,
    sellers: pd.DataFrame,
    start_ts: pd.Timestamp,
    end_ts: pd.Timestamp,
    cutoff_ts: pd.Timestamp,
) -> pd.DataFrame:
    return _generate_promotions(config, rng, sellers, start_ts, end_ts, cutoff_ts)


__all__ = ["generate_inventory_snapshots", "generate_promotions"]
