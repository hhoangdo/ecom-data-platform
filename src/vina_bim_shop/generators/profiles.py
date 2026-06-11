from __future__ import annotations

import numpy as np
import pandas as pd

from vina_bim_shop.generators.config import GeneratorConfig


CATEGORY_BRANDS: dict[str, list[str]] = {
    "FMCG": ["BimCare", "Saigon Fresh", "Mekong Pantry", "GlowVina", "PawJoy"],
    "ELHA": ["VinaTech", "BimDigital", "LotusHome", "SaigonSound", "SmartHue"],
    "Fashion": ["AoNha", "StreetLotus", "VinaWear", "ModaBim", "DenimSaigon"],
    "Home & Living": ["BepNha", "CozyVina", "NhaDep", "MekongHome", "LotusLiving"],
}

PRICE_RANGES: dict[str, tuple[int, int]] = {
    "FMCG": (15000, 450000),
    "ELHA": (350000, 18000000),
    "Fashion": (70000, 1800000),
    "Home & Living": (45000, 4500000),
}

PRICE_BAND_MULTIPLIER: dict[str, float] = {
    "budget": 0.82,
    "value": 0.92,
    "mid": 1.0,
    "premium": 1.18,
}

PAYMENT_METHODS: list[str] = ["cod", "e_wallet", "domestic_card", "bank_transfer", "installment"]
SHIPPING_METHODS: list[str] = ["standard", "express", "same_day", "pickup_point"]

COMMERCE_EVENT_TYPE_MAP: dict[str, str] = {
    "view": "product_viewed",
    "add_to_cart": "add_to_cart",
    "checkout_started": "checkout_started",
    "order_placed": "order_placed",
    "payment_failed": "payment_failed",
}


def weighted_choice(
    rng: np.random.Generator,
    labels: list[str],
    weights: list[float],
    size: int,
) -> np.ndarray:
    weights_array = np.asarray(weights, dtype=float)
    weights_array = weights_array / weights_array.sum()
    return rng.choice(labels, size=size, p=weights_array)


def random_timestamps(
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
    return pd.Series(
        start_ts
        + pd.to_timedelta(days, unit="D")
        + pd.to_timedelta(hours, unit="h")
        + pd.to_timedelta(minutes, unit="m")
        + pd.to_timedelta(seconds, unit="s")
    )


def time_bounds(
    config: GeneratorConfig,
) -> tuple[pd.Timestamp, pd.Timestamp, pd.Timestamp]:
    end_ts: pd.Timestamp = pd.Timestamp(config.end_date).normalize() + pd.Timedelta(hours=23, minutes=59)
    start_ts: pd.Timestamp = end_ts - pd.Timedelta(days=config.history_days - 1)
    cutoff_ratio = float(config.quality["schema_evolution_cutoff_ratio"])
    cutoff_ts: pd.Timestamp = start_ts + pd.Timedelta(days=max(1, int(config.history_days * cutoff_ratio)))
    return start_ts, end_ts, cutoff_ts
