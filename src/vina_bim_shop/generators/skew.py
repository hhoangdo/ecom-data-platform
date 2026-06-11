from __future__ import annotations

import numpy as np
import pandas as pd

from vina_bim_shop.generators.config import GeneratorConfig
from vina_bim_shop.generators.profiles import weighted_choice


CUSTOMER_SEGMENT_PREFERRED_DEVICE_WEIGHTS: list[float] = [0.55, 0.23, 0.16, 0.06]
CUSTOMER_SEGMENT_PREFERRED_DEVICE_LABELS: list[str] = [
    "app_android",
    "app_ios",
    "mobile_web",
    "desktop_web",
]

CUSTOMER_ACQUISITION_CHANNEL_WEIGHTS: list[float] = [0.48, 0.32, 0.13, 0.07]
CUSTOMER_ACQUISITION_CHANNEL_LABELS: list[str] = ["organic", "ads", "referral", "affiliate"]

SHIPPING_METHOD_WEIGHTS: list[float] = [0.62, 0.24, 0.08, 0.06]

FULFILLMENT_CHANNEL_WEIGHTS: list[float] = [0.62, 0.30, 0.08]
FULFILLMENT_CHANNEL_LABELS: list[str] = [
    "seller_fulfilled",
    "platform_fulfilled",
    "cross_dock",
]

FULFILLMENT_CHANNEL_FOR_ORDERS_WEIGHTS: list[float] = [0.64, 0.29, 0.07]

PAYMENT_METHOD_WEIGHTS: list[float] = [0.36, 0.34, 0.18, 0.09, 0.03]

PAYMENT_FAILURE_REASON_WEIGHTS: list[float] = [0.52, 0.30, 0.18]
PAYMENT_FAILURE_REASON_LABELS: list[str] = [
    "insufficient_funds",
    "provider_timeout",
    "auth_failed",
]

SHIPMENT_STATUS_WEIGHTS: list[float] = [0.82, 0.13, 0.05]
SHIPMENT_STATUS_LABELS: list[str] = ["delivered", "in_transit", "delayed"]

PROMOTION_FUNDING_WEIGHTS: list[float] = [0.42, 0.36, 0.22]
PROMOTION_FUNDING_LABELS: list[str] = ["platform", "seller", "mixed"]


def segment_affinity_to_categories(customers: pd.DataFrame, config: GeneratorConfig) -> pd.Series:
    return customers["segment"].map(
        {segment: config.customer_segments[segment]["preferred_category"] for segment in config.customer_segments}
    )


def customer_order_weights(customers: pd.DataFrame) -> np.ndarray:
    return customers["segment"].map(
        {
            "budget_shopper": 0.95,
            "loyal_fmcg_repeat_buyer": 1.7,
            "occasional_high_value_elha_buyer": 0.65,
            "fashion_browser": 0.85,
            "home_improver": 0.75,
        }
    ).to_numpy(dtype=float)


def coupon_affinity(customers: pd.DataFrame, config: GeneratorConfig) -> np.ndarray:
    return customers["segment"].map(
        {segment: config.customer_segments[segment]["coupon_affinity"] for segment in config.customer_segments}
    ).to_numpy(dtype=float)


def preferred_device(rng: np.random.Generator, n: int) -> np.ndarray:
    return weighted_choice(
        rng,
        CUSTOMER_SEGMENT_PREFERRED_DEVICE_LABELS,
        CUSTOMER_SEGMENT_PREFERRED_DEVICE_WEIGHTS,
        n,
    )


def acquisition_channel(rng: np.random.Generator, n: int) -> np.ndarray:
    return weighted_choice(
        rng,
        CUSTOMER_ACQUISITION_CHANNEL_LABELS,
        CUSTOMER_ACQUISITION_CHANNEL_WEIGHTS,
        n,
    )


def shipping_method(rng: np.random.Generator, n: int) -> np.ndarray:
    return weighted_choice(rng, ["standard", "express", "same_day", "pickup_point"], SHIPPING_METHOD_WEIGHTS, n)


def fulfillment_channel(rng: np.random.Generator, n: int) -> np.ndarray:
    return weighted_choice(rng, FULFILLMENT_CHANNEL_LABELS, FULFILLMENT_CHANNEL_WEIGHTS, n)


def fulfillment_channel_for_orders(rng: np.random.Generator, n: int) -> np.ndarray:
    return weighted_choice(rng, FULFILLMENT_CHANNEL_LABELS, FULFILLMENT_CHANNEL_FOR_ORDERS_WEIGHTS, n)


def payment_method(rng: np.random.Generator, n: int) -> np.ndarray:
    return weighted_choice(rng, ["cod", "e_wallet", "domestic_card", "bank_transfer", "installment"], PAYMENT_METHOD_WEIGHTS, n)


def payment_failure_reason(rng: np.random.Generator, n: int) -> np.ndarray:
    return weighted_choice(rng, PAYMENT_FAILURE_REASON_LABELS, PAYMENT_FAILURE_REASON_WEIGHTS, n)


def shipment_status(rng: np.random.Generator, n: int) -> np.ndarray:
    return weighted_choice(rng, SHIPMENT_STATUS_LABELS, SHIPMENT_STATUS_WEIGHTS, n)


def promotion_funding_type(rng: np.random.Generator, n: int) -> np.ndarray:
    return weighted_choice(rng, PROMOTION_FUNDING_LABELS, PROMOTION_FUNDING_WEIGHTS, n)
