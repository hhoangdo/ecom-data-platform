from __future__ import annotations

import numpy as np
import pandas as pd

from vina_bim_shop.generators.config import GeneratorConfig
from vina_bim_shop.generators.ids import dated_ids
from vina_bim_shop.generators.skew import payment_failure_reason, payment_method, shipment_status


def _generate_payments(config: GeneratorConfig, rng: np.random.Generator, orders: pd.DataFrame) -> pd.DataFrame:
    n = len(orders)
    failure_rates = {
        segment: values["payment_failure_rate"]
        for segment, values in config.customer_segments.items()
    }
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
            "payment_method": payment_method(rng, n),
            "amount": orders["order_net_amount"].round(2).to_numpy(),
            "payment_status": payment_status,
            "failure_reason": np.where(failed, payment_failure_reason(rng, n), None),
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
    statuses = np.where(paid, shipment_status(rng, n), "blocked_payment_failed")
    return pd.DataFrame(
        {
            "shipment_id": dated_ids("SHP", orders["shipping_city"].str[:3].str.upper(), order_ts, pd.Series(np.arange(1, n + 1))),
            "order_id": orders["order_id"].to_numpy(),
            "customer_id": orders["customer_id"].to_numpy(),
            "shipping_city": orders["shipping_city"].to_numpy(),
            "shipping_region": orders["shipping_region"].to_numpy(),
            "shipping_method": orders["shipping_method"].to_numpy(),
            "shipment_status": statuses,
            "handoff_ts": np.where(paid, handoff_ts, pd.NaT),
            "estimated_delivery_ts": np.where(paid, delivery_ts, pd.NaT),
            "created_ts": order_ts + pd.to_timedelta(rng.integers(35, 600, n), unit="s"),
        }
    )


def generate_payments(config: GeneratorConfig, rng: np.random.Generator, orders: pd.DataFrame) -> pd.DataFrame:
    return _generate_payments(config, rng, orders)


def generate_shipments(
    config: GeneratorConfig,
    rng: np.random.Generator,
    orders: pd.DataFrame,
    sellers: pd.DataFrame,
) -> pd.DataFrame:
    return _generate_shipments(config, rng, orders, sellers)


__all__ = ["generate_payments", "generate_shipments"]
