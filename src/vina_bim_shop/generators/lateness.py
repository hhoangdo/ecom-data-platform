from __future__ import annotations

from typing import Any

import numpy as np
import pandas as pd

from vina_bim_shop.generators.config import GeneratorConfig


def is_burst_timestamp(value: pd.Timestamp) -> bool:
    timestamp = pd.Timestamp(value)
    return (timestamp.hour == 12 and timestamp.minute <= 20) or (timestamp.hour == 20 and timestamp.minute <= 20)


def apply_created_ts_and_late_arrivals(
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


def inject_device_missingness(
    config: GeneratorConfig,
    rng: np.random.Generator,
    events: pd.DataFrame,
) -> pd.DataFrame:
    output = events.copy()
    missing = rng.random(len(output)) < float(config.quality["missing_device_type_rate"])
    output.loc[missing, ["device_type", "device_os"]] = None
    return output


__all__ = [
    "apply_created_ts_and_late_arrivals",
    "inject_device_missingness",
    "is_burst_timestamp",
]
