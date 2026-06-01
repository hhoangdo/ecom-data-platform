from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml


DEFAULT_STREAMING_CONFIG_PATH = Path(__file__).resolve().parents[3] / "configs" / "pipelines" / "flink_streaming.yaml"


@dataclass(frozen=True)
class StreamingConfig:
    source_topics: dict[str, str]
    derived_topics: dict[str, str]
    window_minutes: int
    out_of_orderness_seconds: int
    allowed_lateness_seconds: dict[str, int]
    payment_failure_alert_threshold: dict[str, float | int]
    checkpoint_bucket: str
    checkpoint_prefix: str
    curated_output_bucket: str
    curated_output_prefix: str


def load_streaming_config(path: str | Path = DEFAULT_STREAMING_CONFIG_PATH) -> StreamingConfig:
    data = yaml.safe_load(Path(path).read_text(encoding="utf-8"))
    if not isinstance(data, dict):
        raise ValueError(f"Expected mapping in {path}")
    return StreamingConfig(
        source_topics=dict(data["source_topics"]),
        derived_topics=dict(data["derived_topics"]),
        window_minutes=int(data["window_minutes"]),
        out_of_orderness_seconds=int(data["out_of_orderness_seconds"]),
        allowed_lateness_seconds={str(key): int(value) for key, value in dict(data["allowed_lateness_seconds"]).items()},
        payment_failure_alert_threshold=_payment_thresholds(data["payment_failure_alert_threshold"]),
        checkpoint_bucket=str(data["checkpoint_bucket"]),
        checkpoint_prefix=str(data["checkpoint_prefix"]),
        curated_output_bucket=str(data["curated_output_bucket"]),
        curated_output_prefix=str(data["curated_output_prefix"]),
    )


def _payment_thresholds(raw: dict[str, Any]) -> dict[str, float | int]:
    return {
        "count": int(raw["count"]),
        "rate": float(raw["rate"]),
    }
