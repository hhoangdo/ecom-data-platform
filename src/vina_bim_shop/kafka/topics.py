from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml


TOPICS_CONFIG_PATH = Path(__file__).resolve().parents[3] / "infra" / "kafka" / "topics.yaml"


def load_topic_config(path: str | Path = TOPICS_CONFIG_PATH) -> dict[str, Any]:
    with Path(path).open("r", encoding="utf-8") as handle:
        data = yaml.safe_load(handle)
    if not isinstance(data, dict):
        raise ValueError(f"Expected topic config mapping in {path}")
    return data


def source_topic_names(path: str | Path = TOPICS_CONFIG_PATH) -> list[str]:
    return list(load_topic_config(path)["source_topics"])


def derived_placeholder_topic_names(path: str | Path = TOPICS_CONFIG_PATH) -> list[str]:
    return list(load_topic_config(path)["derived_placeholder_topics"])


def all_topic_names(path: str | Path = TOPICS_CONFIG_PATH) -> list[str]:
    return [*source_topic_names(path), *derived_placeholder_topic_names(path)]
