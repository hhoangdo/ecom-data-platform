from __future__ import annotations

import shutil
from pathlib import Path
from typing import Any

import pandas as pd

CLEANABLE_GENERATOR_OUTPUTS: list[str] = [
    "customers",
    "sellers",
    "products",
    "product_category_map",
    "inventory_snapshots",
    "promotions",
    "orders",
    "order_items",
    "payments",
    "shipments",
    "bad_snapshots",
    "kafka_topics",
]


def clean_outputs(raw_root: Path, evidence_root: Path) -> None:
    for dataset in CLEANABLE_GENERATOR_OUTPUTS:
        dataset_path = raw_root / dataset
        if dataset_path.exists():
            shutil.rmtree(dataset_path)
    if evidence_root.exists():
        shutil.rmtree(evidence_root)


def write_raw_outputs(
    raw_root: Path,
    datasets: dict[str, pd.DataFrame],
    topic_events: dict[str, pd.DataFrame],
) -> None:
    raw_root.mkdir(parents=True, exist_ok=True)
    for name, frame in datasets.items():
        dataset_path = raw_root / name
        dataset_path.mkdir(parents=True, exist_ok=True)
        if name == "bad_snapshots":
            frame.to_json(dataset_path / "bad_snapshots.jsonl", orient="records", lines=True, date_format="iso")
        else:
            frame.to_parquet(dataset_path / "part-000.parquet", index=False)
    for topic, frame in sorted(topic_events.items()):
        topic_path = raw_root / "kafka_topics" / topic
        topic_path.mkdir(parents=True, exist_ok=True)
        frame.to_json(topic_path / "events.jsonl", orient="records", lines=True, date_format="iso")


__all__ = ["CLEANABLE_GENERATOR_OUTPUTS", "clean_outputs", "write_raw_outputs"]
