from __future__ import annotations

import shutil
from dataclasses import dataclass
from pathlib import Path
from typing import Literal

import pandas as pd

from vina_bim_shop.generators.config import load_generator_config
from vina_bim_shop.generators.evidence import write_evidence
from vina_bim_shop.generators.offline.generator import generate_offline
from vina_bim_shop.generators.streaming.generator import generate_streaming_events

GenerationMode = Literal["offline", "streaming", "full"]


@dataclass(frozen=True)
class GenerationResult:
    raw_root: Path
    evidence_root: Path
    row_counts: dict[str, int]
    evidence_paths: dict[str, Path]


GENERATOR_DATASETS = [
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
    "stream_events",
]


def run_generation(
    *,
    config_path: str | Path,
    scale: str,
    mode: GenerationMode,
    raw_root: str | Path | None = None,
    evidence_root: str | Path | None = None,
    seed: int | None = None,
    clean: bool = False,
) -> GenerationResult:
    config = load_generator_config(
        config_path,
        scale=scale,
        raw_root=raw_root,
        evidence_root=evidence_root,
        seed=seed,
    )
    if clean:
        _clean_outputs(config.raw_root, config.evidence_root)

    offline_generation = generate_offline(config)
    datasets: dict[str, pd.DataFrame] = {}
    issue_records = list(offline_generation.issue_records)

    if mode in {"offline", "full"}:
        datasets.update(offline_generation.datasets)

    if mode in {"streaming", "full"}:
        streaming_generation = generate_streaming_events(config, offline_generation.datasets)
        datasets["stream_events"] = streaming_generation.stream_events
        issue_records.extend(streaming_generation.issue_records)

    _write_raw_outputs(config.raw_root, datasets)
    evidence_paths = write_evidence(config, datasets, issue_records, mode=mode)

    return GenerationResult(
        raw_root=config.raw_root,
        evidence_root=config.evidence_root,
        row_counts={name: len(frame) for name, frame in datasets.items()},
        evidence_paths=evidence_paths,
    )


def _clean_outputs(raw_root: Path, evidence_root: Path) -> None:
    for dataset in GENERATOR_DATASETS:
        dataset_path = raw_root / dataset
        if dataset_path.exists():
            shutil.rmtree(dataset_path)
    if evidence_root.exists():
        shutil.rmtree(evidence_root)


def _write_raw_outputs(raw_root: Path, datasets: dict[str, pd.DataFrame]) -> None:
    raw_root.mkdir(parents=True, exist_ok=True)
    for name, frame in datasets.items():
        dataset_path = raw_root / name
        dataset_path.mkdir(parents=True, exist_ok=True)
        if name == "stream_events":
            frame.to_json(dataset_path / "stream_events.jsonl", orient="records", lines=True, date_format="iso")
        else:
            frame.to_parquet(dataset_path / "part-000.parquet", index=False)
