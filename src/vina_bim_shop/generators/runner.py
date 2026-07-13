"""Public orchestration for the Section 01 generator."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Literal

import pandas as pd

from vina_bim_shop.generators.bad_records import (
    build_bad_snapshots as _build_bad_snapshots,
    build_dead_letter_events as _build_dead_letter_events,
)
from vina_bim_shop.generators.config import load_generator_config
from vina_bim_shop.generators.duplicates import quarantine_issue_records
from vina_bim_shop.generators.evidence import write_evidence
from vina_bim_shop.generators.offline.generator import generate_offline
from vina_bim_shop.generators.streaming.generator import generate_streaming_events
from vina_bim_shop.generators.writer import (
    CLEANABLE_GENERATOR_OUTPUTS,
    clean_outputs,
    write_raw_outputs,
)

GenerationMode = Literal["offline", "streaming", "full"]


@dataclass(frozen=True)
class GenerationResult:
    """Return generated roots, row counts, and evidence paths from one generator run."""

    raw_root: Path
    evidence_root: Path
    row_counts: dict[str, int]
    evidence_paths: dict[str, Path]


def run_generation(
    *,
    config_path: str | Path,
    scale: str,
    mode: GenerationMode,
    raw_root: str | Path | None = None,
    evidence_root: str | Path | None = None,
    seed: int | None = None,
    clean: bool = False,
    publish_kafka: bool = False,
    kafka_bootstrap_servers: str | None = None,
    kafka_flush_timeout_seconds: float = 30.0,
) -> GenerationResult:
    """Generate the selected source modes and return their persisted evidence locations.

    Configuration and write failures propagate so command callers cannot mistake a partial
    dataset for a completed generation run.
    """

    config = load_generator_config(
        config_path,
        scale=scale,
        raw_root=raw_root,
        evidence_root=evidence_root,
        seed=seed,
    )
    if clean:
        clean_outputs(config.raw_root, config.evidence_root)

    offline_generation = generate_offline(config)
    datasets: dict[str, pd.DataFrame] = {}
    topic_events: dict[str, pd.DataFrame] = {}
    issue_records = list(offline_generation.issue_records)

    if mode in {"offline", "full"}:
        datasets.update(offline_generation.datasets)
        datasets["bad_snapshots"] = _build_bad_snapshots(config)
        issue_records.extend(quarantine_issue_records("bad_snapshots", datasets["bad_snapshots"]))

    if mode in {"streaming", "full"}:
        streaming_generation = generate_streaming_events(config, offline_generation.datasets)
        topic_events = streaming_generation.topic_events
        topic_events["dead_letter_events"] = _build_dead_letter_events(config)
        issue_records.extend(streaming_generation.issue_records)
        issue_records.extend(quarantine_issue_records("dead_letter_events", topic_events["dead_letter_events"]))

    write_raw_outputs(config.raw_root, datasets, topic_events)
    if publish_kafka and topic_events:
        from vina_bim_shop.kafka.publisher import publish_topic_events

        publish_topic_events(
            topic_events=topic_events,
            bootstrap_servers=kafka_bootstrap_servers or str(config.kafka["bootstrap_servers"]),
            flush_timeout_seconds=kafka_flush_timeout_seconds,
        )
    evidence_paths = write_evidence(config, datasets, issue_records, mode=mode, topic_events=topic_events)
    row_counts = {name: len(frame) for name, frame in datasets.items()}
    if topic_events:
        row_counts["kafka_topics"] = sum(len(frame) for frame in topic_events.values())

    return GenerationResult(
        raw_root=config.raw_root,
        evidence_root=config.evidence_root,
        row_counts=row_counts,
        evidence_paths=evidence_paths,
    )


__all__ = [
    "CLEANABLE_GENERATOR_OUTPUTS",
    "GenerationMode",
    "GenerationResult",
    "run_generation",
]


_clean_outputs = clean_outputs
_quarantine_issue_records = quarantine_issue_records
