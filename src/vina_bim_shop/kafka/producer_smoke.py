from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from vina_bim_shop.generators.runner import run_generation
from vina_bim_shop.kafka.topics import source_topic_names


def run_producer_smoke(
    *,
    config_path: str | Path = "configs/generator/base.yaml",
    raw_root: str | Path = "data/raw",
    evidence_root: str | Path = "evidence/03_kafka_ingestion",
    bootstrap_servers: str = "localhost:9092",
) -> dict[str, Any]:
    evidence_path = Path(evidence_root)
    result = run_generation(
        config_path=Path(config_path),
        scale="smoke",
        mode="full",
        seed=101,
        clean=True,
        raw_root=Path(raw_root),
        evidence_root=evidence_path / "generator",
        publish_kafka=True,
        kafka_bootstrap_servers=bootstrap_servers,
    )

    topics = source_topic_names()
    jsonl_files = {
        topic: str(result.raw_root / "kafka_topics" / topic / "events.jsonl")
        for topic in topics
    }
    missing_files = [topic for topic, path in jsonl_files.items() if not Path(path).is_file()]
    if missing_files:
        raise RuntimeError(f"Producer smoke did not write JSONL for topics: {', '.join(missing_files)}")

    summary = {
        "kafka_bootstrap_servers": bootstrap_servers,
        "source_topics": topics,
        "row_counts": result.row_counts,
        "raw_root": str(result.raw_root),
        "generator_evidence_root": str(result.evidence_root),
        "jsonl_files": jsonl_files,
    }
    evidence_path.mkdir(parents=True, exist_ok=True)
    (evidence_path / "producer_smoke_summary.json").write_text(
        json.dumps(summary, indent=2, sort_keys=True),
        encoding="utf-8",
    )
    return summary
