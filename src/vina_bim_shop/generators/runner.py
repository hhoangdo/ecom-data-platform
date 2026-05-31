from __future__ import annotations

import json
import shutil
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Literal

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


CLEANABLE_GENERATOR_OUTPUTS = [
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
    topic_events: dict[str, pd.DataFrame] = {}
    issue_records = list(offline_generation.issue_records)

    if mode in {"offline", "full"}:
        datasets.update(offline_generation.datasets)
        datasets["bad_snapshots"] = _build_bad_snapshots(config)
        issue_records.extend(_quarantine_issue_records("bad_snapshots", datasets["bad_snapshots"]))

    if mode in {"streaming", "full"}:
        streaming_generation = generate_streaming_events(config, offline_generation.datasets)
        topic_events = streaming_generation.topic_events
        topic_events["dead_letter_events"] = _build_dead_letter_events(config)
        issue_records.extend(streaming_generation.issue_records)
        issue_records.extend(_quarantine_issue_records("dead_letter_events", topic_events["dead_letter_events"]))

    _write_raw_outputs(config.raw_root, datasets, topic_events)
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


def _clean_outputs(raw_root: Path, evidence_root: Path) -> None:
    for dataset in CLEANABLE_GENERATOR_OUTPUTS:
        dataset_path = raw_root / dataset
        if dataset_path.exists():
            shutil.rmtree(dataset_path)
    if evidence_root.exists():
        shutil.rmtree(evidence_root)


def _write_raw_outputs(
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


def _quarantine_issue_records(dataset: str, records: pd.DataFrame) -> list[dict[str, Any]]:
    total = max(1, len(records))
    return [
        {
            "dataset": dataset,
            "issue_type": str(error_reason),
            "affected_rows": int(count),
            "observed_rate": round(float(count / total), 5),
        }
        for error_reason, count in records["error_reason"].value_counts().sort_index().items()
    ]


def _build_dead_letter_events(config: Any) -> pd.DataFrame:
    ingest_ts = (pd.Timestamp(config.end_date) + pd.Timedelta(days=1, minutes=5)).isoformat()
    raw_payloads = [
        {
            "dlq_id": "DLQ-EVENT-0001",
            "source_topic": "commerce_events",
            "error_reason": "missing_required_key",
            "raw_payload": json.dumps(
                {
                    "event_type": "order_placed",
                    "event_topic": "commerce_events",
                    "schema_version": 1,
                    "event_timestamp": f"{config.end_date}T10:00:00",
                    "created_ts": f"{config.end_date}T10:00:03",
                    "producer": config.kafka["producer"],
                    "payload": {"order_id": "ORD-MISSING-EVENT-ID"},
                },
                separators=(",", ":"),
            ),
        },
        {
            "dlq_id": "DLQ-EVENT-0002",
            "source_topic": "commerce_events",
            "error_reason": "invalid_json",
            "raw_payload": '{"event_id":"BROKEN","event_type":"order_placed",',
        },
        {
            "dlq_id": "DLQ-EVENT-0003",
            "source_topic": "fulfillment_events",
            "error_reason": "invalid_timestamp",
            "raw_payload": json.dumps(
                {
                    "event_id": "BAD-TS-0001",
                    "event_type": "shipment_handoff",
                    "event_topic": "fulfillment_events",
                    "schema_version": 1,
                    "event_timestamp": "not-a-timestamp",
                    "created_ts": f"{config.end_date}T12:00:00",
                    "producer": config.kafka["producer"],
                    "correlation_ids": {"shipment_id": "SHP-BAD-TS"},
                    "payload": {"shipment_status": "handoff"},
                },
                separators=(",", ":"),
            ),
        },
        {
            "dlq_id": "DLQ-EVENT-0004",
            "source_topic": "catalog_events",
            "error_reason": "unknown_schema_version",
            "raw_payload": json.dumps(
                {
                    "event_id": "BAD-SCHEMA-0001",
                    "event_type": "product_updated",
                    "event_topic": "catalog_events",
                    "schema_version": 99,
                    "event_timestamp": f"{config.end_date}T13:00:00",
                    "created_ts": f"{config.end_date}T13:00:02",
                    "producer": config.kafka["producer"],
                    "correlation_ids": {"product_id": "PRD-UNKNOWN-SCHEMA"},
                    "payload": {"field": "category_attributes"},
                },
                separators=(",", ":"),
            ),
        },
    ]
    rows = []
    for payload in raw_payloads:
        rows.append(
            {
                **payload,
                "event_topic": "dead_letter_events",
                "schema_version": 1,
                "ingest_ts": ingest_ts,
            }
        )
    return pd.DataFrame(rows)


def _build_bad_snapshots(config: Any) -> pd.DataFrame:
    ingest_ts = (pd.Timestamp(config.end_date) + pd.Timedelta(days=1, minutes=10)).isoformat()
    records = [
        {
            "bad_record_id": "BAD-SNAPSHOT-0001",
            "source_dataset": "orders",
            "error_reason": "missing_required_key",
            "raw_record": json.dumps(
                {
                    "customer_id": "CUS-MISSING-ORDER-ID",
                    "order_timestamp": f"{config.end_date}T09:00:00",
                    "status": "paid",
                },
                separators=(",", ":"),
            ),
        },
        {
            "bad_record_id": "BAD-SNAPSHOT-0002",
            "source_dataset": "payments",
            "error_reason": "invalid_json",
            "raw_record": '{"payment_id":"PAY-BROKEN","order_id":"ORD-BROKEN",',
        },
        {
            "bad_record_id": "BAD-SNAPSHOT-0003",
            "source_dataset": "shipments",
            "error_reason": "invalid_timestamp",
            "raw_record": json.dumps(
                {
                    "shipment_id": "SHP-BAD-TS",
                    "order_id": "ORD-BAD-TS",
                    "handoff_ts": "tomorrow-ish",
                    "shipment_status": "handoff",
                },
                separators=(",", ":"),
            ),
        },
        {
            "bad_record_id": "BAD-SNAPSHOT-0004",
            "source_dataset": "products",
            "error_reason": "unknown_schema_version",
            "raw_record": json.dumps(
                {
                    "schema_version": 99,
                    "product_id": "PRD-UNKNOWN-SCHEMA",
                    "primary_category": "FMCG",
                    "category_attributes": {"unexpected": "shape"},
                },
                separators=(",", ":"),
            ),
        },
    ]
    return pd.DataFrame([{**record, "ingest_ts": ingest_ts} for record in records])
