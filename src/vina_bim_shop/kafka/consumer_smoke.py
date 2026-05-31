from __future__ import annotations

import json
import time
from collections.abc import Callable, Iterable
from pathlib import Path
from typing import Any

import jsonschema

from vina_bim_shop.kafka.topics import source_topic_names


MessageIter = Callable[[list[str], int], Iterable[dict[str, Any]]]


def _schema_for(repo_root: Path, topic: str) -> dict[str, Any]:
    return json.loads((repo_root / "infra" / "kafka" / "schemas" / f"{topic}-value.schema.json").read_text(encoding="utf-8"))


def _consume_messages(bootstrap_servers: str, group_id: str, topics: list[str], timeout_seconds: int) -> Iterable[dict[str, Any]]:
    from confluent_kafka import Consumer, KafkaError

    consumer = Consumer(
        {
            "bootstrap.servers": bootstrap_servers,
            "group.id": group_id,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
    )
    consumer.subscribe(topics)
    deadline = time.time() + timeout_seconds
    try:
        while time.time() < deadline:
            message = consumer.poll(1.0)
            if message is None:
                continue
            if message.error():
                if message.error().code() == KafkaError._PARTITION_EOF:
                    continue
                raise RuntimeError(message.error())
            yield json.loads(message.value().decode("utf-8"))
    finally:
        consumer.close()


def run_consumer_smoke(
    *,
    bootstrap_servers: str = "localhost:9092",
    evidence_root: str | Path = "evidence/03_kafka_ingestion",
    group_id: str | None = None,
    timeout_seconds: int = 120,
    message_iter: MessageIter | None = None,
) -> dict[str, Any]:
    topics = source_topic_names()
    repo_root = Path(__file__).resolve().parents[3]
    schemas = {topic: _schema_for(repo_root, topic) for topic in topics}
    read_counts = {topic: 0 for topic in topics}
    samples: dict[str, dict[str, Any]] = {}
    iterator = message_iter or (lambda read_topics, timeout: _consume_messages(bootstrap_servers, group_id or f"vina-bim-shop-smoke-{int(time.time())}", read_topics, timeout))

    for event in iterator(topics, timeout_seconds):
        topic = str(event.get("event_topic", ""))
        if topic not in read_counts:
            continue
        jsonschema.validate(event, schemas[topic])
        read_counts[topic] += 1
        samples.setdefault(topic, event)
        if all(count > 0 for count in read_counts.values()):
            break

    missing = [topic for topic, count in read_counts.items() if count == 0]
    if missing:
        raise RuntimeError(f"Consumer smoke did not read messages for topics: {', '.join(missing)}")

    summary = {
        "kafka_bootstrap_servers": bootstrap_servers,
        "group_id": group_id,
        "topics_read": read_counts,
        "timeout_seconds": timeout_seconds,
    }
    evidence_path = Path(evidence_root)
    evidence_path.mkdir(parents=True, exist_ok=True)
    (evidence_path / "consumer_samples.json").write_text(json.dumps(samples, indent=2, sort_keys=True), encoding="utf-8")
    (evidence_path / "consumer_smoke_summary.json").write_text(json.dumps(summary, indent=2, sort_keys=True), encoding="utf-8")
    return summary
