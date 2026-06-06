from pathlib import Path

from vina_bim_shop.generators.runner import run_generation


def test_run_generation_publish_kafka_calls_publisher_and_keeps_jsonl(tmp_path: Path, monkeypatch) -> None:
    repo_root = Path(__file__).resolve().parents[2]
    calls = []

    def fake_publish_topic_events(*, topic_events, bootstrap_servers, flush_timeout_seconds):
        calls.append(
            {
                "topics": sorted(topic_events),
                "bootstrap_servers": bootstrap_servers,
                "flush_timeout_seconds": flush_timeout_seconds,
            }
        )
        return {topic: len(frame) for topic, frame in topic_events.items()}

    monkeypatch.setattr("vina_bim_shop.kafka.publisher.publish_topic_events", fake_publish_topic_events)

    result = run_generation(
        config_path=repo_root / "configs" / "generator" / "base.yaml",
        scale="smoke",
        mode="full",
        raw_root=tmp_path / "raw",
        evidence_root=tmp_path / "evidence",
        clean=True,
        seed=123,
        publish_kafka=True,
        kafka_bootstrap_servers="localhost:9092",
        kafka_flush_timeout_seconds=5.0,
    )

    assert calls == [
        {
            "topics": ["catalog_events", "commerce_events", "dead_letter_events", "fulfillment_events", "ops_events"],
            "bootstrap_servers": "localhost:9092",
            "flush_timeout_seconds": 5.0,
        }
    ]
    assert (result.raw_root / "kafka_topics" / "commerce_events" / "events.jsonl").is_file()


def test_run_generation_does_not_publish_by_default(tmp_path: Path, monkeypatch) -> None:
    repo_root = Path(__file__).resolve().parents[2]
    calls = []

    def fake_publish_topic_events(**kwargs):
        calls.append(kwargs)
        return {}

    monkeypatch.setattr("vina_bim_shop.kafka.publisher.publish_topic_events", fake_publish_topic_events)

    run_generation(
        config_path=repo_root / "configs" / "generator" / "base.yaml",
        scale="smoke",
        mode="full",
        raw_root=tmp_path / "raw",
        evidence_root=tmp_path / "evidence",
        clean=True,
        seed=123,
    )

    assert calls == []
