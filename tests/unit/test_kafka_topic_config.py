from pathlib import Path
from unittest.mock import Mock
import json

import yaml


SOURCE_TOPICS = {
    "commerce_events",
    "catalog_events",
    "fulfillment_events",
    "ops_events",
    "dead_letter_events",
}

DERIVED_PLACEHOLDER_TOPICS = {
    "realtime_commerce_metrics_1m",
    "realtime_ops_alerts",
    "realtime_metric_corrections",
}


def test_topic_config_preserves_adr01_topic_contract() -> None:
    repo_root = Path(__file__).resolve().parents[2]
    topics_path = repo_root / "infra" / "kafka" / "topics.yaml"

    config = yaml.safe_load(topics_path.read_text(encoding="utf-8"))

    assert set(config["source_topics"]) == SOURCE_TOPICS
    assert set(config["derived_placeholder_topics"]) == DERIVED_PLACEHOLDER_TOPICS


def test_topic_config_is_single_broker_and_resettable() -> None:
    repo_root = Path(__file__).resolve().parents[2]
    topics_path = repo_root / "infra" / "kafka" / "topics.yaml"

    config = yaml.safe_load(topics_path.read_text(encoding="utf-8"))

    for topic_config in [*config["source_topics"].values(), *config["derived_placeholder_topics"].values()]:
        assert topic_config["replication_factor"] == 1
        assert topic_config["partitions"] == 1
        assert topic_config["cleanup_policy"] == "delete"


def test_topic_loader_returns_source_placeholder_and_all_topics() -> None:
    from vina_bim_shop.kafka.topics import (
        all_topic_names,
        derived_placeholder_topic_names,
        source_topic_names,
    )

    assert set(source_topic_names()) == SOURCE_TOPICS
    assert set(derived_placeholder_topic_names()) == DERIVED_PLACEHOLDER_TOPICS
    assert all_topic_names() == [
        "commerce_events",
        "catalog_events",
        "fulfillment_events",
        "ops_events",
        "dead_letter_events",
        "realtime_commerce_metrics_1m",
        "realtime_ops_alerts",
        "realtime_metric_corrections",
    ]


def test_bootstrap_topics_creates_every_topic_idempotently() -> None:
    from vina_bim_shop.kafka.bootstrap import bootstrap_topics

    runner = Mock()

    bootstrap_topics(runner=runner)

    assert runner.call_count == 8
    commands = [call.args[0] for call in runner.call_args_list]
    joined = [" ".join(command) for command in commands]
    assert all("--if-not-exists" in command for command in joined)
    assert all("--replication-factor 1" in command for command in joined)
    assert all("--partitions 1" in command for command in joined)
    assert all("--config cleanup.policy=delete" in command for command in joined)
    assert any("--topic commerce_events" in command for command in joined)
    assert any("--topic realtime_metric_corrections" in command for command in joined)


def test_kafka_connect_s3_template_targets_source_topics_only() -> None:
    repo_root = Path(__file__).resolve().parents[2]
    template = json.loads((repo_root / "infra" / "kafka" / "connect" / "source-events-s3-sink.template.json").read_text(encoding="utf-8"))

    assert template["config"]["topics"] == "commerce_events,catalog_events,fulfillment_events,ops_events,dead_letter_events"
    assert "realtime_commerce_metrics_1m" not in template["config"]["topics"]
    assert template["config"]["value.converter"] == "org.apache.kafka.connect.json.JsonConverter"
    assert template["config"]["value.converter.schemas.enable"] == "false"


def test_kafka_connect_s3_template_uses_bronze_event_prefix_contract() -> None:
    repo_root = Path(__file__).resolve().parents[2]
    template = json.loads((repo_root / "infra" / "kafka" / "connect" / "source-events-s3-sink.template.json").read_text(encoding="utf-8"))

    config = template["config"]
    assert config["s3.bucket.name"] == "${BRONZE_BUCKET}"
    assert config["topics.dir"] == "events"
    assert "ingest_date=" in config["path.format"]
    assert config["partitioner.class"] != "io.confluent.connect.storage.partitioner.DefaultPartitioner"


def test_kafka_connect_s3_template_uses_time_partitioner_compatible_path_format() -> None:
    repo_root = Path(__file__).resolve().parents[2]
    template = json.loads((repo_root / "infra" / "kafka" / "connect" / "source-events-s3-sink.template.json").read_text(encoding="utf-8"))

    config = template["config"]
    assert config["topics.dir"] == "events"
    assert config["path.format"] == "'ingest_date='YYYY-MM-dd"
    assert "${topic}" not in config["path.format"]


def test_cleanup_deletes_topics_then_bootstraps_again(tmp_path: Path, monkeypatch) -> None:
    from vina_bim_shop.kafka.cleanup import cleanup_kafka

    commands = []
    bootstrapped = []

    def runner(command):
        commands.append(command)

    monkeypatch.setattr("vina_bim_shop.kafka.cleanup.bootstrap_topics", lambda runner, bootstrap_server: bootstrapped.append(bootstrap_server))

    evidence_root = tmp_path / "evidence"
    evidence_root.mkdir()
    (evidence_root / "old.json").write_text("{}", encoding="utf-8")

    cleanup_kafka(runner=runner, bootstrap_server="kafka:29092", evidence_root=evidence_root, clean_evidence=True)

    assert len(commands) == 8
    assert all("--delete" in " ".join(command) for command in commands)
    assert all("--if-exists" in " ".join(command) for command in commands)
    assert bootstrapped == ["kafka:29092"]
    assert not evidence_root.exists()
