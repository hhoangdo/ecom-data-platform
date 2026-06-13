import json
from pathlib import Path

import pytest


def test_producer_smoke_runs_deterministic_generation_and_writes_summary(tmp_path: Path, monkeypatch) -> None:
    from vina_bim_shop.kafka.producer_smoke import run_producer_smoke

    calls = []

    def fake_run_generation(**kwargs):
        calls.append(kwargs)

        class Result:
            raw_root = tmp_path / "raw"
            evidence_root = tmp_path / "evidence" / "generator"
            row_counts = {"kafka_topics": 5}

        for topic in ["commerce_events", "catalog_events", "fulfillment_events", "ops_events", "dead_letter_events"]:
            topic_path = Result.raw_root / "kafka_topics" / topic
            topic_path.mkdir(parents=True, exist_ok=True)
            (topic_path / "events.jsonl").write_text('{"event_topic":"' + topic + '"}\n', encoding="utf-8")
        return Result()

    monkeypatch.setattr("vina_bim_shop.kafka.producer_smoke.run_generation", fake_run_generation)

    summary = run_producer_smoke(
        config_path=Path("configs/generator/base.yaml"),
        raw_root=tmp_path / "raw",
        evidence_root=tmp_path / "evidence",
        bootstrap_servers="localhost:9092",
    )

    assert calls[0]["scale"] == "smoke"
    assert calls[0]["mode"] == "full"
    assert calls[0]["seed"] == 101
    assert calls[0]["publish_kafka"] is True
    assert calls[0]["evidence_root"] == tmp_path / "evidence" / "generator"
    assert summary["source_topics"] == ["commerce_events", "catalog_events", "fulfillment_events", "ops_events", "dead_letter_events"]
    assert (tmp_path / "evidence" / "producer_smoke_summary.json").is_file()
    persisted = json.loads((tmp_path / "evidence" / "producer_smoke_summary.json").read_text(encoding="utf-8"))
    assert persisted["kafka_bootstrap_servers"] == "localhost:9092"


def test_consumer_smoke_writes_samples_and_summary(tmp_path: Path) -> None:
    from vina_bim_shop.kafka.consumer_smoke import run_consumer_smoke

    messages = []
    for topic in ["commerce_events", "catalog_events", "fulfillment_events", "ops_events"]:
        messages.append(
            {
                "event_id": f"{topic}-1",
                "event_type": "sample_event",
                "event_topic": topic,
                "schema_version": 1,
                "event_timestamp": "2026-05-01T00:00:00",
                "created_ts": "2026-05-01T00:00:01",
                "producer": "vina_bim_shop.synthetic_source",
                "correlation_ids": {},
                "payload": {},
            }
        )
    messages.append(
        {
            "dlq_id": "DLQ-1",
            "source_topic": "commerce_events",
            "error_reason": "invalid_json",
            "raw_payload": "{bad",
            "event_topic": "dead_letter_events",
            "schema_version": 1,
            "ingest_ts": "2026-05-02T00:00:00",
        }
    )

    summary = run_consumer_smoke(
        bootstrap_servers="localhost:9092",
        evidence_root=tmp_path,
        message_iter=lambda _topics, _timeout_seconds: iter(messages),
    )

    assert summary["topics_read"] == {
        "commerce_events": 1,
        "catalog_events": 1,
        "fulfillment_events": 1,
        "ops_events": 1,
        "dead_letter_events": 1,
    }
    assert (tmp_path / "consumer_samples.json").is_file()
    assert (tmp_path / "consumer_smoke_summary.json").is_file()


def test_consumer_smoke_default_timeout_allows_local_group_assignment(tmp_path: Path) -> None:
    from vina_bim_shop.kafka.consumer_smoke import run_consumer_smoke

    observed_timeouts = []
    messages = [
        {
            "event_id": f"{topic}-1",
            "event_type": "sample_event",
            "event_topic": topic,
            "schema_version": 1,
            "event_timestamp": "2026-05-01T00:00:00",
            "created_ts": "2026-05-01T00:00:01",
            "producer": "vina_bim_shop.synthetic_source",
            "correlation_ids": {},
            "payload": {},
        }
        for topic in ["commerce_events", "catalog_events", "fulfillment_events", "ops_events"]
    ]
    messages.append(
        {
            "dlq_id": "DLQ-1",
            "source_topic": "commerce_events",
            "error_reason": "invalid_json",
            "raw_payload": "{bad",
            "event_topic": "dead_letter_events",
            "schema_version": 1,
            "ingest_ts": "2026-05-02T00:00:00",
        }
    )

    def message_iter(_topics, timeout_seconds):
        observed_timeouts.append(timeout_seconds)
        return iter(messages)

    run_consumer_smoke(evidence_root=tmp_path, message_iter=message_iter)

    assert observed_timeouts == [120]


def test_capture_evidence_writes_manifest_and_service_artifacts(tmp_path: Path) -> None:
    from vina_bim_shop.kafka.evidence import capture_evidence

    def fake_get_json(url: str):
        if url.endswith("/subjects"):
            return ["commerce_events-value", "catalog_events-value"]
        if url.endswith("/connectors"):
            return []
        return {"status": "ok"}

    def fake_run_command(command):
        joined = " ".join(command)
        if "--list" in joined:
            return "commerce_events\ncatalog_events\nfulfillment_events\nops_events\ndead_letter_events\nrealtime_commerce_metrics_1m\nrealtime_ops_alerts\nrealtime_metric_corrections\n"
        if "--describe" in joined:
            return "Topic: commerce_events\nTopic: catalog_events\n"
        return ""

    manifest = capture_evidence(
        evidence_root=tmp_path,
        get_json=fake_get_json,
        run_command=fake_run_command,
    )

    assert (tmp_path / "topic_list.txt").is_file()
    assert (tmp_path / "topic_descriptions.txt").is_file()
    assert (tmp_path / "schema_registry_subjects.json").is_file()
    assert (tmp_path / "kafka_connect_status.json").is_file()
    assert (tmp_path / "version_matrix.json").is_file()
    assert (tmp_path / "run_manifest.json").is_file()
    assert not (tmp_path / "screenshots").exists()
    assert "kafka_ui" not in manifest["service_urls"]
    assert all(not artifact.startswith("screenshots/") for artifact in manifest["artifacts"])


def test_capture_evidence_writes_failed_manifest_when_kafka_connect_probe_fails(tmp_path: Path) -> None:
    from vina_bim_shop.kafka.evidence import capture_evidence

    def fake_get_json(url: str):
        if url.endswith("/subjects"):
            return ["commerce_events-value"]
        if url.endswith("/connectors"):
            raise RuntimeError("connect unavailable")
        return {"status": "ok"}

    def fake_run_command(command):
        joined = " ".join(command)
        if "--list" in joined:
            return "commerce_events\n"
        if "--describe" in joined:
            return "Topic: commerce_events\n"
        return ""

    with pytest.raises(RuntimeError, match="Kafka evidence capture failed"):
        capture_evidence(evidence_root=tmp_path, get_json=fake_get_json, run_command=fake_run_command)

    manifest = json.loads((tmp_path / "run_manifest.json").read_text(encoding="utf-8"))
    assert manifest["status"] == "failed"
    assert manifest["failures"] == [{"step": "kafka_connect", "error": "connect unavailable"}]
    assert "schema_registry_subjects.json" in manifest["artifacts"]
