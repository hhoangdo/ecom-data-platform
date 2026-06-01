import json
from pathlib import Path


def test_publish_smoke_writes_summary_and_uses_existing_topic_contract(tmp_path: Path) -> None:
    from vina_bim_shop.flink.smoke import run_streaming_smoke_publish

    published_batches = []

    def fake_publish_topic_events(*, topic_events, bootstrap_servers, flush_timeout_seconds=30.0):
        published_batches.append(
            {
                "topics": sorted(topic_events),
                "counts": {topic: len(frame) for topic, frame in topic_events.items()},
                "bootstrap_servers": bootstrap_servers,
            }
        )
        return {topic: len(frame) for topic, frame in topic_events.items()}

    summary = run_streaming_smoke_publish(
        evidence_root=tmp_path,
        publish_topic_events=fake_publish_topic_events,
    )

    assert published_batches == [
        {
            "topics": ["catalog_events", "commerce_events", "fulfillment_events", "ops_events"],
            "counts": {
                "catalog_events": 1,
                "commerce_events": 9,
                "fulfillment_events": 2,
                "ops_events": 3,
            },
            "bootstrap_servers": "localhost:9092",
        }
    ]
    assert summary["expected_outputs"] == {
        "commerce_metrics_topic": "realtime_commerce_metrics_1m",
        "ops_alerts_topic": "realtime_ops_alerts",
        "metric_corrections_topic": "realtime_metric_corrections",
    }
    assert (tmp_path / "flink_smoke_publish_summary.json").is_file()


def test_capture_evidence_writes_streaming_manifest_and_artifacts(tmp_path: Path) -> None:
    from vina_bim_shop.flink.evidence import capture_evidence

    def fake_get_json(url: str):
        if url.endswith("/overview"):
            return {"taskmanagers": 1, "slots-total": 4}
        if url.endswith("/jobs"):
            return {"jobs": [{"id": "job-1", "name": "commerce-metrics"}]}
        if url.endswith("/taskmanagers"):
            return {"taskmanagers": [{"id": "tm-1"}]}
        return {"url": url}

    def fake_run_command(command):
        joined = " ".join(command)
        if "--list" in joined:
            return "realtime_commerce_metrics_1m\nrealtime_ops_alerts\nrealtime_metric_corrections\n"
        if "mc ls" in joined and "checkpoints" in joined:
            return "[2026-06-01] 0B checkpoints/flink/commerce_metrics/\n"
        if "mc ls" in joined and "streaming_curated" in joined:
            return "[2026-06-01] 2KB evidence/streaming_curated/realtime_ops_alerts/event_date=2026-06-01/\n"
        if "kcat" in joined or "kafka-console-consumer" in joined:
            return '{"topic":"realtime_ops_alerts","sample":true}\n'
        return ""

    manifest = capture_evidence(
        evidence_root=tmp_path,
        get_json=fake_get_json,
        run_command=fake_run_command,
        screenshot_capturer=lambda *, flink_ui_url, screenshots_path: {
            "jobs": str((screenshots_path / "flink_jobs.png").resolve()),
            "checkpoints": str((screenshots_path / "flink_checkpoints.png").resolve()),
        },
    )

    expected_files = [
        "flink_overview.json",
        "flink_jobs.json",
        "flink_taskmanagers.json",
        "derived_topic_samples.json",
        "checkpoint_listing.txt",
        "curated_output_listing.txt",
        "version_matrix.json",
        "run_manifest.json",
        "screenshots/README.md",
    ]
    for relative_path in expected_files:
        assert (tmp_path / relative_path).is_file()

    samples = json.loads((tmp_path / "derived_topic_samples.json").read_text(encoding="utf-8"))
    assert set(samples) == {
        "realtime_commerce_metrics_1m",
        "realtime_ops_alerts",
        "realtime_metric_corrections",
    }
    assert manifest["service_urls"]["flink_ui"] == "http://localhost:8086"
