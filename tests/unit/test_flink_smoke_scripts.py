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


def test_build_cleanroom_smoke_phases_stage_initial_control_and_late_events() -> None:
    from vina_bim_shop.flink.smoke import build_cleanroom_smoke_phases

    phases = build_cleanroom_smoke_phases()

    assert [phase["name"] for phase in phases] == [
        "initial_business_events",
        "watermark_control_event",
        "late_correction_event",
    ]

    phase1 = phases[0]["topic_events"]
    phase2 = phases[1]["topic_events"]
    phase3 = phases[2]["topic_events"]

    assert [record["event_id"] for record in phase1["commerce_events"].to_dict("records")] == [
        "evt-1",
        "evt-2",
        "evt-3",
        "evt-3",
        "evt-4",
        "evt-5",
        "evt-6",
        "evt-7",
    ]
    assert [record["event_id"] for record in phase2["commerce_events"].to_dict("records")] == ["ctrl-1"]
    assert [record["event_id"] for record in phase3["commerce_events"].to_dict("records")] == ["evt-8"]

    control_event = phase2["commerce_events"].to_dict("records")[0]
    assert control_event["payload"]["primary_category"] == "CONTROL"
    assert control_event["event_timestamp"] == "2026-05-01T10:01:10"

    late_event = phase3["commerce_events"].to_dict("records")[0]
    assert late_event["created_ts"] == "2026-05-01T10:06:10"
    assert late_event["event_timestamp"] == "2026-05-01T10:00:40"
    assert set(phase1) == {"catalog_events", "commerce_events", "fulfillment_events", "ops_events"}
    assert set(phase2) == {"commerce_events"}
    assert set(phase3) == {"commerce_events"}


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
    ]
    for relative_path in expected_files:
        assert (tmp_path / relative_path).is_file()

    samples = json.loads((tmp_path / "derived_topic_samples.json").read_text(encoding="utf-8"))
    assert set(samples) == {
        "realtime_commerce_metrics_1m",
        "realtime_ops_alerts",
        "realtime_metric_corrections",
    }
    assert not (tmp_path / "screenshots").exists()
    assert "flink_ui" not in manifest["service_urls"]
    assert all(not artifact.startswith("screenshots/") for artifact in manifest["artifacts"])
