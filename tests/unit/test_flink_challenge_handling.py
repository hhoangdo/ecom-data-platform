from __future__ import annotations

from pathlib import Path

import yaml


REPO_ROOT = Path(__file__).resolve().parents[2]
RUNTIME_PATH = REPO_ROOT / "src" / "vina_bim_shop" / "flink" / "runtime.py"
COMMERCE_PATH = REPO_ROOT / "src" / "vina_bim_shop" / "flink" / "commerce_job.py"
OPS_PATH = REPO_ROOT / "src" / "vina_bim_shop" / "flink" / "ops_job.py"
METRICS_PATH = REPO_ROOT / "src" / "vina_bim_shop" / "flink" / "metrics.py"
CORRECTIONS_PATH = REPO_ROOT / "src" / "vina_bim_shop" / "flink" / "corrections.py"
ALERTS_PATH = REPO_ROOT / "src" / "vina_bim_shop" / "flink" / "alerts.py"
CONFIG_PATH = REPO_ROOT / "configs" / "pipelines" / "flink_streaming.yaml"


def _read(path: Path) -> str:
    return path.read_text(encoding="utf-8")


# ---------------------------------------------------------------------------------
# 1. Watermark strategy
# ---------------------------------------------------------------------------------


def test_event_timestamp_assigner_uses_bounded_out_of_orderness() -> None:
    runtime_text = _read(RUNTIME_PATH)
    assert "WatermarkStrategy.for_bounded_out_of_orderness" in runtime_text
    assert "Duration.of_seconds(out_of_orderness_seconds)" in runtime_text
    assert "with_timestamp_assigner(EventTimestampAssigner())" in runtime_text


def test_event_timestamp_millis_parses_iso_string() -> None:
    from vina_bim_shop.flink.runtime import event_timestamp_millis

    iso_value = "2026-05-01T10:00:15"
    expected = 1777629615000
    assert event_timestamp_millis({"event_timestamp": iso_value}) == expected
    import json as _json

    assert event_timestamp_millis(_json.dumps({"event_timestamp": iso_value})) == expected


# ---------------------------------------------------------------------------------
# 2. Checkpointing
# ---------------------------------------------------------------------------------


def test_configure_checkpointing_enables_30s_exactly_once() -> None:
    runtime_text = _read(RUNTIME_PATH)
    assert "env.enable_checkpointing(30_000)" in runtime_text
    assert "CheckpointingMode.EXACTLY_ONCE" in runtime_text
    assert "set_min_pause_between_checkpoints(15_000)" in runtime_text
    assert "FileSystemCheckpointStorage" in runtime_text


# ---------------------------------------------------------------------------------
# 3. Tumbling event-time windows and per-topic allowed lateness
# ---------------------------------------------------------------------------------


def test_commerce_job_uses_one_minute_tumbling_event_time_window() -> None:
    commerce_text = _read(COMMERCE_PATH)
    assert "TumblingEventTimeWindows.of(Time.minutes(config.window_minutes))" in commerce_text
    assert ".allowed_lateness(" in commerce_text


def test_commerce_job_allowed_lateness_is_per_topic() -> None:
    commerce_text = _read(COMMERCE_PATH)
    assert "config.allowed_lateness_seconds[config.source_topics[\"commerce\"]]" in commerce_text


def test_commerce_job_key_by_includes_schema_version_and_dims() -> None:
    commerce_text = _read(COMMERCE_PATH)
    # Grab the full key_by block by counting parentheses from ".key_by(".
    start = commerce_text.find(".key_by(")
    assert start >= 0
    depth = 0
    end = -1
    for offset, char in enumerate(commerce_text[start:], start=start):
        if char == "(":
            depth += 1
        elif char == ")":
            depth -= 1
            if depth == 0:
                end = offset
                break
    assert end > start
    key_section = commerce_text[start:end + 1]
    for field in ["primary_category", "source", "device_type", "payment_method", "order_status"]:
        assert f'"{field}"' in key_section
    assert "schema_version" in key_section


def test_commerce_job_sinks_to_three_derived_kafka_topics() -> None:
    commerce_text = _read(COMMERCE_PATH)
    assert "kafka_sink(topic=config.derived_topics[\"commerce_metrics\"]" in commerce_text
    assert "kafka_sink(topic=config.derived_topics[\"metric_corrections\"]" in commerce_text
    assert "kafka_sink(topic=config.derived_topics[\"ops_alerts\"]" in commerce_text


# ---------------------------------------------------------------------------------
# 4. Dedupe
# ---------------------------------------------------------------------------------


def test_dedupe_events_dedupes_by_event_id() -> None:
    metrics_text = _read(METRICS_PATH)
    assert "def dedupe_events(events:" in metrics_text
    assert "event_id" in metrics_text
    assert "duplicate_count" in metrics_text


def test_build_metric_snapshot_exposes_duplicate_and_late_counts() -> None:
    metrics_text = _read(METRICS_PATH)
    assert "duplicate_event_count" in metrics_text
    assert "late_event_count" in metrics_text


def test_dimension_fields_include_schema_version_and_nullable_dims() -> None:
    metrics_text = _read(METRICS_PATH)
    assert "DIMENSION_FIELDS" in metrics_text
    for field in ["primary_category", "source", "device_type", "payment_method", "order_status", "schema_version"]:
        assert f'"{field}"' in metrics_text


# ---------------------------------------------------------------------------------
# 5. Corrections
# ---------------------------------------------------------------------------------


def test_next_correction_version_increments() -> None:
    from vina_bim_shop.flink.corrections import next_correction_version

    assert next_correction_version(previous_version=0) == 1
    assert next_correction_version(previous_version=2) == 3


def test_build_correction_record_carries_version_reason_and_snapshot() -> None:
    corrections_text = _read(CORRECTIONS_PATH)
    assert "correction_version" in corrections_text
    assert "correction_reason" in corrections_text
    assert "metric_snapshot" in corrections_text
    assert "dimension_hash" in corrections_text


# ---------------------------------------------------------------------------------
# 6. Ops signals
# ---------------------------------------------------------------------------------


def test_ops_job_filters_known_ops_signal_types() -> None:
    ops_text = _read(OPS_PATH)
    for event_type in [
        "traffic_burst_detected",
        "late_arrival_observed",
        "duplicate_event_observed",
        "inventory_low_stock",
        "shipment_delayed",
        "shipment_blocked_payment_failed",
    ]:
        assert event_type in ops_text


def test_alerts_map_known_ops_event_types() -> None:
    alerts_text = _read(ALERTS_PATH)
    for event_type in [
        "traffic_burst_detected",
        "late_arrival_observed",
        "duplicate_event_observed",
    ]:
        assert event_type in alerts_text


# ---------------------------------------------------------------------------------
# 7. YAML config matches code expectations
# ---------------------------------------------------------------------------------


def test_flink_yaml_declares_three_derived_topics() -> None:
    config = yaml.safe_load(CONFIG_PATH.read_text(encoding="utf-8"))
    assert config["derived_topics"]["commerce_metrics"] == "realtime_commerce_metrics_1m"
    assert config["derived_topics"]["metric_corrections"] == "realtime_metric_corrections"
    assert config["derived_topics"]["ops_alerts"] == "realtime_ops_alerts"


def test_flink_yaml_declares_per_topic_allowed_lateness() -> None:
    config = yaml.safe_load(CONFIG_PATH.read_text(encoding="utf-8"))
    assert config["window_minutes"] == 1
    assert config["out_of_orderness_seconds"] == 5
    allowed = config["allowed_lateness_seconds"]
    assert allowed["commerce_events"] == 300
    assert allowed["catalog_events"] == 600
    assert allowed["fulfillment_events"] == 900
    assert allowed["ops_events"] == 120


# ---------------------------------------------------------------------------------
# 8. Cross-link to solution map
# ---------------------------------------------------------------------------------


def test_flink_deliverable_mentions_required_behaviors() -> None:
    text = (REPO_ROOT / "deliverables" / "06_flink_streaming.md").read_text(encoding="utf-8")
    assert "event time" in text.lower()
    assert "watermark" in text.lower()
    assert "allowed lateness" in text.lower() or "allowed_lateness" in text.lower()
    assert "dedupe" in text.lower()
    assert "correction" in text.lower()
    assert "checkpoint" in text.lower()


def test_data_generator_deliverable_links_to_solution_map() -> None:
    text = (REPO_ROOT / "deliverables" / "01_data_generator.md").read_text(encoding="utf-8")
    assert "11_solving_data_challenges.md" in text


def test_deliverable_11_solving_data_challenges_mentions_flink_features() -> None:
    text = (REPO_ROOT / "deliverables" / "11_solving_data_challenges.md").read_text(encoding="utf-8")
    assert "WatermarkStrategy.for_bounded_out_of_orderness" in text
    assert "EXACTLY_ONCE" in text
    assert "TumblingEventTimeWindows" in text
    assert "dedupe_events" in text
    assert "build_correction_record" in text
