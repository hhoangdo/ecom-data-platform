from pathlib import Path


def test_streaming_config_preserves_adr04_topic_and_lateness_contract() -> None:
    from vina_bim_shop.flink.config import load_streaming_config

    config = load_streaming_config()

    assert config.source_topics == {
        "commerce": "commerce_events",
        "catalog": "catalog_events",
        "fulfillment": "fulfillment_events",
        "ops": "ops_events",
    }
    assert config.derived_topics == {
        "commerce_metrics": "realtime_commerce_metrics_1m",
        "ops_alerts": "realtime_ops_alerts",
        "metric_corrections": "realtime_metric_corrections",
    }
    assert config.window_minutes == 1
    assert config.out_of_orderness_seconds == 5
    assert config.allowed_lateness_seconds == {
        "commerce_events": 300,
        "catalog_events": 600,
        "fulfillment_events": 900,
        "ops_events": 120,
    }
    assert config.payment_failure_alert_threshold == {
        "count": 3,
        "rate": 0.05,
    }


def test_streaming_config_file_exists_and_uses_expected_buckets() -> None:
    repo_root = Path(__file__).resolve().parents[2]
    config_path = repo_root / "configs" / "pipelines" / "flink_streaming.yaml"

    assert config_path.is_file()

    from vina_bim_shop.flink.config import load_streaming_config

    config = load_streaming_config(config_path)
    assert config.checkpoint_prefix == "flink"
    assert config.curated_output_bucket == "evidence"
    assert config.curated_output_prefix == "streaming_curated"
