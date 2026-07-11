from __future__ import annotations

import hashlib
import importlib.util
import json
import sys
from dataclasses import replace
from pathlib import Path

import pytest


def test_experiment_profiles_are_isolated_and_preserve_canonical_optimized_values() -> None:
    from vina_bim_shop.flink.baseline_experiment import load_experiment_profile

    baseline = load_experiment_profile("baseline")
    optimized = load_experiment_profile("optimized")

    assert baseline.job_name == "vina-bim-shop-flink-baseline"
    assert baseline.parallelism == 1
    assert baseline.checkpointing_enabled is False
    assert baseline.out_of_orderness_seconds == 0
    assert set(baseline.allowed_lateness_seconds.values()) == {0}

    assert optimized.job_name == "vina-bim-shop-flink-optimized"
    assert optimized.checkpointing_enabled is True
    assert optimized.out_of_orderness_seconds == 5
    assert optimized.allowed_lateness_seconds == {
        "commerce_events": 300,
        "catalog_events": 600,
        "fulfillment_events": 900,
        "ops_events": 120,
    }

    assert baseline.consumer_groups != optimized.consumer_groups
    assert set(baseline.derived_topics.values()).isdisjoint({"realtime_commerce_metrics_1m", "realtime_metric_corrections", "realtime_ops_alerts"})
    assert set(optimized.derived_topics.values()).isdisjoint({"realtime_commerce_metrics_1m", "realtime_metric_corrections", "realtime_ops_alerts"})
    assert baseline.checkpoint_prefix != optimized.checkpoint_prefix
    assert baseline.curated_output_prefix != optimized.curated_output_prefix


def test_experiment_profile_rejects_unknown_variant() -> None:
    from vina_bim_shop.flink.baseline_experiment import load_experiment_profile

    with pytest.raises(ValueError, match="baseline or optimized"):
        load_experiment_profile("canonical")


def test_optimized_profile_derives_window_size_from_canonical_config() -> None:
    from vina_bim_shop.flink.baseline_experiment import load_experiment_profile, profile_streaming_config
    from vina_bim_shop.flink.config import load_streaming_config

    profile = load_experiment_profile("optimized", streaming_config=replace(load_streaming_config(), window_minutes=2))

    assert profile.window_minutes == 2
    assert profile_streaming_config(profile).window_minutes == 2


def test_compare_runs_requires_same_replay_and_on_time_aggregates() -> None:
    from vina_bim_shop.flink.baseline_experiment import compare_runs

    baseline = {
        "replay_sha256": "same-input",
        "on_time_aggregates": {
            "metric-1": {"order_count": 2, "duplicate_event_count": 1},
        },
        "correction_count": 0,
    }
    optimized = {
        "replay_sha256": "same-input",
        "on_time_aggregates": {
            "metric-1": {"order_count": 2, "duplicate_event_count": 1},
        },
        "correction_count": 1,
    }

    result = compare_runs(baseline, optimized)

    assert result["passed"] is True
    assert result["on_time_aggregates_equal"] is True
    assert result["correction_counts"] == {"baseline": 0, "optimized": 1}

    with pytest.raises(ValueError, match="replay hashes"):
        compare_runs({**baseline, "replay_sha256": "other-input"}, optimized)
    with pytest.raises(ValueError, match="on-time aggregates"):
        compare_runs({**baseline, "on_time_aggregates": {}}, optimized)


def test_write_replay_fixture_preserves_three_phases_and_hashes_exact_bytes(tmp_path) -> None:
    from vina_bim_shop.flink.baseline_experiment import write_replay_fixture

    replay_path = tmp_path / "replay.ndjson"
    manifest = write_replay_fixture(replay_path)

    payload = replay_path.read_bytes()
    rows = [json.loads(line) for line in replay_path.read_text(encoding="utf-8").splitlines()]

    assert manifest["replay_sha256"] == hashlib.sha256(payload).hexdigest()
    assert manifest["phases"] == [
        "initial_business_events",
        "watermark_control_event",
        "late_correction_event",
    ]
    assert [row["phase"] for row in rows[-2:]] == ["watermark_control_event", "late_correction_event"]
    assert rows[-1]["event_id"] == "evt-8"
    assert all(row["value_b64"] for row in rows)


def test_compose_variant_job_adds_commerce_and_ops_with_profiled_runtime() -> None:
    from vina_bim_shop.flink.baseline_experiment import compose_variant_job, load_experiment_profile

    calls: list[tuple[str, object]] = []

    class FakeEnvironment:
        def set_parallelism(self, value: int) -> None:
            calls.append(("parallelism", value))

        def execute(self, job_name: str) -> str:
            calls.append(("execute", job_name))
            return "job-result"

    profile = load_experiment_profile("baseline")
    result = compose_variant_job(
        profile,
        environment_factory=FakeEnvironment,
        add_required_jars_fn=lambda _env: calls.append(("jars", None)),
        configure_checkpointing_fn=lambda _env, checkpoint_uri: calls.append(("checkpoint", checkpoint_uri)),
        add_commerce_pipeline_fn=lambda env, config, runtime, group_id: calls.append(("commerce", group_id)),
        add_ops_pipeline_fn=lambda env, config, runtime, group_ids: calls.append(("ops", group_ids)),
    )

    assert result == "job-result"
    assert ("parallelism", 1) in calls
    assert ("commerce", "vina-bim-shop-commerce-metrics-experiment-baseline") in calls
    assert any(name == "ops" and value["ops"] == "vina-bim-shop-ops-alerts-experiment-baseline" for name, value in calls)
    assert not any(name == "checkpoint" for name, _value in calls)
    assert ("execute", "vina-bim-shop-flink-baseline") in calls


def test_commerce_window_processor_routes_experiment_metrics_to_profiled_topic() -> None:
    from vina_bim_shop.flink.commerce_job import CommerceWindowProcessor, add_commerce_pipeline
    from vina_bim_shop.flink.ops_job import add_ops_pipeline

    class FakeWindow:
        start = 1_777_629_600_000
        end = 1_777_629_660_000

    class FakeContext:
        def window(self):
            return FakeWindow()

        def current_watermark(self) -> int:
            return FakeWindow.end

    processor = CommerceWindowProcessor(
        metrics_topic="realtime_commerce_metrics_1m_baseline",
        correction_topic="realtime_metric_corrections_baseline",
        ops_alerts_topic="realtime_ops_alerts_baseline",
        payment_failure_threshold={"count": 3, "rate": 0.05},
        allowed_lateness_seconds=0,
    )
    event = {
        "event_id": "evt-1",
        "event_type": "order_placed",
        "event_timestamp": "2026-05-01T10:00:15",
        "created_ts": "2026-05-01T10:00:15",
        "schema_version": 1,
        "payload": {
            "primary_category": "FMCG",
            "source": "app",
            "device_type": "app_android",
            "payment_method": "wallet",
            "order_status": "paid",
            "order_net_amount": 125000.0,
            "amount": 125000.0,
        },
    }

    outputs = list(processor.process("key", FakeContext(), [event]))

    assert callable(add_commerce_pipeline)
    assert callable(add_ops_pipeline)
    assert outputs[0]["target_topic"] == "realtime_commerce_metrics_1m_baseline"


def test_baseline_comparison_script_parses_submit_and_execute_modes(monkeypatch, tmp_path: Path) -> None:
    script_path = Path(__file__).resolve().parents[2] / "scripts" / "flink" / "run_baseline_comparison.py"
    assert script_path.is_file()
    spec = importlib.util.spec_from_file_location("run_baseline_comparison_script", script_path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)

    monkeypatch.setattr(
        sys,
        "argv",
        [
            "run_baseline_comparison.py",
            "--variant",
            "baseline",
            "--evidence-root",
            str(tmp_path),
            "--replay-path",
            str(tmp_path / "replay.ndjson"),
        ],
    )
    submit_args = module.parse_args()
    assert submit_args.variant == "baseline"
    assert submit_args.execute_job is False

    monkeypatch.setattr(sys, "argv", ["run_baseline_comparison.py", "--variant", "optimized", "--execute-job"])
    execute_args = module.parse_args()
    assert execute_args.variant == "optimized"
    assert execute_args.execute_job is True


def test_baseline_submission_passes_python_arguments_after_the_script() -> None:
    from vina_bim_shop.flink.baseline_experiment import load_experiment_profile

    script_path = Path(__file__).resolve().parents[2] / "scripts" / "flink" / "run_baseline_comparison.py"
    spec = importlib.util.spec_from_file_location("run_baseline_comparison_command", script_path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)

    command = module.build_submission_command(load_experiment_profile("baseline"))

    assert "--pyArgs" not in command
    assert command[-3:] == ["--variant", "baseline", "--execute-job"]


def test_flink_deliverables_link_ordered_topic04_runtime_proof() -> None:
    repo_root = Path(__file__).resolve().parents[2]
    flink_deliverable = (repo_root / "deliverables" / "06_flink_streaming.md").read_text(encoding="utf-8")
    challenge_deliverable = (repo_root / "deliverables" / "11_solving_data_challenges.md").read_text(encoding="utf-8")

    headings = [line for line in flink_deliverable.splitlines() if line.startswith("### Row 2")]
    assert headings == [
        "### Row 21 - Baseline and Optimized Comparison",
        "### Row 22 - Burst Proof",
        "### Row 23 - Late Arrival Proof",
        "### Row 24 - Duplicate Proof",
        "### Row 25 - Event-Time Window Proof",
    ]
    for artifact in [
        "optimization/baseline_metrics.json",
        "optimization/optimized_metrics.json",
        "optimization/comparison.json",
        "optimization/challenge_samples.json",
        "screenshots/flink_baseline_job.png",
        "screenshots/flink_optimized_job.png",
    ]:
        assert artifact in flink_deliverable
    assert "optimization/report.md" in challenge_deliverable


def test_root_flink_manifest_inventories_topic04_optimization_artifacts() -> None:
    manifest_path = Path(__file__).resolve().parents[2] / "evidence" / "06_flink_streaming" / "run_manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))

    assert "optimization/run_manifest.json" in manifest["artifacts"]
    assert "screenshots/flink_baseline_job.png" in manifest["artifacts"]
    assert "screenshots/flink_optimized_job.png" in manifest["artifacts"]


def test_job_summary_preserves_observed_timing_fields() -> None:
    script_path = Path(__file__).resolve().parents[2] / "scripts" / "flink" / "run_baseline_comparison.py"
    spec = importlib.util.spec_from_file_location("run_baseline_comparison_metrics", script_path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)

    assert module.job_summary({"start-time": 10, "end-time": 30, "duration": 20}) == {
        "start_time_epoch_ms": 10,
        "end_time_epoch_ms": 30,
        "duration_ms": 20,
    }
