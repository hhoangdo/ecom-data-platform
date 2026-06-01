from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

import pytest


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[2]


def _load_script_module(script_relative_path: str, module_name: str):
    script_path = _repo_root() / script_relative_path
    assert script_path.is_file(), f"Expected script at {script_relative_path}."
    spec = importlib.util.spec_from_file_location(module_name, script_path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_runtime_limit_defaults_resolve_to_safe_streaming_bounds(monkeypatch) -> None:
    from vina_bim_shop.flink.runtime import load_container_runtime_limit

    monkeypatch.delenv("VBS_FLINK_MAX_RUNTIME_MINUTES", raising=False)
    monkeypatch.delenv("VBS_FLINK_MAX_RUNTIME_GRACE_SECONDS", raising=False)
    monkeypatch.delenv("VBS_FLINK_DISABLE_AUTO_STOP", raising=False)

    config = load_container_runtime_limit()

    assert config.max_runtime_minutes == 45
    assert config.grace_seconds == 30
    assert config.disable_auto_stop is False


def test_reset_cleanroom_state_only_clears_selected_streaming_runtime_surfaces(tmp_path: Path) -> None:
    from vina_bim_shop.flink.verification import reset_cleanroom_state

    compose_commands: list[list[str]] = []
    kafka_cleanup_calls: list[dict[str, object]] = []
    deleted_prefixes: list[tuple[str, str]] = []
    deleted_paths: list[Path] = []

    run_root = tmp_path / "run"
    run_root.mkdir(parents=True)

    reset_cleanroom_state(
        run_command=lambda command: compose_commands.append(command) or "",
        cleanup_kafka_fn=lambda **kwargs: kafka_cleanup_calls.append(kwargs),
        delete_object_prefix=lambda bucket, prefix: deleted_prefixes.append((bucket, prefix)),
        remove_tree=lambda path: deleted_paths.append(Path(path)),
        run_root=run_root,
        checkpoint_bucket="checkpoints",
        checkpoint_prefix="flink",
        curated_output_bucket="evidence",
        curated_output_prefix="streaming_curated",
    )

    assert kafka_cleanup_calls and kafka_cleanup_calls[0]["clean_evidence"] is False
    assert any(command[:3] == ["docker", "compose", "stop"] for command in compose_commands)
    assert not any(command[:4] == ["docker", "compose", "--profile", "serving"] and "-v" in command for command in compose_commands)
    assert deleted_prefixes == [
        ("checkpoints", "flink"),
        ("evidence", "streaming_curated/realtime_metric_corrections"),
        ("evidence", "streaming_curated/realtime_ops_alerts"),
    ]
    assert deleted_paths == [run_root]
    assert all("bronze" not in prefix and "silver" not in prefix and "gold" not in prefix for _bucket, prefix in deleted_prefixes)


def test_build_pre_publish_state_requires_empty_topics_prefixes_and_no_running_jobs() -> None:
    from vina_bim_shop.flink.verification import build_pre_publish_state

    state = build_pre_publish_state(
        topic_counts={
            "realtime_commerce_metrics_1m": 0,
            "realtime_metric_corrections": 0,
            "realtime_ops_alerts": 0,
        },
        checkpoint_listing="",
        curated_listings={
            "realtime_metric_corrections": "",
            "realtime_ops_alerts": "",
        },
        running_jobs=[],
    )

    assert state["is_clean"] is True
    assert state["topic_counts"]["realtime_metric_corrections"] == 0


def test_build_pre_publish_state_marks_run_unclean_when_any_runtime_state_remains() -> None:
    from vina_bim_shop.flink.verification import build_pre_publish_state

    state = build_pre_publish_state(
        topic_counts={
            "realtime_commerce_metrics_1m": 0,
            "realtime_metric_corrections": 1,
            "realtime_ops_alerts": 0,
        },
        checkpoint_listing="checkpoints/flink/commerce_metrics/part-00001",
        curated_listings={
            "realtime_metric_corrections": "",
            "realtime_ops_alerts": "",
        },
        running_jobs=["vina-bim-shop-commerce-metrics"],
    )

    assert state["is_clean"] is False
    assert "realtime_metric_corrections must be empty before publish." in state["failures"]
    assert "No ADR 04 Flink jobs should be running before clean-room replay." in state["failures"]


def test_evaluate_adr04_assertions_requires_exact_counts_and_corrected_snapshot_values() -> None:
    from vina_bim_shop.flink.verification import evaluate_adr04_assertions

    metric_key = "commerce|2026-05-01T10:00:00+00:00|abc123"
    result = evaluate_adr04_assertions(
        topic_counts={
            "realtime_commerce_metrics_1m": 3,
            "realtime_metric_corrections": 1,
            "realtime_ops_alerts": 7,
        },
        metric_rows=[
            {
                "metric_key": metric_key,
                "window_start_ts": "2026-05-01T10:00:00+00:00",
                "window_end_ts": "2026-05-01T10:01:00+00:00",
                "correction_version": 0,
            }
        ],
        correction_rows=[
            {
                "correction_reason": "late_event",
                "correction_version": 1,
                "target_topic": "realtime_commerce_metrics_1m",
                "dimension_hash": "abc",
                "metric_snapshot": {
                    "metric_key": metric_key,
                    "window_start_ts": "2026-05-01T10:00:00+00:00",
                    "window_end_ts": "2026-05-01T10:01:00+00:00",
                    "order_count": 2,
                    "order_placed_count": 2,
                    "revenue_amount": 125000.0,
                    "gmv_proxy_amount": 155000.0,
                    "duplicate_event_count": 1,
                },
            }
        ],
        checkpoint_listing="checkpoints/flink/commerce_metrics/chk-1",
        curated_listings={
            "realtime_metric_corrections": "evidence/streaming_curated/realtime_metric_corrections/event_date=2026-05-01/part-00001.jsonl",
            "realtime_ops_alerts": "evidence/streaming_curated/realtime_ops_alerts/event_date=2026-05-01/part-00001.jsonl",
        },
    )

    assert result["passed"] is True
    assert result["topic_counts"]["realtime_metric_corrections"] == 1


def test_evaluate_adr04_assertions_rejects_missing_correction_row() -> None:
    from vina_bim_shop.flink.verification import evaluate_adr04_assertions

    with pytest.raises(ValueError, match="Expected exactly 1 correction row"):
        evaluate_adr04_assertions(
            topic_counts={
                "realtime_commerce_metrics_1m": 3,
                "realtime_metric_corrections": 0,
                "realtime_ops_alerts": 7,
            },
            metric_rows=[],
            correction_rows=[],
            checkpoint_listing="",
            curated_listings={
                "realtime_metric_corrections": "",
                "realtime_ops_alerts": "",
            },
        )


def test_evaluate_pinot_gate_requires_live_corrections_rows() -> None:
    from vina_bim_shop.flink.verification import evaluate_pinot_gate

    result = evaluate_pinot_gate(
        controller_health={"status": "GOOD"},
        broker_health={"status": "GOOD"},
        row_counts={
            "pinot_realtime_commerce_metrics_1m": 3,
            "pinot_realtime_metric_corrections": 1,
            "pinot_realtime_ops_alerts": 7,
        },
    )

    assert result["passed"] is True

    with pytest.raises(ValueError, match="pinot_realtime_metric_corrections"):
        evaluate_pinot_gate(
            controller_health={"status": "GOOD"},
            broker_health={"status": "GOOD"},
            row_counts={
                "pinot_realtime_commerce_metrics_1m": 3,
                "pinot_realtime_metric_corrections": 0,
                "pinot_realtime_ops_alerts": 7,
            },
        )


def test_cleanup_runtime_state_reclaims_safe_docker_surfaces_only() -> None:
    from vina_bim_shop.flink.verification import cleanup_runtime_state

    commands: list[list[str]] = []
    snapshots: list[dict[str, object]] = []

    result = cleanup_runtime_state(
        run_command=lambda command: commands.append(command) or "",
        capture_storage_snapshot=lambda: snapshots.append({"captured": len(snapshots)}) or snapshots[-1],
    )

    assert result["before"] == {"captured": 0}
    assert result["after"] == {"captured": 1}
    assert any(command[:3] == ["docker", "compose", "stop"] for command in commands)
    assert ["docker", "image", "prune", "-f"] in commands
    assert ["docker", "builder", "prune", "-f", "--filter", "until=168h"] in commands
    joined = [" ".join(command) for command in commands]
    assert not any("volume prune" in command for command in joined)
    assert not any("system prune --volumes" in command for command in joined)


def test_cleanroom_script_parses_phase_and_paths(monkeypatch, tmp_path: Path) -> None:
    module = _load_script_module("scripts/flink/cleanroom_verify.py", "cleanroom_verify_script")

    monkeypatch.setattr(
        sys,
        "argv",
        [
            "cleanroom_verify.py",
            "--phase",
            "verify-adr04",
            "--base-evidence-root",
            str(tmp_path),
        ],
    )

    args = module.parse_args()

    assert args.phase == "verify-adr04"
    assert args.base_evidence_root == str(tmp_path)


def test_gitignore_excludes_runtime_cleanroom_artifacts() -> None:
    gitignore = (_repo_root() / ".gitignore").read_text(encoding="utf-8")

    assert "evidence/runtime/" in gitignore
