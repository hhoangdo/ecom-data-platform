from __future__ import annotations

import json
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from vina_bim_shop.flink.runtime import load_container_runtime_limit
from vina_bim_shop.flink.smoke import build_cleanroom_smoke_phases
from vina_bim_shop.kafka.publisher import publish_topic_events
from vina_bim_shop.pinot.bootstrap import apply_assets
from vina_bim_shop.pinot.evidence import capture_evidence as capture_pinot_evidence

from ._constants import (
    DEFAULT_BASE_EVIDENCE_ROOT,
    DERIVED_TOPICS,
    EXPECTED_ADR04_COUNTS,
    INITIAL_ADR04_COUNTS,
    MINIMAL_RUNTIME_SERVICES,
    SERVING_SERVICES,
    RunCommand,
)
from ._probes import (
    _consume_topic_rows,
    _container_env_value,
    _extract_pinot_row_count,
    _has_checkpoint_metadata,
    _list_minio_prefix,
    _remove_docker_volumes,
    _run_command,
    _running_compose_services,
    _safe_minio_listing,
    _safe_topic_counts,
    _topic_counts_with_artifact,
    _wait_for_flink_runtime,
    _wait_for_pinot_runtime,
    _write_json,
    capture_storage_snapshot,
)
from .assertions import (
    build_pre_publish_state,
    evaluate_adr04_assertions,
    evaluate_pinot_gate,
)
from .cleanroom import cleanup_runtime_state, reset_cleanroom_state


def create_run_root(base_evidence_root: str | Path = DEFAULT_BASE_EVIDENCE_ROOT) -> Path:
    base_path = Path(base_evidence_root)
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    run_root = base_path / timestamp
    run_root.mkdir(parents=True, exist_ok=True)
    return run_root


def collect_preflight_manifest(
    *,
    run_root: Path,
    run_command: RunCommand,
) -> dict[str, Any]:
    runtime_limit = load_container_runtime_limit()
    manifest = {
        "captured_at": datetime.now(timezone.utc).isoformat(),
        "runtime_limit": {
            "max_runtime_minutes": runtime_limit.max_runtime_minutes,
            "grace_seconds": runtime_limit.grace_seconds,
            "disable_auto_stop": runtime_limit.disable_auto_stop,
        },
        "storage_snapshot": capture_storage_snapshot(run_command=run_command),
        "compose_ps": run_command(["docker", "compose", "ps", "-a"]),
        "running_services": _running_compose_services(run_command=run_command),
        "topic_counts": _safe_topic_counts(run_command=run_command),
        "checkpoint_listing": _safe_minio_listing("checkpoints", "flink", run_command=run_command),
        "curated_output_listing": _safe_minio_listing("evidence", "streaming_curated", run_command=run_command),
    }
    _write_json(run_root / "preflight_manifest.json", manifest)
    return manifest


def run_cleanroom_verification(
    *,
    phase: str = "all",
    base_evidence_root: str | Path = DEFAULT_BASE_EVIDENCE_ROOT,
    run_root: Path | None = None,
    include_pinot: bool = False,
    poll_timeout_seconds: int = 240,
    run_command: RunCommand = _run_command,
) -> dict[str, Any]:
    config_root = run_root or create_run_root(base_evidence_root)
    summary: dict[str, Any] = {
        "phase": phase,
        "run_root": str(config_root),
    }

    if phase in {"preflight", "all"}:
        summary["preflight"] = collect_preflight_manifest(run_root=config_root, run_command=run_command)

    if phase in {"reset", "all"}:
        summary["reset"] = reset_cleanroom_state(
            run_command=run_command,
            run_root=config_root,
            checkpoint_bucket="checkpoints",
            checkpoint_prefix="flink",
            curated_output_bucket="evidence",
            curated_output_prefix="streaming_curated",
        )

    if phase in {"verify-adr04", "all"}:
        summary["verify_adr04"] = _run_verify_adr04(run_root=config_root, poll_timeout_seconds=poll_timeout_seconds, run_command=run_command)

    if phase == "verify-pinot" or (phase == "all" and include_pinot):
        summary["verify_pinot"] = _run_verify_pinot(run_root=config_root, run_command=run_command)

    if phase in {"cleanup", "all"}:
        cleanup_summary = cleanup_runtime_state(run_command=run_command)
        _write_json(config_root / "post_cleanup_storage.json", cleanup_summary)
        summary["cleanup"] = cleanup_summary

    _write_json(config_root / "cleanroom_run_summary.json", summary)
    return summary


def _run_verify_adr04(*, run_root: Path, poll_timeout_seconds: int, run_command: RunCommand) -> dict[str, Any]:
    pre_publish_state = build_pre_publish_state(
        topic_counts=_topic_counts_with_artifact(
            run_command=run_command,
            run_root=run_root,
            phase_name="pre_publish_state",
        ),
        checkpoint_listing=_list_minio_prefix("checkpoints", "flink", run_command=run_command),
        curated_listings={
            "realtime_metric_corrections": _list_minio_prefix(
                "evidence",
                "streaming_curated/realtime_metric_corrections",
                run_command=run_command,
            ),
            "realtime_ops_alerts": _list_minio_prefix(
                "evidence",
                "streaming_curated/realtime_ops_alerts",
                run_command=run_command,
            ),
        },
        running_jobs=[service for service in _running_compose_services(run_command=run_command) if service.startswith("flink-")],
    )
    _write_json(run_root / "pre_publish_state.json", pre_publish_state)
    if not pre_publish_state["is_clean"]:
        raise ValueError("Pre-publish clean-room state is not empty.")

    run_command(["docker", "compose", "up", "-d", *MINIMAL_RUNTIME_SERVICES])
    _wait_for_flink_runtime(timeout_seconds=poll_timeout_seconds)

    runtime_limit = {
        "VBS_FLINK_MAX_RUNTIME_MINUTES": _container_env_value("flink-jobmanager", "VBS_FLINK_MAX_RUNTIME_MINUTES", run_command=run_command),
        "VBS_FLINK_MAX_RUNTIME_GRACE_SECONDS": _container_env_value("flink-jobmanager", "VBS_FLINK_MAX_RUNTIME_GRACE_SECONDS", run_command=run_command),
        "VBS_FLINK_DISABLE_AUTO_STOP": _container_env_value("flink-jobmanager", "VBS_FLINK_DISABLE_AUTO_STOP", run_command=run_command),
    }
    _write_json(run_root / "container_runtime_limit.json", runtime_limit)

    phases = build_cleanroom_smoke_phases()
    _publish_cleanroom_phase(phases[0], run_root=run_root)
    _publish_cleanroom_phase(phases[1], run_root=run_root)

    initial_counts, initial_checkpoint_listing, initial_curated_listings = _wait_for_output_state(
        expected_counts=INITIAL_ADR04_COUNTS,
        required_curated_topics=("realtime_ops_alerts",),
        run_command=run_command,
        timeout_seconds=poll_timeout_seconds,
        run_root=run_root,
        phase_name="initial_window_close",
    )
    _write_json(
        run_root / "phase_initial_window_close_state.json",
        {
            "captured_at": datetime.now(timezone.utc).isoformat(),
            "topic_counts": initial_counts,
            "checkpoint_listing": initial_checkpoint_listing,
            "curated_listings": initial_curated_listings,
        },
    )

    _publish_cleanroom_phase(phases[2], run_root=run_root)
    counts, checkpoint_listing, curated_listings = _wait_for_output_state(
        expected_counts=EXPECTED_ADR04_COUNTS,
        required_curated_topics=("realtime_metric_corrections", "realtime_ops_alerts"),
        run_command=run_command,
        timeout_seconds=poll_timeout_seconds,
        run_root=run_root,
        phase_name="late_correction_emission",
    )
    metric_rows = _consume_topic_rows("realtime_commerce_metrics_1m", counts["realtime_commerce_metrics_1m"], run_command=run_command)
    correction_rows = _consume_topic_rows("realtime_metric_corrections", counts["realtime_metric_corrections"], run_command=run_command)
    assertions = evaluate_adr04_assertions(
        topic_counts=counts,
        metric_rows=metric_rows,
        correction_rows=correction_rows,
        checkpoint_listing=checkpoint_listing,
        curated_listings=curated_listings,
    )
    _write_json(run_root / "adr04_cleanroom_assertions.json", assertions)
    return assertions


def _run_verify_pinot(*, run_root: Path, run_command: RunCommand) -> dict[str, Any]:
    adr04_gate_path = run_root / "adr04_cleanroom_assertions.json"
    if not adr04_gate_path.is_file():
        raise ValueError("ADR 04 clean-room assertions must pass before the Pinot gate can run.")
    adr04_gate = json.loads(adr04_gate_path.read_text(encoding="utf-8"))
    if not adr04_gate.get("passed"):
        raise ValueError("ADR 04 clean-room assertions did not pass, so the Pinot gate is blocked.")

    run_command(["docker", "compose", "stop", *SERVING_SERVICES])
    run_command(["docker", "compose", "rm", "-f", "-s", *SERVING_SERVICES])
    _remove_docker_volumes(volume_suffixes=("pinot_zookeeper_data",), run_command=run_command)
    run_command(["docker", "compose", "up", "-d", *SERVING_SERVICES])
    _wait_for_pinot_runtime()

    apply_assets(evidence_root=run_root / "pinot_bootstrap")
    pinot_evidence_root = run_root / "pinot_evidence"
    capture_pinot_evidence(evidence_root=pinot_evidence_root)

    controller_health = json.loads((pinot_evidence_root / "controller_health.json").read_text(encoding="utf-8"))
    broker_health = json.loads((pinot_evidence_root / "broker_health.json").read_text(encoding="utf-8"))
    row_count_payloads = json.loads((pinot_evidence_root / "row_counts.json").read_text(encoding="utf-8"))
    row_counts = {table_name: _extract_pinot_row_count(payload) for table_name, payload in row_count_payloads.items()}
    result = evaluate_pinot_gate(
        controller_health=controller_health,
        broker_health=broker_health,
        row_counts=row_counts,
    )
    _write_json(run_root / "adr05_pinot_gate.json", result)
    _write_json(
        run_root / "adr05_official_evidence_note.json",
        {
            "captured_at": datetime.now(timezone.utc).isoformat(),
            "message": (
                "Clean-room Pinot verification writes runtime-only proof under this run root. "
                "Refresh committed ADR 05 evidence separately with scripts/pinot/refresh_evidence.py."
            ),
        },
    )
    return result


def _wait_for_output_state(
    *,
    expected_counts: dict[str, int],
    required_curated_topics: tuple[str, ...],
    run_command: RunCommand,
    timeout_seconds: int,
    run_root: Path | None = None,
    phase_name: str = "verification",
) -> tuple[dict[str, int], str, dict[str, str]]:
    deadline = time.time() + timeout_seconds
    last_counts = {topic: 0 for topic in DERIVED_TOPICS}
    last_checkpoint_listing = ""
    last_curated = {
        "realtime_metric_corrections": "",
        "realtime_ops_alerts": "",
    }

    while time.time() < deadline:
        last_counts = _topic_counts_with_artifact(
            run_command=run_command,
            run_root=run_root,
            phase_name=phase_name,
        )
        last_checkpoint_listing = _list_minio_prefix("checkpoints", "flink", run_command=run_command)
        last_curated = {
            "realtime_metric_corrections": _list_minio_prefix(
                "evidence",
                "streaming_curated/realtime_metric_corrections",
                run_command=run_command,
            ),
            "realtime_ops_alerts": _list_minio_prefix(
                "evidence",
                "streaming_curated/realtime_ops_alerts",
                run_command=run_command,
            ),
        }
        if last_counts == expected_counts and _has_checkpoint_metadata(last_checkpoint_listing, job_prefix="commerce_metrics") and all(
            last_curated[topic].strip() for topic in required_curated_topics
        ):
            return last_counts, last_checkpoint_listing, last_curated
        time.sleep(5)
    if run_root is not None:
        _write_json(
            run_root / f"phase_{phase_name}_timeout_state.json",
            {
                "captured_at": datetime.now(timezone.utc).isoformat(),
                "expected_counts": expected_counts,
                "topic_counts": last_counts,
                "checkpoint_listing": last_checkpoint_listing,
                "curated_listings": last_curated,
            },
        )
    return last_counts, last_checkpoint_listing, last_curated


def _publish_cleanroom_phase(phase: dict[str, Any], *, run_root: Path) -> dict[str, Any]:
    published_counts = publish_topic_events(
        topic_events=phase["topic_events"],
        bootstrap_servers="localhost:9092",
    )
    summary = {
        "captured_at": datetime.now(timezone.utc).isoformat(),
        "phase": phase["name"],
        "published_counts": published_counts,
        "topics": sorted(phase["topic_events"]),
    }
    _write_json(run_root / f"phase_{phase['name']}_publish_summary.json", summary)
    return summary
