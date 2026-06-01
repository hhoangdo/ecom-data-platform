from __future__ import annotations

import json
import shutil
import subprocess
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable

import requests

from vina_bim_shop.flink.runtime import load_container_runtime_limit
from vina_bim_shop.flink.smoke import run_streaming_smoke_publish
from vina_bim_shop.kafka.cleanup import cleanup_kafka
from vina_bim_shop.pinot.bootstrap import apply_assets
from vina_bim_shop.pinot.evidence import capture_evidence as capture_pinot_evidence


RunCommand = Callable[[list[str]], str]
DeleteObjectPrefix = Callable[[str, str], None]

DEFAULT_BASE_EVIDENCE_ROOT = Path("evidence/runtime/cleanroom/adr04")
DERIVED_TOPICS = (
    "realtime_commerce_metrics_1m",
    "realtime_metric_corrections",
    "realtime_ops_alerts",
)
STREAMING_SERVICES = (
    "flink-jobmanager",
    "flink-taskmanager",
    "flink-job-submit",
)
SERVING_SERVICES = (
    "pinot-zookeeper",
    "pinot-controller",
    "pinot-broker",
    "pinot-server",
)
MINIMAL_RUNTIME_SERVICES = (
    "kafka",
    "minio",
    "minio-init",
    "flink-jobmanager",
    "flink-taskmanager",
    "flink-job-submit",
)
RUNTIME_CLEANUP_SERVICES = (*SERVING_SERVICES, *STREAMING_SERVICES, "kafka", "minio", "minio-init")
EXPECTED_ADR04_COUNTS = {
    "realtime_commerce_metrics_1m": 3,
    "realtime_metric_corrections": 1,
    "realtime_ops_alerts": 7,
}


def _run_command(command: list[str]) -> str:
    completed = subprocess.run(command, check=True, text=True, capture_output=True)
    return completed.stdout


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")


def create_run_root(base_evidence_root: str | Path = DEFAULT_BASE_EVIDENCE_ROOT) -> Path:
    base_path = Path(base_evidence_root)
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    run_root = base_path / timestamp
    run_root.mkdir(parents=True, exist_ok=True)
    return run_root


def capture_storage_snapshot(*, run_command: RunCommand = _run_command) -> dict[str, Any]:
    anchor = Path.cwd().anchor or "/"
    usage = shutil.disk_usage(anchor)
    return {
        "captured_at": datetime.now(timezone.utc).isoformat(),
        "drive": anchor,
        "free_bytes": usage.free,
        "free_gb": round(usage.free / (1024**3), 3),
        "docker_system_df": run_command(["docker", "system", "df"]),
    }


def collect_preflight_manifest(
    *,
    run_root: Path,
    run_command: RunCommand = _run_command,
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


def reset_cleanroom_state(
    *,
    run_command: RunCommand = _run_command,
    cleanup_kafka_fn: Callable[..., None] = cleanup_kafka,
    delete_object_prefix: DeleteObjectPrefix | None = None,
    remove_tree: Callable[[Path], None] = shutil.rmtree,
    run_root: Path,
    checkpoint_bucket: str,
    checkpoint_prefix: str,
    curated_output_bucket: str,
    curated_output_prefix: str,
) -> dict[str, Any]:
    delete_prefix = delete_object_prefix or (lambda bucket, prefix: _delete_minio_prefix(bucket, prefix, run_command=run_command))
    if run_root.exists():
        remove_tree(run_root)

    run_command(["docker", "compose", "stop", *SERVING_SERVICES])
    run_command(["docker", "compose", "stop", *STREAMING_SERVICES])
    run_command(["docker", "compose", "up", "-d", "kafka", "minio", "minio-init"])

    cleanup_kafka_fn(
        runner=lambda command: subprocess.run(command, check=True),
        bootstrap_server="kafka:29092",
        evidence_root=Path("evidence/03_kafka_ingestion"),
        clean_evidence=False,
    )
    delete_prefix(checkpoint_bucket, checkpoint_prefix)
    delete_prefix(curated_output_bucket, f"{curated_output_prefix}/realtime_metric_corrections")
    delete_prefix(curated_output_bucket, f"{curated_output_prefix}/realtime_ops_alerts")

    manifest = {
        "captured_at": datetime.now(timezone.utc).isoformat(),
        "checkpoint_prefix": f"{checkpoint_bucket}/{checkpoint_prefix}",
        "curated_prefixes": [
            f"{curated_output_bucket}/{curated_output_prefix}/realtime_metric_corrections",
            f"{curated_output_bucket}/{curated_output_prefix}/realtime_ops_alerts",
        ],
    }
    run_root.mkdir(parents=True, exist_ok=True)
    _write_json(run_root / "reset_manifest.json", manifest)
    return manifest


def build_pre_publish_state(
    *,
    topic_counts: dict[str, int],
    checkpoint_listing: str,
    curated_listings: dict[str, str],
    running_jobs: list[str],
) -> dict[str, Any]:
    failures: list[str] = []
    for topic in DERIVED_TOPICS:
        if int(topic_counts.get(topic, 0)) != 0:
            failures.append(f"{topic} must be empty before publish.")
    if checkpoint_listing.strip():
        failures.append("checkpoints/flink/ must be empty before publish.")
    if curated_listings.get("realtime_metric_corrections", "").strip():
        failures.append("evidence/streaming_curated/realtime_metric_corrections must be empty before publish.")
    if curated_listings.get("realtime_ops_alerts", "").strip():
        failures.append("evidence/streaming_curated/realtime_ops_alerts must be empty before publish.")
    if running_jobs:
        failures.append("No ADR 04 Flink jobs should be running before clean-room replay.")
    return {
        "captured_at": datetime.now(timezone.utc).isoformat(),
        "is_clean": not failures,
        "topic_counts": topic_counts,
        "checkpoint_listing": checkpoint_listing,
        "curated_listings": curated_listings,
        "running_jobs": running_jobs,
        "failures": failures,
    }


def evaluate_adr04_assertions(
    *,
    topic_counts: dict[str, int],
    metric_rows: list[dict[str, Any]],
    correction_rows: list[dict[str, Any]],
    checkpoint_listing: str,
    curated_listings: dict[str, str],
) -> dict[str, Any]:
    for topic, expected_count in EXPECTED_ADR04_COUNTS.items():
        actual_count = int(topic_counts.get(topic, 0))
        if actual_count != expected_count:
            if topic == "realtime_metric_corrections":
                raise ValueError(f"Expected exactly 1 correction row in {topic}, found {actual_count}.")
            raise ValueError(f"Expected {expected_count} rows in {topic}, found {actual_count}.")

    if len(correction_rows) != 1:
        raise ValueError(f"Expected exactly 1 correction row in realtime_metric_corrections, found {len(correction_rows)}.")

    correction = correction_rows[0]
    if correction.get("correction_reason") != "late_event":
        raise ValueError("Correction row must use correction_reason=late_event.")
    if int(correction.get("correction_version", -1)) != 1:
        raise ValueError("Correction row must use correction_version=1.")
    if correction.get("target_topic") != "realtime_commerce_metrics_1m":
        raise ValueError("Correction row must target realtime_commerce_metrics_1m.")
    if not str(correction.get("dimension_hash", "")).strip():
        raise ValueError("Correction row must include a non-empty dimension_hash.")

    snapshot = dict(correction.get("metric_snapshot", {}))
    metric_keys = {str(row.get("metric_key")) for row in metric_rows}
    if str(snapshot.get("metric_key")) not in metric_keys:
        raise ValueError("Correction metric_snapshot.metric_key must match one initial realtime_commerce_metrics_1m row.")

    expected_snapshot = {
        "window_start_ts": "2026-05-01T10:00:00+00:00",
        "order_count": 2,
        "order_placed_count": 2,
        "revenue_amount": 125000.0,
        "gmv_proxy_amount": 155000.0,
        "duplicate_event_count": 1,
    }
    for field, expected_value in expected_snapshot.items():
        if snapshot.get(field) != expected_value:
            raise ValueError(f"Correction metric_snapshot.{field} must equal {expected_value!r}.")

    if "checkpoints/flink/commerce_metrics/" not in checkpoint_listing:
        raise ValueError("At least one checkpoint object must exist under checkpoints/flink/commerce_metrics/.")
    if not curated_listings.get("realtime_metric_corrections", "").strip():
        raise ValueError("Curated realtime_metric_corrections JSONL output must exist in MinIO.")
    if not curated_listings.get("realtime_ops_alerts", "").strip():
        raise ValueError("Curated realtime_ops_alerts JSONL output must exist in MinIO.")

    return {
        "captured_at": datetime.now(timezone.utc).isoformat(),
        "passed": True,
        "topic_counts": topic_counts,
        "correction_row": correction,
    }


def evaluate_pinot_gate(
    *,
    controller_health: dict[str, Any],
    broker_health: dict[str, Any],
    row_counts: dict[str, int],
) -> dict[str, Any]:
    controller_status = str(controller_health.get("status", "")).upper()
    broker_status = str(broker_health.get("status", "")).upper()
    if controller_status not in {"GOOD", "HEALTHY", "OK"}:
        raise ValueError("Pinot controller health is not GOOD.")
    if broker_status not in {"GOOD", "HEALTHY", "OK"}:
        raise ValueError("Pinot broker health is not GOOD.")
    for table_name in [
        "pinot_realtime_commerce_metrics_1m",
        "pinot_realtime_metric_corrections",
        "pinot_realtime_ops_alerts",
    ]:
        if int(row_counts.get(table_name, 0)) <= 0:
            raise ValueError(f"{table_name} must contain live rows after Pinot bootstrap.")
    return {
        "captured_at": datetime.now(timezone.utc).isoformat(),
        "passed": True,
        "row_counts": row_counts,
    }


def cleanup_runtime_state(
    *,
    run_command: RunCommand = _run_command,
    capture_storage_snapshot: Callable[[], dict[str, Any]] | None = None,
) -> dict[str, Any]:
    snapshot_fn = capture_storage_snapshot or (lambda: globals()["capture_storage_snapshot"](run_command=run_command))
    before = snapshot_fn()
    run_command(["docker", "compose", "stop", *RUNTIME_CLEANUP_SERVICES])
    run_command(["docker", "compose", "rm", "-f", "-s", *RUNTIME_CLEANUP_SERVICES])
    run_command(["docker", "image", "prune", "-f"])
    run_command(["docker", "builder", "prune", "-f", "--filter", "until=168h"])
    after = snapshot_fn()
    return {
        "captured_at": datetime.now(timezone.utc).isoformat(),
        "before": before,
        "after": after,
    }


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
        topic_counts=_topic_counts(run_command=run_command),
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

    smoke_summary = run_streaming_smoke_publish(bootstrap_servers="localhost:9092", evidence_root=run_root)
    _write_json(run_root / "smoke_publish_summary.json", smoke_summary)

    counts, checkpoint_listing, curated_listings = _wait_for_expected_outputs(run_command=run_command, timeout_seconds=poll_timeout_seconds)
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

    run_command(["docker", "compose", "--profile", "serving", "down", "-v"])
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
    return result


def _wait_for_expected_outputs(*, run_command: RunCommand, timeout_seconds: int) -> tuple[dict[str, int], str, dict[str, str]]:
    deadline = time.time() + timeout_seconds
    last_counts = _topic_counts(run_command=run_command)
    last_checkpoint_listing = ""
    last_curated = {
        "realtime_metric_corrections": "",
        "realtime_ops_alerts": "",
    }

    while time.time() < deadline:
        last_counts = _topic_counts(run_command=run_command)
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
        if last_counts == EXPECTED_ADR04_COUNTS and "checkpoints/flink/commerce_metrics/" in last_checkpoint_listing and all(
            listing.strip() for listing in last_curated.values()
        ):
            return last_counts, last_checkpoint_listing, last_curated
        time.sleep(5)
    return last_counts, last_checkpoint_listing, last_curated


def _wait_for_flink_runtime(*, timeout_seconds: int) -> None:
    def _jobs_ready(payload: dict[str, Any]) -> bool:
        jobs = {str(job.get("name")) for job in payload.get("jobs", [])}
        return {"vina-bim-shop-commerce-metrics", "vina-bim-shop-ops-alerts"}.issubset(jobs)

    _wait_for_json("http://localhost:8086/overview", lambda payload: int(payload.get("taskmanagers", 0)) >= 1, timeout_seconds=timeout_seconds)
    _wait_for_json("http://localhost:8086/taskmanagers", lambda payload: bool(payload.get("taskmanagers")), timeout_seconds=timeout_seconds)
    _wait_for_json("http://localhost:8086/jobs/overview", _jobs_ready, timeout_seconds=timeout_seconds)


def _wait_for_pinot_runtime(*, timeout_seconds: int = 180) -> None:
    _wait_for_json("http://localhost:9003/health", lambda payload: str(payload.get("status", "")).upper() in {"GOOD", "HEALTHY"}, timeout_seconds=timeout_seconds)
    _wait_for_json("http://localhost:8000/health", lambda payload: str(payload.get("status", "")).upper() in {"GOOD", "HEALTHY"}, timeout_seconds=timeout_seconds)


def _wait_for_json(url: str, predicate: Callable[[dict[str, Any]], bool], *, timeout_seconds: int) -> dict[str, Any]:
    deadline = time.time() + timeout_seconds
    last_payload: dict[str, Any] = {}
    while time.time() < deadline:
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        last_payload = response.json()
        if predicate(last_payload):
            return last_payload
        time.sleep(5)
    raise TimeoutError(f"Timed out waiting for {url}. Last payload: {last_payload}")


def _topic_counts(*, run_command: RunCommand) -> dict[str, int]:
    return {topic: _topic_message_count(topic, run_command=run_command) for topic in DERIVED_TOPICS}


def _safe_topic_counts(*, run_command: RunCommand) -> dict[str, Any]:
    try:
        return _topic_counts(run_command=run_command)
    except Exception as exc:  # pragma: no cover - defensive capture for preflight
        return {"error": str(exc)}


def _topic_message_count(topic: str, *, run_command: RunCommand) -> int:
    output = run_command(
        [
            "docker",
            "compose",
            "exec",
            "-T",
            "kafka",
            "kafka-run-class",
            "kafka.tools.GetOffsetShell",
            "--broker-list",
            "kafka:29092",
            "--topic",
            topic,
            "--time",
            "-1",
        ]
    ).strip()
    if not output:
        return 0
    total = 0
    for line in output.splitlines():
        total += int(line.rsplit(":", 1)[-1])
    return total


def _consume_topic_rows(topic: str, count: int, *, run_command: RunCommand) -> list[dict[str, Any]]:
    if count <= 0:
        return []
    output = run_command(
        [
            "docker",
            "compose",
            "exec",
            "-T",
            "kafka",
            "kafka-console-consumer",
            "--bootstrap-server",
            "kafka:29092",
            "--topic",
            topic,
            "--from-beginning",
            "--max-messages",
            str(count),
            "--timeout-ms",
            "20000",
        ]
    ).strip()
    if not output:
        return []
    return [json.loads(line) for line in output.splitlines() if line.strip()]


def _running_compose_services(*, run_command: RunCommand) -> list[str]:
    output = run_command(["docker", "compose", "ps", "--services", "--status", "running"]).strip()
    if not output:
        return []
    return [line.strip() for line in output.splitlines() if line.strip()]


def _list_minio_prefix(bucket: str, prefix: str, *, run_command: RunCommand) -> str:
    return run_command(
        [
            "docker",
            "compose",
            "run",
            "--rm",
            "--no-deps",
            "--entrypoint",
            "/bin/sh",
            "minio-init",
            "-c",
            'mc alias set ALIAS http://minio:9000 "${MINIO_ROOT_USER}" "${MINIO_ROOT_PASSWORD}" >/dev/null '
            f'&& mc ls --recursive "ALIAS/{bucket}/{prefix}" 2>/dev/null || true',
        ]
    )


def _safe_minio_listing(bucket: str, prefix: str, *, run_command: RunCommand) -> dict[str, str]:
    try:
        return {"bucket": bucket, "prefix": prefix, "listing": _list_minio_prefix(bucket, prefix, run_command=run_command)}
    except Exception as exc:  # pragma: no cover - defensive capture for preflight
        return {"bucket": bucket, "prefix": prefix, "error": str(exc)}


def _delete_minio_prefix(bucket: str, prefix: str, *, run_command: RunCommand) -> None:
    run_command(
        [
            "docker",
            "compose",
            "run",
            "--rm",
            "--no-deps",
            "--entrypoint",
            "/bin/sh",
            "minio-init",
            "-c",
            'mc alias set ALIAS http://minio:9000 "${MINIO_ROOT_USER}" "${MINIO_ROOT_PASSWORD}" >/dev/null '
            f'&& (mc ls "ALIAS/{bucket}/{prefix}" >/dev/null 2>&1 && mc rm --recursive --force "ALIAS/{bucket}/{prefix}" || true)',
        ]
    )


def _container_env_value(container: str, variable_name: str, *, run_command: RunCommand) -> str:
    return run_command(
        [
            "docker",
            "compose",
            "exec",
            "-T",
            container,
            "/bin/sh",
            "-lc",
            f'printenv {variable_name}',
        ]
    ).strip()


def _extract_pinot_row_count(payload: Any) -> int:
    if isinstance(payload, int):
        return payload
    rows = payload.get("resultTable", {}).get("rows", [])
    if not rows:
        return 0
    return int(rows[0][0])
