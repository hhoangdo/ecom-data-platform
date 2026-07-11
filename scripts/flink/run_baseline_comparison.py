from __future__ import annotations

import argparse
import base64
import hashlib
import json
import re
import subprocess
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import requests

from vina_bim_shop.flink.baseline_experiment import (
    FlinkExperimentProfile,
    compare_runs,
    load_experiment_profile,
    run_experiment_job,
    write_replay_fixture,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run one isolated Flink baseline or optimized comparison variant.")
    parser.add_argument("--variant", choices=["baseline", "optimized"], required=True)
    parser.add_argument("--evidence-root", default="evidence/06_flink_streaming/optimization")
    parser.add_argument("--replay-path")
    parser.add_argument("--bootstrap-servers", default="localhost:9092")
    parser.add_argument("--flink-api-url", default="http://localhost:8086")
    parser.add_argument("--poll-timeout-seconds", type=int, default=240)
    parser.add_argument("--execute-job", action="store_true", help="Run inside the submitted PyFlink process.")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    if args.execute_job:
        run_experiment_job(args.variant)
        return

    evidence_root = Path(args.evidence_root)
    evidence_root.mkdir(parents=True, exist_ok=True)
    profile = load_experiment_profile(args.variant)
    replay_path = Path(args.replay_path) if args.replay_path else evidence_root / "replay.ndjson"
    replay_manifest = _ensure_replay(replay_path, evidence_root / "replay_manifest.json")
    _assert_no_canonical_jobs(args.flink_api_url)
    _reset_variant_state(profile)
    job_id = _submit_variant(profile)
    _wait_for_job_state(job_id, "RUNNING", args.flink_api_url, args.poll_timeout_seconds)

    _publish_phase(replay_path, "initial_business_events", args.bootstrap_servers)
    _publish_phase(replay_path, "watermark_control_event", args.bootstrap_servers)
    initial_rows = _wait_for_outputs(
        profile,
        expected_corrections=0,
        require_checkpoint=profile.checkpointing_enabled,
        timeout_seconds=args.poll_timeout_seconds,
    )
    _publish_phase(replay_path, "late_correction_event", args.bootstrap_servers)
    rows = _wait_for_outputs(
        profile,
        expected_corrections=1 if profile.variant == "optimized" else 0,
        require_checkpoint=profile.checkpointing_enabled,
        timeout_seconds=args.poll_timeout_seconds,
    )
    metrics = _build_metrics(profile, job_id, replay_manifest, initial_rows, rows, args.flink_api_url)
    _cancel_job(job_id, args.flink_api_url, args.poll_timeout_seconds)
    terminal_job = _get_job(job_id, args.flink_api_url)
    metrics["terminal_job_state"] = terminal_job.get("state", "UNKNOWN")
    metrics["job_timing"] = job_summary(terminal_job)
    metrics_path = evidence_root / f"{profile.variant}_metrics.json"
    _write_json(metrics_path, metrics)
    _finalize_comparison(evidence_root)
    print(f"Captured {profile.variant} Flink experiment metrics for job {job_id}.")


def _ensure_replay(replay_path: Path, manifest_path: Path) -> dict[str, Any]:
    if replay_path.exists():
        manifest = json.loads(manifest_path.read_text(encoding="utf-8")) if manifest_path.exists() else {}
        manifest["replay_sha256"] = hashlib.sha256(replay_path.read_bytes()).hexdigest()
    else:
        manifest = write_replay_fixture(replay_path)
    manifest["replay_path"] = str(replay_path)
    _write_json(manifest_path, manifest)
    return manifest


def _assert_no_canonical_jobs(flink_api_url: str) -> None:
    jobs = requests.get(f"{flink_api_url.rstrip('/')}/jobs/overview", timeout=30).json().get("jobs", [])
    forbidden = {"vina-bim-shop-commerce-metrics", "vina-bim-shop-ops-alerts"}
    running = [str(job.get("name", "")) for job in jobs if str(job.get("state", "")) == "RUNNING"]
    collisions = sorted(set(running) & forbidden)
    if collisions:
        raise RuntimeError(f"Canonical Flink jobs are running: {', '.join(collisions)}")


def _reset_variant_state(profile: FlinkExperimentProfile) -> None:
    topics = sorted({*profile.source_topics.values(), *profile.derived_topics.values()})
    for topic in topics:
        _run(
            [
                "docker",
                "compose",
                "exec",
                "-T",
                "kafka",
                "kafka-topics",
                "--bootstrap-server",
                "kafka:29092",
                "--delete",
                "--if-exists",
                "--topic",
                topic,
            ],
            check=False,
        )
    for group_id in profile.consumer_groups.values():
        _run(
            [
                "docker",
                "compose",
                "exec",
                "-T",
                "kafka",
                "kafka-consumer-groups",
                "--bootstrap-server",
                "kafka:29092",
                "--delete",
                "--group",
                group_id,
            ],
            check=False,
        )
    time.sleep(2)
    for topic in topics:
        _run(
            [
                "docker",
                "compose",
                "exec",
                "-T",
                "kafka",
                "kafka-topics",
                "--bootstrap-server",
                "kafka:29092",
                "--create",
                "--if-not-exists",
                "--topic",
                topic,
                "--partitions",
                "1",
                "--replication-factor",
                "1",
                "--config",
                "cleanup.policy=delete",
            ]
        )
    _delete_minio_prefix(profile.checkpoint_bucket, profile.checkpoint_prefix)
    _delete_minio_prefix(profile.curated_output_bucket, profile.curated_output_prefix)


def _submit_variant(profile: FlinkExperimentProfile) -> str:
    output = _run(build_submission_command(profile))
    match = re.search(r"JobID\s+([0-9a-f]+)", output, flags=re.IGNORECASE)
    if not match:
        raise RuntimeError(f"Could not parse Flink job ID from submission output: {output}")
    return match.group(1)


def build_submission_command(profile: FlinkExperimentProfile) -> list[str]:
    return [
        "docker",
        "compose",
        "exec",
        "-T",
        "flink-jobmanager",
        "/opt/flink/bin/flink",
        "run",
        "-d",
        "-m",
        "flink-jobmanager:8081",
        "--jarfile",
        "/opt/flink/usrlib/flink-sql-connector-kafka-3.2.0-1.19.jar",
        "-py",
        "/workspace/scripts/flink/run_baseline_comparison.py",
        "--variant",
        profile.variant,
        "--execute-job",
    ]


def _publish_phase(replay_path: Path, phase: str, bootstrap_servers: str) -> None:
    from confluent_kafka import Producer

    producer = Producer({"bootstrap.servers": bootstrap_servers})
    published = 0
    for line in replay_path.read_text(encoding="utf-8").splitlines():
        row = json.loads(line)
        if row["phase"] != phase:
            continue
        producer.produce(
            str(row["topic"]),
            key=str(row["key"]).encode("utf-8"),
            value=base64.b64decode(str(row["value_b64"])),
        )
        producer.poll(0)
        published += 1
    if producer.flush(30) != 0:
        raise RuntimeError(f"Timed out publishing replay phase {phase}")
    if published == 0:
        raise ValueError(f"Replay phase {phase} has no rows")


def _wait_for_outputs(
    profile: FlinkExperimentProfile,
    *,
    expected_corrections: int,
    require_checkpoint: bool,
    timeout_seconds: int,
) -> dict[str, list[dict[str, Any]]]:
    deadline = time.monotonic() + timeout_seconds
    expected = {
        profile.derived_topics["commerce_metrics"]: 3,
        profile.derived_topics["ops_alerts"]: 7,
        profile.derived_topics["metric_corrections"]: expected_corrections,
    }
    last_counts: dict[str, int] = {}
    while time.monotonic() < deadline:
        last_counts = {topic: _topic_count(topic) for topic in expected}
        if last_counts == expected:
            checkpoint_listing = _list_minio_prefix(profile.checkpoint_bucket, profile.checkpoint_prefix)
            if not require_checkpoint or "/chk-" in checkpoint_listing:
                return {
                    "metrics": _consume_rows(profile.derived_topics["commerce_metrics"], last_counts[profile.derived_topics["commerce_metrics"]]),
                    "alerts": _consume_rows(profile.derived_topics["ops_alerts"], last_counts[profile.derived_topics["ops_alerts"]]),
                    "corrections": _consume_rows(profile.derived_topics["metric_corrections"], last_counts[profile.derived_topics["metric_corrections"]]),
                    "checkpoint_listing": [{"listing": checkpoint_listing}],
                }
        time.sleep(5)
    raise TimeoutError(f"Timed out waiting for {profile.variant} output: {last_counts}")


def _build_metrics(
    profile: FlinkExperimentProfile,
    job_id: str,
    replay_manifest: dict[str, Any],
    initial_rows: dict[str, list[dict[str, Any]]],
    rows: dict[str, list[dict[str, Any]]],
    flink_api_url: str,
) -> dict[str, Any]:
    metric_rows = rows["metrics"]
    checkpoint_listing = str(rows["checkpoint_listing"][0]["listing"])
    on_time_aggregates = {
        str(row["metric_key"]): _stable_metric_row(row)
        for row in metric_rows
        if int(row.get("correction_version", 0)) == 0
    }
    return {
        "captured_at": datetime.now(timezone.utc).isoformat(),
        "variant": profile.variant,
        "job_id": job_id,
        "job_name": profile.job_name,
        "job_state_at_capture": _get_job(job_id, flink_api_url).get("state", "UNKNOWN"),
        "replay_sha256": replay_manifest["replay_sha256"],
        "replay_record_count": replay_manifest.get("record_count"),
        "profile": {
            "parallelism": profile.parallelism,
            "checkpointing_enabled": profile.checkpointing_enabled,
            "out_of_orderness_seconds": profile.out_of_orderness_seconds,
            "allowed_lateness_seconds": profile.allowed_lateness_seconds,
            "consumer_groups": profile.consumer_groups,
            "derived_topics": profile.derived_topics,
            "checkpoint_prefix": profile.checkpoint_prefix,
            "curated_output_prefix": profile.curated_output_prefix,
        },
        "output_counts": {
            "commerce_metrics": len(metric_rows),
            "ops_alerts": len(rows["alerts"]),
            "metric_corrections": len(rows["corrections"]),
        },
        "on_time_aggregates": on_time_aggregates,
        "correction_count": len(rows["corrections"]),
        "checkpoint_count": checkpoint_listing.count("/chk-"),
        "initial_output_counts": {
            "commerce_metrics": len(initial_rows["metrics"]),
            "ops_alerts": len(initial_rows["alerts"]),
            "metric_corrections": len(initial_rows["corrections"]),
        },
        "output_rows": rows,
    }


def _stable_metric_row(row: dict[str, Any]) -> dict[str, Any]:
    fields = (
        "metric_key",
        "window_start_ts",
        "window_end_ts",
        "checkout_started_count",
        "order_placed_count",
        "order_count",
        "payment_failure_count",
        "duplicate_event_count",
        "late_event_count",
        "revenue_amount",
        "gmv_proxy_amount",
    )
    return {field: row.get(field) for field in fields}


def job_summary(job: dict[str, Any]) -> dict[str, int]:
    return {
        "start_time_epoch_ms": int(job.get("start-time", 0)),
        "end_time_epoch_ms": int(job.get("end-time", 0)),
        "duration_ms": int(job.get("duration", 0)),
    }


def _finalize_comparison(evidence_root: Path) -> None:
    baseline_path = evidence_root / "baseline_metrics.json"
    optimized_path = evidence_root / "optimized_metrics.json"
    if not baseline_path.is_file() or not optimized_path.is_file():
        return
    baseline = json.loads(baseline_path.read_text(encoding="utf-8"))
    optimized = json.loads(optimized_path.read_text(encoding="utf-8"))
    comparison = {
        **compare_runs(baseline, optimized),
        "profiles": {"baseline": baseline["profile"], "optimized": optimized["profile"]},
        "job_ids": {"baseline": baseline["job_id"], "optimized": optimized["job_id"]},
    }
    _write_json(evidence_root / "comparison.json", comparison)
    _write_json(evidence_root / "challenge_samples.json", _challenge_samples(baseline, optimized))
    _write_json(
        evidence_root / "run_manifest.json",
        {
            "captured_at": datetime.now(timezone.utc).isoformat(),
            "replay_sha256": comparison["replay_sha256"],
            "job_ids": comparison["job_ids"],
            "artifacts": [
                "replay.ndjson",
                "replay_manifest.json",
                "baseline_metrics.json",
                "optimized_metrics.json",
                "comparison.json",
                "challenge_samples.json",
                "report.md",
            ],
        },
    )


def _challenge_samples(baseline: dict[str, Any], optimized: dict[str, Any]) -> dict[str, Any]:
    baseline_rows = baseline["output_rows"]
    optimized_rows = optimized["output_rows"]
    burst = _find_row(optimized_rows["alerts"], "alert_type", "traffic_burst")
    duplicate = next(row for row in optimized_rows["metrics"] if int(row.get("duplicate_event_count", 0)) == 1)
    correction = optimized_rows["corrections"][0]
    window = next(row for row in optimized_rows["metrics"] if row.get("window_start_ts") == "2026-05-01T10:00:00+00:00")
    return {
        "burst": {"source_event_ids": ["ops-1"], "baseline_output_count": len(baseline_rows["alerts"]), "optimized_output": burst},
        "late_arrival": {
            "source_event_ids": ["evt-8"],
            "baseline_correction_count": len(baseline_rows["corrections"]),
            "optimized_correction": correction,
        },
        "duplicate": {"source_event_ids": ["evt-3", "evt-3"], "optimized_metric": duplicate},
        "window_processing": {"source_event_ids": ["evt-1", "evt-2", "evt-3", "evt-4", "evt-5", "evt-6", "evt-7"], "optimized_metric": window},
    }


def _find_row(rows: list[dict[str, Any]], field: str, value: Any) -> dict[str, Any]:
    return next(row for row in rows if row.get(field) == value)


def _topic_count(topic: str) -> int:
    output = _run(
        [
            "docker",
            "compose",
            "exec",
            "-T",
            "kafka",
            "kafka-get-offsets",
            "--bootstrap-server",
            "kafka:29092",
            "--topic",
            topic,
            "--time",
            "-1",
        ]
    ).strip()
    return sum(int(line.rsplit(":", 1)[-1]) for line in output.splitlines() if line.strip())


def _consume_rows(topic: str, count: int) -> list[dict[str, Any]]:
    if count == 0:
        return []
    output = _run(
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
    )
    return [json.loads(line) for line in output.splitlines() if line.strip()]


def _delete_minio_prefix(bucket: str, prefix: str) -> None:
    _run(
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
            f'&& mc rm --recursive --force "ALIAS/{bucket}/{prefix}" 2>/dev/null || true',
        ]
    )


def _list_minio_prefix(bucket: str, prefix: str) -> str:
    return _run(
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


def _get_job(job_id: str, flink_api_url: str) -> dict[str, Any]:
    response = requests.get(f"{flink_api_url.rstrip('/')}/jobs/{job_id}", timeout=30)
    response.raise_for_status()
    return dict(response.json())


def _wait_for_job_state(job_id: str, expected_state: str, flink_api_url: str, timeout_seconds: int) -> None:
    deadline = time.monotonic() + timeout_seconds
    last_state = "UNKNOWN"
    while time.monotonic() < deadline:
        last_state = str(_get_job(job_id, flink_api_url).get("state", "UNKNOWN"))
        if last_state == expected_state:
            return
        time.sleep(2)
    raise TimeoutError(f"Job {job_id} did not reach {expected_state}; last state was {last_state}")


def _cancel_job(job_id: str, flink_api_url: str, timeout_seconds: int) -> None:
    response = requests.patch(f"{flink_api_url.rstrip('/')}/jobs/{job_id}?mode=cancel", timeout=30)
    response.raise_for_status()
    _wait_for_job_state(job_id, "CANCELED", flink_api_url, timeout_seconds)


def _run(command: list[str], *, check: bool = True) -> str:
    completed = subprocess.run(command, check=check, text=True, capture_output=True)
    return completed.stdout


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")


if __name__ == "__main__":
    main()
