from __future__ import annotations

import json
import shutil
import subprocess
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable

import requests

from ._constants import COMPOSE_PROJECT_NAME, DERIVED_TOPICS, RunCommand


class TopicProbeError(RuntimeError):
    def __init__(self, *, topic: str, command: list[str], stderr: str = "", stdout: str = ""):
        message = f"Kafka offset probe failed for {topic} using {' '.join(command)}"
        details = stderr.strip() or stdout.strip()
        if details:
            message = f"{message}: {details}"
        super().__init__(message)
        self.topic = topic
        self.command = command
        self.stderr = stderr
        self.stdout = stdout


def _run_command(command: list[str]) -> str:
    completed = subprocess.run(command, check=True, text=True, capture_output=True)
    return completed.stdout


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")


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


def _health_is_good(payload: dict[str, Any]) -> bool:
    status_candidates = [
        payload.get("status", ""),
        payload.get("text", ""),
        payload.get("body", ""),
    ]
    return any(str(candidate).strip().upper() in {"GOOD", "HEALTHY", "OK"} for candidate in status_candidates)


def _topic_counts(*, run_command: RunCommand) -> dict[str, int]:
    return {topic: _topic_message_count(topic, run_command=run_command) for topic in DERIVED_TOPICS}


def _safe_topic_counts(*, run_command: RunCommand) -> dict[str, Any]:
    try:
        return _topic_counts(run_command=run_command)
    except Exception as exc:  # pragma: no cover - defensive capture for preflight
        return {"error": str(exc)}


def _topic_message_count(topic: str, *, run_command: RunCommand) -> int:
    command = [
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
    try:
        output = run_command(command).strip()
    except subprocess.CalledProcessError as exc:
        raise TopicProbeError(
            topic=topic,
            command=command,
            stderr=getattr(exc, "stderr", "") or "",
            stdout=getattr(exc, "stdout", "") or "",
        ) from exc
    if not output:
        return 0
    total = 0
    for line in output.splitlines():
        total += int(line.rsplit(":", 1)[-1])
    return total


def _topic_counts_with_artifact(
    *,
    run_command: RunCommand,
    run_root: Path | None,
    phase_name: str,
) -> dict[str, int]:
    try:
        return _topic_counts(run_command=run_command)
    except TopicProbeError as exc:
        if run_root is not None:
            _write_json(
                run_root / f"topic_probe_failure_{phase_name}.json",
                {
                    "captured_at": datetime.now(timezone.utc).isoformat(),
                    "phase": phase_name,
                    "topic": exc.topic,
                    "command": exc.command,
                    "stderr": exc.stderr,
                    "stdout": exc.stdout,
                    "message": str(exc),
                },
            )
        raise


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


def _has_checkpoint_metadata(checkpoint_listing: str, *, job_prefix: str) -> bool:
    lines = [line.strip() for line in checkpoint_listing.splitlines() if line.strip()]
    return any(f"{job_prefix}/" in line and "/chk-" in line for line in lines)


def _remove_docker_volumes(*, volume_suffixes: tuple[str, ...], run_command: RunCommand) -> None:
    output = run_command(["docker", "volume", "ls", "--format", "{{.Name}}"]).strip()
    if not output:
        return
    volumes = [line.strip() for line in output.splitlines() if line.strip()]
    matches = [f"{COMPOSE_PROJECT_NAME}_{suffix}" for suffix in volume_suffixes if f"{COMPOSE_PROJECT_NAME}_{suffix}" in volumes]
    if matches:
        run_command(["docker", "volume", "rm", *matches])


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


def _wait_for_json(url: str, predicate: Callable[[dict[str, Any]], bool], *, timeout_seconds: int) -> dict[str, Any]:
    deadline = time.time() + timeout_seconds
    last_payload: dict[str, Any] = {}
    while time.time() < deadline:
        try:
            response = requests.get(url, timeout=30)
        except requests.RequestException as exc:
            last_payload = {"request_error": str(exc)}
            time.sleep(5)
            continue

        try:
            last_payload = response.json()
        except ValueError:
            last_payload = {"status_code": response.status_code, "body": response.text}

        if response.status_code >= 500:
            time.sleep(5)
            continue
        response.raise_for_status()
        if predicate(last_payload):
            return last_payload
        time.sleep(5)
    raise TimeoutError(f"Timed out waiting for {url}. Last payload: {last_payload}")


def _wait_for_flink_runtime(*, timeout_seconds: int) -> None:
    def _jobs_ready(payload: dict[str, Any]) -> bool:
        jobs = {str(job.get("name")) for job in payload.get("jobs", [])}
        return {"vina-bim-shop-commerce-metrics", "vina-bim-shop-ops-alerts"}.issubset(jobs)

    _wait_for_json("http://localhost:8086/overview", lambda payload: int(payload.get("taskmanagers", 0)) >= 1, timeout_seconds=timeout_seconds)
    _wait_for_json("http://localhost:8086/taskmanagers", lambda payload: bool(payload.get("taskmanagers")), timeout_seconds=timeout_seconds)
    _wait_for_json("http://localhost:8086/jobs/overview", _jobs_ready, timeout_seconds=timeout_seconds)


def _wait_for_pinot_runtime(*, timeout_seconds: int = 180) -> None:
    _wait_for_json("http://localhost:9003/health", _health_is_good, timeout_seconds=timeout_seconds)
    _wait_for_json("http://localhost:8000/health", _health_is_good, timeout_seconds=timeout_seconds)
