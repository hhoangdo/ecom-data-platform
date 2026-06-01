from __future__ import annotations

import os
import subprocess
from pathlib import Path


def render_batch_snapshot_prefix(dataset: str, snapshot_date: str) -> str:
    return f"bronze/batch/{dataset}/snapshot_date={snapshot_date}/"


def render_batch_snapshot_key(dataset: str, snapshot_date: str, filename: str) -> str:
    return f"{render_batch_snapshot_prefix(dataset, snapshot_date)}{filename}"


def render_event_prefix(topic: str, ingest_date: str) -> str:
    return f"bronze/events/{topic}/ingest_date={ingest_date}/"


def render_event_key(topic: str, ingest_date: str, filename: str) -> str:
    return f"{render_event_prefix(topic, ingest_date)}{filename}"


def build_batch_snapshot_upload_commands(
    *,
    raw_root: str | Path,
    snapshot_date: str,
    minio_alias: str,
) -> list[list[str]]:
    raw_path = Path(raw_root)
    commands: list[list[str]] = []
    ignored_datasets = {"bad_snapshots", "kafka_topics"}
    for file_path in sorted(path for path in raw_path.glob("*/*") if path.is_file()):
        dataset = file_path.parent.name
        if dataset in ignored_datasets:
            continue
        destination = f"{minio_alias}/{render_batch_snapshot_key(dataset, snapshot_date, file_path.name)}"
        commands.append(["mc", "cp", str(file_path), destination])
    return commands


def _run_command(command: list[str]) -> None:
    subprocess.run(command, check=True)


def execute_batch_snapshot_uploads(
    *,
    raw_root: str | Path,
    snapshot_date: str,
    minio_alias: str,
    runner=_run_command,
    use_docker_mc: bool = False,
) -> dict[str, object]:
    commands = build_batch_snapshot_upload_commands(
        raw_root=raw_root,
        snapshot_date=snapshot_date,
        minio_alias=minio_alias,
    )
    raw_root_path = Path(raw_root).resolve()
    minio_internal_endpoint = os.getenv("VBS_MINIO_INTERNAL_ENDPOINT", "http://minio:9000")
    for command in commands:
        if use_docker_mc:
            source_path = Path(command[2]).resolve().relative_to(raw_root_path).as_posix()
            runner(
                [
                    "docker",
                    "compose",
                    "run",
                    "--rm",
                    "--no-deps",
                    "-v",
                    f"{raw_root_path}:/workdir:ro",
                    "--entrypoint",
                    "/bin/sh",
                    "minio-init",
                    "-c",
                    f'mc alias set {minio_alias} {minio_internal_endpoint} "$MINIO_ROOT_USER" "$MINIO_ROOT_PASSWORD" >/dev/null && '
                    f'mc cp "/workdir/{source_path}" "{command[3]}"',
                ]
            )
            continue
        runner(command)
    return {
        "uploaded_files": len(commands),
        "snapshot_date": snapshot_date,
        "minio_alias": minio_alias,
        "destinations": [command[-1] for command in commands],
    }
