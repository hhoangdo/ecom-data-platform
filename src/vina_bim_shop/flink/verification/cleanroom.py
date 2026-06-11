from __future__ import annotations

import shutil
import subprocess
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable

from vina_bim_shop.kafka.cleanup import cleanup_kafka

from ._constants import (
    RUNTIME_CLEANUP_SERVICES,
    SERVING_SERVICES,
    STREAMING_SERVICES,
    DeleteObjectPrefix,
    RunCommand,
)
from ._probes import (
    _delete_minio_prefix,
    _write_json,
    capture_storage_snapshot as _capture_storage_snapshot,
)


def _run_command(command: list[str]) -> str:
    completed = subprocess.run(command, check=True, text=True, capture_output=True)
    return completed.stdout


def reset_cleanroom_state(
    *,
    run_command: RunCommand,
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


def cleanup_runtime_state(
    *,
    run_command: RunCommand = _run_command,
    capture_storage_snapshot: Callable[[], dict[str, Any]] | None = None,
) -> dict[str, Any]:
    snapshot_fn = capture_storage_snapshot or (lambda: _capture_storage_snapshot(run_command=run_command))
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
