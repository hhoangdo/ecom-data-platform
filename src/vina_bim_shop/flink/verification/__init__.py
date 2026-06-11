from __future__ import annotations

from typing import Any, Callable

from ._constants import (
    COMPOSE_PROJECT_NAME,
    DEFAULT_BASE_EVIDENCE_ROOT,
    DERIVED_TOPICS,
    EXPECTED_ADR04_COUNTS,
    INITIAL_ADR04_COUNTS,
    MINIMAL_RUNTIME_SERVICES,
    RUNTIME_CLEANUP_SERVICES,
    SERVING_SERVICES,
    STREAMING_SERVICES,
)
from ._probes import (
    TopicProbeError,
    _consume_topic_rows,
    _container_env_value,
    _delete_minio_prefix,
    _extract_pinot_row_count,
    _has_checkpoint_metadata,
    _health_is_good,
    _list_minio_prefix,
    _remove_docker_volumes,
    _run_command,
    _running_compose_services,
    _safe_minio_listing,
    _safe_topic_counts,
    _topic_counts,
    _topic_counts_with_artifact,
    _topic_message_count,
    _wait_for_flink_runtime,
    _wait_for_json,
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
from .runner import (
    _publish_cleanroom_phase,
    _run_verify_adr04,
    _run_verify_pinot,
    _wait_for_output_state,
    collect_preflight_manifest,
    create_run_root,
    run_cleanroom_verification,
)

RunCommand = Callable[[list[str]], str]
DeleteObjectPrefix = Callable[[str, str], None]

__all__ = [
    "COMPOSE_PROJECT_NAME",
    "DEFAULT_BASE_EVIDENCE_ROOT",
    "DERIVED_TOPICS",
    "EXPECTED_ADR04_COUNTS",
    "INITIAL_ADR04_COUNTS",
    "MINIMAL_RUNTIME_SERVICES",
    "RUNTIME_CLEANUP_SERVICES",
    "RunCommand",
    "DeleteObjectPrefix",
    "SERVING_SERVICES",
    "STREAMING_SERVICES",
    "TopicProbeError",
    "_consume_topic_rows",
    "_container_env_value",
    "_delete_minio_prefix",
    "_extract_pinot_row_count",
    "_has_checkpoint_metadata",
    "_health_is_good",
    "_list_minio_prefix",
    "_publish_cleanroom_phase",
    "_remove_docker_volumes",
    "_run_command",
    "_run_verify_adr04",
    "_run_verify_pinot",
    "_running_compose_services",
    "_safe_minio_listing",
    "_safe_topic_counts",
    "_topic_counts",
    "_topic_counts_with_artifact",
    "_topic_message_count",
    "_wait_for_flink_runtime",
    "_wait_for_json",
    "_wait_for_output_state",
    "_wait_for_pinot_runtime",
    "_write_json",
    "build_pre_publish_state",
    "capture_storage_snapshot",
    "cleanup_runtime_state",
    "collect_preflight_manifest",
    "create_run_root",
    "evaluate_adr04_assertions",
    "evaluate_pinot_gate",
    "reset_cleanroom_state",
    "run_cleanroom_verification",
]


def _legacy_namespace() -> dict[str, Any]:
    return {name: globals()[name] for name in __all__}
