from __future__ import annotations

from pathlib import Path
from typing import Callable

RunCommand = Callable[[list[str]], str]
DeleteObjectPrefix = Callable[[str, str], None]


DEFAULT_BASE_EVIDENCE_ROOT = Path("evidence/runtime/cleanroom/adr04")
COMPOSE_PROJECT_NAME = "vina-bim-shop"
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
INITIAL_ADR04_COUNTS = {
    "realtime_commerce_metrics_1m": 3,
    "realtime_metric_corrections": 0,
    "realtime_ops_alerts": 7,
}
