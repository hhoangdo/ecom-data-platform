from pathlib import Path
from typing import Any

import yaml


COMPOSE_INCLUDE_PATHS = (
    "compose/ingestion.kafka.yml",
    "compose/lakehouse.yml",
    "compose/batch.spark.yml",
    "compose/streaming.flink.yml",
    "compose/serving.pinot.yml",
    "compose/orchestration.airflow.yml",
    "compose/governance.datahub.yml",
)


def load_compose_model(repo_root: Path) -> dict[str, Any]:
    root_model = yaml.safe_load((repo_root / "docker-compose.yml").read_text(encoding="utf-8"))
    services: dict[str, Any] = {}

    for include_path in COMPOSE_INCLUDE_PATHS:
        include_model = yaml.safe_load((repo_root / include_path).read_text(encoding="utf-8"))
        for service_name, service in include_model.get("services", {}).items():
            if service_name in services:
                raise AssertionError(f"Duplicate Compose service: {service_name}")
            services[service_name] = service

    merged = dict(root_model)
    merged["services"] = services
    return merged
