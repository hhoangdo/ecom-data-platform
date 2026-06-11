"""``kafka_topic_bootstrap`` DAG runtime.

Bootstraps the Kafka ingestion contract:

- Topics defined in ``infra/kafka/topics.yaml`` are created via
  ``vina_bim_shop.kafka.bootstrap``.
- Schema subjects under ``infra/kafka/schemas/`` are registered through
  Schema Registry.
- The ``source-events-s3-sink`` connector is applied via Kafka Connect
  with the MinIO Bronze bucket as the destination.

Evidence is written under ``evidence/08_airflow_gx/runs/kafka_topic_bootstrap/<run_id>/``.
"""
from __future__ import annotations

import subprocess
from typing import Any

from vina_bim_shop.kafka.bootstrap import bootstrap_topics
from vina_bim_shop.kafka.bronze_sink import register_bronze_sink
from vina_bim_shop.kafka.schema_registry import register_schema_subjects

from .paths import REPO_ROOT, _utc_now, _write_json, build_run_root
from .subprocess_helpers import _get_json, _run_command


def run_kafka_topic_bootstrap(*, run_id: str) -> dict[str, Any]:
    run_root = build_run_root("kafka_topic_bootstrap", run_id)
    bootstrap_topics(
        runner=lambda command: subprocess.run(command, check=True, cwd=str(REPO_ROOT)),
        bootstrap_server="kafka:29092",
        topics_file=REPO_ROOT / "infra" / "kafka" / "topics.yaml",
    )

    schema_responses = register_schema_subjects(
        registry_url="http://schema-registry:8081",
        schemas_dir=REPO_ROOT / "infra" / "kafka" / "schemas",
        evidence_root=run_root,
    )
    connector_response = register_bronze_sink(
        connect_url="http://kafka-connect:8083",
        template_path=REPO_ROOT / "infra" / "kafka" / "connect" / "source-events-s3-sink.template.json",
        connector_name="source-events-s3-sink",
        bronze_bucket="bronze",
        minio_endpoint="http://minio:9000",
        minio_region="us-east-1",
        minio_access_key="vina_minio",
        minio_secret_key="vina_minio_password",
    )

    health = {
        "captured_at": _utc_now(),
        "schema_registry": _get_json("http://schema-registry:8081/subjects"),
        "kafka_connect": _get_json("http://kafka-connect:8083/connectors"),
    }
    topic_list = _run_command(
        ["docker", "compose", "exec", "-T", "kafka", "kafka-topics", "--bootstrap-server", "kafka:29092", "--list"],
        cwd=REPO_ROOT,
    )
    _write_json(run_root / "bootstrap_health.json", health)
    _write_json(run_root / "connector_response.json", connector_response)
    (run_root / "topic_list.txt").write_text(topic_list, encoding="utf-8")
    manifest = {
        "captured_at": _utc_now(),
        "topics_file": "infra/kafka/topics.yaml",
        "registered_subject_count": len(schema_responses),
        "artifacts": [
            "bootstrap_health.json",
            "connector_response.json",
            "topic_list.txt",
        ],
    }
    _write_json(run_root / "run_manifest.json", manifest)
    return manifest
