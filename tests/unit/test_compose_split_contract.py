from pathlib import Path

import yaml

from compose_model import COMPOSE_INCLUDE_PATHS, load_compose_model


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[2]


def test_root_compose_is_compatibility_include_entrypoint() -> None:
    root_compose = yaml.safe_load((_repo_root() / "docker-compose.yml").read_text(encoding="utf-8"))

    assert root_compose["name"] == "vina-bim-shop"
    assert "services" not in root_compose
    assert [entry["path"] for entry in root_compose["include"]] == list(COMPOSE_INCLUDE_PATHS)
    assert all(entry["project_directory"] == "." for entry in root_compose["include"])


def test_compose_domain_files_own_each_service_once() -> None:
    repo_root = _repo_root()
    service_owners: dict[str, str] = {}

    for include_path in COMPOSE_INCLUDE_PATHS:
        compose = yaml.safe_load((repo_root / include_path).read_text(encoding="utf-8"))
        assert set(compose) == {"services"}
        for service_name in compose["services"]:
            assert service_name not in service_owners
            service_owners[service_name] = include_path

    assert service_owners == {
        "kafka": "compose/ingestion.kafka.yml",
        "schema-registry": "compose/ingestion.kafka.yml",
        "kafka-connect": "compose/ingestion.kafka.yml",
        "kafka-ui": "compose/ingestion.kafka.yml",
        "minio": "compose/lakehouse.yml",
        "minio-init": "compose/lakehouse.yml",
        "lakehouse-postgres": "compose/lakehouse.yml",
        "hive-metastore-init": "compose/lakehouse.yml",
        "hive-metastore": "compose/lakehouse.yml",
        "trino": "compose/lakehouse.yml",
        "trino-worker": "compose/lakehouse.yml",
        "spark-master": "compose/batch.spark.yml",
        "spark-worker": "compose/batch.spark.yml",
        "spark-history-server": "compose/batch.spark.yml",
        "flink-jobmanager": "compose/streaming.flink.yml",
        "flink-taskmanager": "compose/streaming.flink.yml",
        "flink-job-submit": "compose/streaming.flink.yml",
        "pinot-zookeeper": "compose/serving.pinot.yml",
        "pinot-controller": "compose/serving.pinot.yml",
        "pinot-broker": "compose/serving.pinot.yml",
        "pinot-server": "compose/serving.pinot.yml",
        "airflow-webserver": "compose/orchestration.airflow.yml",
        "airflow-scheduler": "compose/orchestration.airflow.yml",
        "airflow-init": "compose/orchestration.airflow.yml",
        "gx-docs": "compose/orchestration.airflow.yml",
        "datahub-elasticsearch": "compose/governance.datahub.yml",
        "datahub-system-update": "compose/governance.datahub.yml",
        "datahub-gms": "compose/governance.datahub.yml",
        "datahub-frontend": "compose/governance.datahub.yml",
        "datahub-actions": "compose/governance.datahub.yml",
    }


def test_root_compose_preserves_shared_named_volumes() -> None:
    compose = load_compose_model(_repo_root())

    assert set(compose["volumes"]) == {
        "kafka_kraft_data",
        "minio_data",
        "lakehouse_postgres_data",
        "datahub_search_data",
        "pinot_zookeeper_data",
        "pinot_zookeeper_datalog",
    }
