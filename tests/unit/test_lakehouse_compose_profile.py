from pathlib import Path

import yaml


def test_root_compose_declares_lakehouse_services_and_ports() -> None:
    repo_root = Path(__file__).resolve().parents[2]
    compose = yaml.safe_load((repo_root / "docker-compose.yml").read_text(encoding="utf-8"))

    services = compose["services"]
    expected_services = {"minio", "minio-init", "lakehouse-postgres", "hive-metastore", "trino"}
    assert expected_services.issubset(services)

    for service_name in expected_services:
        assert services[service_name]["profiles"] == ["lakehouse", "all"]

    assert "9000:9000" in services["minio"]["ports"]
    assert "9001:9001" in services["minio"]["ports"]
    assert "5433:5432" in services["lakehouse-postgres"]["ports"]
    assert "9083:9083" in services["hive-metastore"]["ports"]
    assert "8080:8080" in services["trino"]["ports"]


def test_lakehouse_service_dependencies_preserve_catalog_boundaries() -> None:
    repo_root = Path(__file__).resolve().parents[2]
    compose = yaml.safe_load((repo_root / "docker-compose.yml").read_text(encoding="utf-8"))
    services = compose["services"]

    assert "hive-metastore" in services["trino"]["depends_on"]
    assert "lakehouse-postgres" in services["hive-metastore"]["depends_on"]
    assert "minio-init" in services["hive-metastore"]["depends_on"]

    trino_catalog_dir = repo_root / "infra" / "lakehouse" / "trino" / "catalog"
    assert (trino_catalog_dir / "iceberg.properties").is_file()
    assert not (trino_catalog_dir / "hive.properties").exists()
    assert not (trino_catalog_dir / "bronze.properties").exists()


def test_lakehouse_compose_declares_persistent_state_volumes() -> None:
    repo_root = Path(__file__).resolve().parents[2]
    compose = yaml.safe_load((repo_root / "docker-compose.yml").read_text(encoding="utf-8"))

    volumes = compose["volumes"]
    assert "minio_data" in volumes
    assert "lakehouse_postgres_data" in volumes


def test_hive_metastore_image_includes_postgres_jdbc_driver() -> None:
    repo_root = Path(__file__).resolve().parents[2]
    compose = yaml.safe_load((repo_root / "docker-compose.yml").read_text(encoding="utf-8"))
    hive_service = compose["services"]["hive-metastore"]

    assert hive_service["build"] == {
        "context": "./infra/lakehouse/hive",
        "dockerfile": "Dockerfile",
    }

    dockerfile = (repo_root / "infra" / "lakehouse" / "hive" / "Dockerfile").read_text(encoding="utf-8")
    assert "apache/hive:4.1.0" in dockerfile
    assert "postgresql-42.7.4.jar" in dockerfile
