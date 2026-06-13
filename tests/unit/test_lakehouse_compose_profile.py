from pathlib import Path

from compose_model import load_compose_model


def test_root_compose_declares_lakehouse_services_and_ports() -> None:
    repo_root = Path(__file__).resolve().parents[2]
    compose = load_compose_model(repo_root)

    services = compose["services"]
    expected_services = {
        "minio",
        "minio-init",
        "lakehouse-postgres",
        "hive-metastore-init",
        "hive-metastore",
        "trino",
        "trino-worker",
    }
    assert expected_services.issubset(services)

    for service_name in expected_services:
        assert {"lakehouse", "all"}.issubset(set(services[service_name]["profiles"]))

    assert "9000:9000" in services["minio"]["ports"]
    assert "9001:9001" in services["minio"]["ports"]
    assert "5433:5432" in services["lakehouse-postgres"]["ports"]
    assert "9083:9083" in services["hive-metastore"]["ports"]
    assert "8080:8080" in services["trino"]["ports"]
    assert "ports" not in services["trino-worker"]


def test_lakehouse_service_dependencies_preserve_catalog_boundaries() -> None:
    repo_root = Path(__file__).resolve().parents[2]
    compose = load_compose_model(repo_root)
    services = compose["services"]

    assert "hive-metastore" in services["trino"]["depends_on"]
    assert "trino" in services["trino-worker"]["depends_on"]
    assert "hive-metastore-init" in services["hive-metastore"]["depends_on"]
    assert "lakehouse-postgres" in services["hive-metastore-init"]["depends_on"]
    assert "minio-init" in services["hive-metastore-init"]["depends_on"]

    trino_catalog_dir = repo_root / "infra" / "lakehouse" / "trino" / "catalog"
    assert (trino_catalog_dir / "iceberg.properties").is_file()
    assert not (trino_catalog_dir / "hive.properties").exists()
    assert not (trino_catalog_dir / "bronze.properties").exists()

    hive_environment = services["hive-metastore"]["environment"]
    assert hive_environment["HIVE_METASTORE_DB"] == "${VBS_HIVE_METASTORE_DB:-hive_metastore}"
    assert hive_environment["HIVE_METASTORE_USER"] == "${VBS_HIVE_METASTORE_USER:-hive_metastore}"
    assert hive_environment["HIVE_METASTORE_PASSWORD"] == "${VBS_HIVE_METASTORE_PASSWORD:-hive_metastore_password}"
    assert hive_environment["HIVE_AUX_JARS_PATH"] == "/opt/hadoop/share/hadoop/tools/lib"
    assert hive_environment["IS_RESUME"] == "true"


def test_lakehouse_compose_declares_persistent_state_volumes() -> None:
    repo_root = Path(__file__).resolve().parents[2]
    compose = load_compose_model(repo_root)

    volumes = compose["volumes"]
    assert "minio_data" in volumes
    assert "lakehouse_postgres_data" in volumes


def test_hive_metastore_image_includes_postgres_and_s3a_runtime_jars() -> None:
    repo_root = Path(__file__).resolve().parents[2]
    compose = load_compose_model(repo_root)
    hive_init_service = compose["services"]["hive-metastore-init"]
    hive_service = compose["services"]["hive-metastore"]

    assert hive_init_service["build"] == {
        "context": "./infra/lakehouse/hive",
        "dockerfile": "Dockerfile",
    }
    assert "build" not in hive_service
    assert hive_init_service["image"] == "vina-bim-shop/hive-metastore:3.1.3-postgres"
    assert hive_service["image"] == "vina-bim-shop/hive-metastore:3.1.3-postgres"

    dockerfile = (repo_root / "infra" / "lakehouse" / "hive" / "Dockerfile").read_text(encoding="utf-8")
    assert "apache/hive:3.1.3" in dockerfile
    assert "postgresql-client" in dockerfile
    assert "postgresql-42.7.4.jar" in dockerfile
    assert "init-schema.sh" in dockerfile
    assert "hadoop-aws-3.1.0.jar" in dockerfile
    assert "aws-java-sdk-bundle-1.11.271.jar" in dockerfile
    assert "custom-entrypoint.sh" not in dockerfile
    assert "*** End of File" not in dockerfile


def test_root_compose_wires_kafka_connect_to_minio_for_bronze_event_landing() -> None:
    repo_root = Path(__file__).resolve().parents[2]
    compose = load_compose_model(repo_root)

    environment = compose["services"]["kafka-connect"]["environment"]
    assert environment["BRONZE_BUCKET"] == "${VBS_BRONZE_BUCKET:-bronze}"
    assert environment["MINIO_ENDPOINT"] == "${VBS_MINIO_INTERNAL_ENDPOINT:-http://minio:9000}"
    assert environment["MINIO_REGION"] == "${VBS_MINIO_REGION:-us-east-1}"
    assert environment["MINIO_ACCESS_KEY"] == "${VBS_MINIO_ROOT_USER:-vina_minio}"
    assert environment["MINIO_SECRET_KEY"] == "${VBS_MINIO_ROOT_PASSWORD:-vina_minio_password}"


def test_batch_profile_can_activate_required_lakehouse_dependencies() -> None:
    repo_root = Path(__file__).resolve().parents[2]
    compose = load_compose_model(repo_root)
    services = compose["services"]

    for service_name in ["minio", "minio-init", "lakehouse-postgres", "hive-metastore", "trino", "trino-worker"]:
        assert "batch" in services[service_name]["profiles"]
