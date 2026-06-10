from pathlib import Path

from compose_model import load_compose_model


def test_root_compose_declares_ingestion_services_and_ports() -> None:
    repo_root = Path(__file__).resolve().parents[2]
    compose = load_compose_model(repo_root)

    services = compose["services"]
    assert {"kafka", "schema-registry", "kafka-connect", "kafka-ui"}.issubset(services)
    for service_name in ["kafka", "schema-registry", "kafka-connect", "kafka-ui"]:
        assert services[service_name]["profiles"] == ["ingestion", "all"]

    assert "9092:9092" in services["kafka"]["ports"]
    assert "8081:8081" in services["schema-registry"]["ports"]
    assert "8083:8083" in services["kafka-connect"]["ports"]
    assert "8084:8080" in services["kafka-ui"]["ports"]


def test_kafka_compose_uses_single_broker_kraft() -> None:
    repo_root = Path(__file__).resolve().parents[2]
    compose = load_compose_model(repo_root)
    kafka_environment = compose["services"]["kafka"]["environment"]

    assert kafka_environment["KAFKA_PROCESS_ROLES"] == "broker,controller"
    assert kafka_environment["KAFKA_NODE_ID"] == "1"
    assert kafka_environment["KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR"] == "1"
    assert kafka_environment["KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR"] == "1"
    assert "zookeeper" not in compose["services"]


def test_env_example_documents_ingestion_urls() -> None:
    repo_root = Path(__file__).resolve().parents[2]
    env_example = (repo_root / ".env.example").read_text(encoding="utf-8")

    assert "VBS_KAFKA_BOOTSTRAP_SERVERS=localhost:9092" in env_example
    assert "VBS_SCHEMA_REGISTRY_URL=http://localhost:8081" in env_example
    assert "VBS_KAFKA_CONNECT_URL=http://localhost:8083" in env_example
    assert "VBS_KAFKA_UI_URL=http://localhost:8084" in env_example
