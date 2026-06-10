from pathlib import Path

from compose_model import load_compose_model


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[2]


def test_root_compose_declares_serving_services_and_ports() -> None:
    compose = load_compose_model(_repo_root())

    services = compose["services"]
    expected = {"pinot-zookeeper", "pinot-controller", "pinot-broker", "pinot-server"}
    assert expected.issubset(services)

    for service_name in expected:
        assert services[service_name]["profiles"] == ["serving", "all"]

    assert "9003:9000" in services["pinot-controller"]["ports"]
    assert "8000:8000" in services["pinot-broker"]["ports"]


def test_serving_services_depend_on_kafka_and_pinot_cluster_primitives() -> None:
    compose = load_compose_model(_repo_root())
    services = compose["services"]

    assert "pinot-zookeeper" in services["pinot-controller"]["depends_on"]
    assert "pinot-controller" in services["pinot-broker"]["depends_on"]
    assert "pinot-controller" in services["pinot-server"]["depends_on"]
    assert "kafka" in services["pinot-server"]["depends_on"]


def test_env_example_documents_pinot_urls() -> None:
    env_example = (_repo_root() / ".env.example").read_text(encoding="utf-8")

    for expected_line in [
        "VBS_PINOT_CONTROLLER_URL=http://localhost:9003",
        "VBS_PINOT_CONTROLLER_INTERNAL_URL=http://pinot-controller:9000",
        "VBS_PINOT_BROKER_URL=http://localhost:8000",
        "VBS_PINOT_BROKER_INTERNAL_URL=http://pinot-broker:8000",
    ]:
        assert expected_line in env_example


def test_serving_profile_declares_persistent_state_volume() -> None:
    compose = load_compose_model(_repo_root())

    assert "pinot_zookeeper_data" in compose["volumes"]
    assert "pinot_zookeeper_datalog" in compose["volumes"]
