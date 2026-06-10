from pathlib import Path

from compose_model import load_compose_model


def test_root_compose_declares_streaming_services_and_ports() -> None:
    repo_root = Path(__file__).resolve().parents[2]
    compose = load_compose_model(repo_root)

    services = compose["services"]
    expected = {"flink-jobmanager", "flink-taskmanager", "flink-job-submit"}
    assert expected.issubset(services)

    for service_name in expected:
        assert services[service_name]["profiles"] == ["streaming", "all"]

    assert "8086:8081" in services["flink-jobmanager"]["ports"]


def test_streaming_services_depend_on_ingestion_and_lakehouse_primitives() -> None:
    repo_root = Path(__file__).resolve().parents[2]
    compose = load_compose_model(repo_root)
    services = compose["services"]

    assert "kafka" in services["flink-jobmanager"]["depends_on"]
    assert "minio" in services["flink-jobmanager"]["depends_on"]
    assert "flink-jobmanager" in services["flink-taskmanager"]["depends_on"]
    assert "flink-jobmanager" in services["flink-job-submit"]["depends_on"]
    assert "flink-taskmanager" in services["flink-job-submit"]["depends_on"]


def test_env_example_documents_streaming_urls() -> None:
    repo_root = Path(__file__).resolve().parents[2]
    env_example = (repo_root / ".env.example").read_text(encoding="utf-8")

    assert "VBS_FLINK_UI_URL=http://localhost:8086" in env_example
    assert "VBS_FLINK_JOBMANAGER_URL=http://localhost:8086" in env_example
    assert "VBS_FLINK_JOBMANAGER_INTERNAL_URL=http://flink-jobmanager:8081" in env_example
    assert "VBS_FLINK_MAX_RUNTIME_MINUTES=45" in env_example
    assert "VBS_FLINK_MAX_RUNTIME_GRACE_SECONDS=30" in env_example
    assert "VBS_FLINK_DISABLE_AUTO_STOP=false" in env_example


def test_streaming_services_use_runtime_limit_wrapper() -> None:
    repo_root = Path(__file__).resolve().parents[2]
    compose = load_compose_model(repo_root)
    services = compose["services"]

    for service_name in ["flink-jobmanager", "flink-taskmanager", "flink-job-submit"]:
        service = services[service_name]
        assert service["entrypoint"] == ["/opt/flink/bin/run-with-time-limit.sh"]
        environment = service["environment"]
        assert "VBS_FLINK_MAX_RUNTIME_MINUTES" in environment
        assert "VBS_FLINK_MAX_RUNTIME_GRACE_SECONDS" in environment
        assert "VBS_FLINK_DISABLE_AUTO_STOP" in environment
