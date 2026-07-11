from pathlib import Path

from compose_model import load_compose_model


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[2]


def test_root_compose_declares_orchestration_services_and_ports() -> None:
    compose = load_compose_model(_repo_root())

    services = compose["services"]
    expected = {"airflow-webserver", "airflow-scheduler", "airflow-init", "gx-docs"}
    assert expected.issubset(services)

    for service_name in expected:
        assert services[service_name]["profiles"] == ["orchestration", "all"]

    assert "8082:8080" in services["airflow-webserver"]["ports"]
    assert "8088:80" in services["gx-docs"]["ports"]


def test_orchestration_services_reuse_shared_postgres_and_repo_workspace() -> None:
    compose = load_compose_model(_repo_root())
    services = compose["services"]

    assert "orchestration" in services["lakehouse-postgres"]["profiles"]
    assert "airflow-init" in services["airflow-webserver"]["depends_on"]
    assert "airflow-init" in services["airflow-scheduler"]["depends_on"]

    for service_name in ["airflow-webserver", "airflow-scheduler", "airflow-init"]:
        service = services[service_name]
        assert service["build"] == {
            "context": ".",
            "dockerfile": "./infra/orchestration/airflow/Dockerfile",
        }
        assert "./infra/orchestration/airflow/dags:/opt/airflow/dags:ro" in service["volumes"]
        assert "./src:/workspace/src:ro" in service["volumes"]
        assert "/var/run/docker.sock:/var/run/docker.sock" in service["volumes"]


def test_env_example_documents_orchestration_urls_and_credentials() -> None:
    env_example = (_repo_root() / ".env.example").read_text(encoding="utf-8")

    for expected_line in [
        "VBS_AIRFLOW_UI_URL=http://localhost:8082",
        "VBS_GX_DOCS_URL=http://localhost:8088",
        "VBS_AIRFLOW_ADMIN_USERNAME=airflow",
        "VBS_AIRFLOW_ADMIN_PASSWORD=airflow",
    ]:
        assert expected_line in env_example


def test_airflow_runtime_assets_exist() -> None:
    repo_root = _repo_root()
    assert (repo_root / "infra" / "orchestration" / "airflow" / "Dockerfile").is_file()
    assert (repo_root / "infra" / "orchestration" / "airflow" / "dags").is_dir()
    dockerfile = (repo_root / "infra" / "orchestration" / "airflow" / "Dockerfile").read_text(encoding="utf-8")
    assert "/usr/local/bin/mc" in dockerfile
    assert '"numpy==1.26.4"' in dockerfile
    assert '"scipy==1.14.1"' in dockerfile


def test_airflow_init_mounts_and_runs_coursework_metadata_seed() -> None:
    compose = load_compose_model(_repo_root())
    init = compose["services"]["airflow-init"]

    assert "./infra/orchestration/airflow/bootstrap:/opt/airflow/bootstrap:ro" in init["volumes"]
    assert "seed_coursework_metadata.py" in str(init["command"])
    assert init["command"][0] == "bash"
    assert "AIRFLOW_CONN_DATAHUB_REST_DEFAULT" in init["environment"]
    assert "VBS_KAFKA_BOOTSTRAP_SERVERS" in init["environment"]
    assert "VBS_MINIO_INTERNAL_ENDPOINT" in init["environment"]
