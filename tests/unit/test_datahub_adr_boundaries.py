import json
from pathlib import Path

import pytest
import yaml

from compose_model import load_compose_model


def test_datahub_db_already_configured_in_shared_postgres() -> None:
    repo_root = Path(__file__).resolve().parents[2]
    init_sql = (repo_root / "infra" / "lakehouse" / "postgres" / "init" / "01-create-platform-databases.sql").read_text(encoding="utf-8")

    assert "CREATE USER datahub" in init_sql
    assert "CREATE DATABASE datahub OWNER datahub" in init_sql


def test_governance_compose_profile_has_required_services() -> None:
    repo_root = Path(__file__).resolve().parents[2]
    compose = load_compose_model(repo_root)

    governance_services = {
        name
        for name, svc in compose.get("services", {}).items()
        if "governance" in svc.get("profiles", [])
    }

    assert "datahub-gms" in governance_services
    assert "datahub-frontend" in governance_services
    assert "datahub-elasticsearch" in governance_services
    assert "datahub-actions" in governance_services
    assert "datahub-system-update" in governance_services


def test_governance_profile_reuses_shared_kafka_and_postgres() -> None:
    repo_root = Path(__file__).resolve().parents[2]
    compose = load_compose_model(repo_root)

    gms = compose["services"]["datahub-gms"]
    env_vars = {k: v for k, v in gms["environment"].items()}

    assert "lakehouse-postgres:5432" in env_vars["EBEAN_DATASOURCE_URL"]
    assert env_vars["EBEAN_DATASOURCE_USERNAME"] == "datahub"
    assert env_vars["KAFKA_BOOTSTRAP_SERVER"] == "kafka:29092"
    assert env_vars["ELASTICSEARCH_HOST"] == "datahub-elasticsearch"
    assert env_vars["KAFKA_SCHEMAREGISTRY_URL"] == "http://schema-registry:8081"
    assert env_vars["METADATA_CHANGE_LOG_KAFKA_CONSUMER_GROUP_ID"] == "datahub-1-6-recovery-indexer-v2"
    assert env_vars["KAFKA_CONSUMER_MCL_AUTO_OFFSET_RESET"] == "latest"
    assert env_vars["ES_BULK_REFRESH_POLICY"] == "NONE"

    system_update = compose["services"]["datahub-system-update"]
    assert system_update["environment"]["KAFKA_SCHEMAREGISTRY_URL"] == "http://schema-registry:8081"
    assert system_update["depends_on"]["schema-registry"]["condition"] == "service_healthy"
    assert compose["services"]["datahub-actions"]["environment"]["SCHEMA_REGISTRY_URL"] == "http://schema-registry:8081"


def test_governance_runtime_uses_aligned_elasticsearch_versions_and_persistence() -> None:
    repo_root = Path(__file__).resolve().parents[2]
    compose = load_compose_model(repo_root)

    elasticsearch = compose["services"]["datahub-elasticsearch"]
    assert elasticsearch["image"] == "elasticsearch:7.10.1"
    assert "datahub_search_data:/usr/share/elasticsearch/data" in elasticsearch["volumes"]

    for service_name, image in {
        "datahub-system-update": "acryldata/datahub-upgrade:v1.6.0",
        "datahub-gms": "acryldata/datahub-gms:v1.6.0",
        "datahub-frontend": "acryldata/datahub-frontend-react:v1.6.0",
        "datahub-actions": "acryldata/datahub-actions:v1.6.0-slim",
    }.items():
        assert compose["services"][service_name]["image"] == image

    for service_name in ("datahub-system-update", "datahub-gms"):
        environment = compose["services"][service_name]["environment"]
        assert environment["ELASTICSEARCH_HOST"] == "datahub-elasticsearch"
        assert environment["ELASTICSEARCH_IMPLEMENTATION"] == "elasticsearch"

    frontend = compose["services"]["datahub-frontend"]
    assert frontend["environment"]["ELASTIC_CLIENT_HOST"] == "datahub-elasticsearch"
    assert frontend["environment"]["DATAHUB_APP_VERSION"] == "v1.6.0"
    assert len(frontend["environment"]["DATAHUB_SECRET"]) >= 32


def test_datahub_does_not_change_canonical_truth_policy() -> None:
    repo_root = Path(__file__).resolve().parents[2]
    schema_design = (repo_root / "deliverables" / "02_schema_design.md").read_text(encoding="utf-8")
    governance = (repo_root / "deliverables" / "09_datahub_governance.md").read_text(encoding="utf-8")

    assert "Trino-served Gold tables are the canonical reconciled truth" in schema_design
    assert "Pinot is not used as the official financial source" in schema_design
    assert "DataHub is the governance and metadata catalog layer" in governance
    assert "can emit metadata about the same assets that the pipelines produce" in governance
    assert "Tags such as `bronze`, `silver`, `gold`, `official`, `provisional`, and `quality_gate` classify assets" in governance


def test_datahub_ingestion_evidence_artifacts_exist() -> None:
    repo_root = Path(__file__).resolve().parents[2]
    evidence_root = repo_root / "evidence" / "09_datahub_governance"

    assert (evidence_root / "run_manifest.json").is_file()
    assert (evidence_root / "datahub_health.json").is_file()
    assert (evidence_root / "dataset_count.json").is_file()
    assert (evidence_root / "tag_count.json").is_file()


def test_governance_vocabulary_covers_medallion_layers() -> None:
    repo_root = Path(__file__).resolve().parents[2]
    runtime = (repo_root / "src" / "vina_bim_shop" / "orchestration" / "datahub_ingestion.py").read_text(encoding="utf-8")

    for tag in ["bronze", "silver", "gold", "official", "provisional", "pii_safe", "regression_oracle", "quality_gate"]:
        assert f'"{tag}"' in runtime


def test_datahub_ingestion_recipes_exist() -> None:
    repo_root = Path(__file__).resolve().parents[2]
    recipes_dir = repo_root / "infra" / "governance" / "recipes"

    assert (recipes_dir / "kafka_topics.yml").is_file()
    assert (recipes_dir / "minio_storage.yml").is_file()
    assert (recipes_dir / "trino_tables.yml").is_file()
    assert (recipes_dir / "dbt_legacy.yml").is_file()


def test_datahub_recipes_match_current_cli_contract() -> None:
    repo_root = Path(__file__).resolve().parents[2]
    recipes_dir = repo_root / "infra" / "governance" / "recipes"

    kafka_recipe = yaml.safe_load((recipes_dir / "kafka_topics.yml").read_text(encoding="utf-8"))
    kafka_config = kafka_recipe["source"]["config"]
    assert kafka_config["connection"]["schema_registry_url"] == "http://schema-registry:8081"
    assert "schema_registry_url" not in kafka_config
    assert "stateful_ingestion" not in kafka_config

    trino_recipe = yaml.safe_load((recipes_dir / "trino_tables.yml").read_text(encoding="utf-8"))
    assert "stateful_ingestion" not in trino_recipe["source"]["config"]

    dbt_recipe = yaml.safe_load((recipes_dir / "dbt_legacy.yml").read_text(encoding="utf-8"))
    dbt_config = dbt_recipe["source"]["config"]
    assert dbt_config["manifest_path"] == "/workspace/infra/analytics/dbt/target/manifest.json"
    assert dbt_config["run_results_paths"] == ["/workspace/infra/analytics/dbt/target/run_results.json"]
    assert "catalog_path" not in dbt_config
    assert "load_schemas" not in dbt_config
    assert "stateful_ingestion" not in dbt_config


def test_minio_container_metadata_seed_exists() -> None:
    repo_root = Path(__file__).resolve().parents[2]
    metadata_path = repo_root / "infra" / "governance" / "recipes" / "minio_container_metadata.json"

    payload = json.loads(metadata_path.read_text(encoding="utf-8"))
    assert isinstance(payload, list)
    assert len(payload) >= 4
    assert any(item["entityUrn"].startswith("urn:li:dataset:(urn:li:dataPlatform:s3,bronze") for item in payload)


def test_datahub_lineage_package_exports_correctly():
    import importlib

    try:
        importlib.import_module("datahub")
    except ImportError:
        pytest.skip("acryl-datahub not installed in local venv (Docker-only dependency)")

    from vina_bim_shop.datahub_lineage import DataHubLineageEmitter, emit_spark_batch_lineage, emit_flink_streaming_lineage

    assert DataHubLineageEmitter is not None
    assert emit_spark_batch_lineage is not None
    assert emit_flink_streaming_lineage is not None


def test_datahub_airflow_plugin_config_declares_correct_cluster() -> None:
    repo_root = Path(__file__).resolve().parents[2]
    plugin_file = repo_root / "infra" / "orchestration" / "airflow" / "plugins" / "datahub_plugin.py"

    assert plugin_file.is_file()
    content = plugin_file.read_text(encoding="utf-8")
    assert "vina-bim-shop-local" in content


def test_custom_datahub_lineage_uses_explicit_upstream_type() -> None:
    repo_root = Path(__file__).resolve().parents[2]
    emitter_file = repo_root / "src" / "vina_bim_shop" / "datahub_lineage" / "emitter.py"

    contents = emitter_file.read_text(encoding="utf-8")
    assert "DatasetLineageTypeClass" in contents
    assert "type=DatasetLineageTypeClass.TRANSFORMED" in contents


def test_datahub_gms_port_does_not_conflict_with_trino() -> None:
    repo_root = Path(__file__).resolve().parents[2]
    compose = load_compose_model(repo_root)

    host_ports = {}
    for name, svc in compose.get("services", {}).items():
        if "ports" not in svc:
            continue
        for port_mapping in svc["ports"]:
            host_port = port_mapping.split(":")[0]
            if host_port in host_ports:
                pass
            host_ports.setdefault(host_port, []).append(name)

    trino_host = host_ports.get("8080", [])
    gms_host = host_ports.get("8087", [])
    assert "trino" in trino_host or any("trino" in s for s in trino_host)
    assert any("datahub-gms" in s for s in gms_host)
    assert "8087" not in [p.split(":")[0] for svc in compose["services"].values() for p in svc.get("ports", []) if "trino" in svc.get("image", "")]
