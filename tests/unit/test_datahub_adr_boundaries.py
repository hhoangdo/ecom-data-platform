import json
from pathlib import Path

import pytest
import yaml


def test_datahub_db_already_configured_in_shared_postgres() -> None:
    repo_root = Path(__file__).resolve().parents[2]
    init_sql = (repo_root / "infra" / "lakehouse" / "postgres" / "init" / "01-create-platform-databases.sql").read_text(encoding="utf-8")

    assert "CREATE USER datahub" in init_sql
    assert "CREATE DATABASE datahub OWNER datahub" in init_sql


def test_governance_compose_profile_has_required_services() -> None:
    repo_root = Path(__file__).resolve().parents[2]
    compose = yaml.safe_load((repo_root / "docker-compose.yml").read_text(encoding="utf-8"))

    governance_services = {
        name
        for name, svc in compose.get("services", {}).items()
        if "governance" in svc.get("profiles", [])
    }

    assert "datahub-gms" in governance_services
    assert "datahub-frontend" in governance_services
    assert "datahub-opensearch" in governance_services
    assert "datahub-actions" in governance_services
    assert "datahub-system-update" in governance_services


def test_governance_profile_reuses_shared_kafka_and_postgres() -> None:
    repo_root = Path(__file__).resolve().parents[2]
    compose = yaml.safe_load((repo_root / "docker-compose.yml").read_text(encoding="utf-8"))

    gms = compose["services"]["datahub-gms"]
    env_vars = {k: v for k, v in gms["environment"].items()}

    assert "lakehouse-postgres:5432" in env_vars["EBEAN_DATASOURCE_URL"]
    assert env_vars["EBEAN_DATASOURCE_USERNAME"] == "datahub"
    assert env_vars["KAFKA_BOOTSTRAP_SERVER"] == "kafka:29092"
    assert "datahub-opensearch" in env_vars["ELASTICSEARCH_HOST"]


def test_datahub_does_not_change_canonical_truth_policy() -> None:
    repo_root = Path(__file__).resolve().parents[2]
    adr07 = (repo_root / "architecture" / "decisions" / "2026-06-01-07-datahub-governance-plan.md").read_text(encoding="utf-8")

    assert "Do not let DataHub change the canonical truth policy" in adr07


def test_datahub_ingestion_evidence_scaffold_exists() -> None:
    repo_root = Path(__file__).resolve().parents[2]
    assert (repo_root / "evidence" / "09_datahub_governance" / ".gitkeep").is_file()
    assert (repo_root / "evidence" / "09_datahub_governance" / "screenshots" / ".gitkeep").is_file()


def test_governance_vocabulary_covers_medallion_layers() -> None:
    repo_root = Path(__file__).resolve().parents[2]
    runtime = (repo_root / "src" / "vina_bim_shop" / "orchestration" / "runtime.py").read_text(encoding="utf-8")

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
    assert dbt_config["manifest_path"] == "/workspace/dbt/target/manifest.json"
    assert dbt_config["run_results_paths"] == ["/workspace/dbt/target/run_results.json"]
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
    compose = yaml.safe_load((repo_root / "docker-compose.yml").read_text(encoding="utf-8"))

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
