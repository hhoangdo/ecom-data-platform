from pathlib import Path


REQUIRED_BUCKETS = {"bronze", "silver", "gold", "checkpoints", "evidence"}


def test_env_example_documents_fixed_lakehouse_credentials_and_urls() -> None:
    repo_root = Path(__file__).resolve().parents[2]
    env_example = (repo_root / ".env.example").read_text(encoding="utf-8")

    expected_lines = [
        "VBS_MINIO_ENDPOINT=http://localhost:9000",
        "VBS_MINIO_INTERNAL_ENDPOINT=http://minio:9000",
        "VBS_MINIO_CONSOLE_URL=http://localhost:9001",
        "VBS_MINIO_ROOT_USER=vina_minio",
        "VBS_MINIO_ROOT_PASSWORD=vina_minio_password",
        "VBS_LAKEHOUSE_POSTGRES_PORT=5433",
        "VBS_HIVE_METASTORE_DB=hive_metastore",
        "VBS_HIVE_METASTORE_URI=thrift://localhost:9083",
        "VBS_TRINO_URL=http://localhost:8080",
        "VBS_TRINO_ICEBERG_CATALOG=iceberg",
    ]
    for expected_line in expected_lines:
        assert expected_line in env_example


def test_minio_bucket_init_creates_exact_required_buckets() -> None:
    repo_root = Path(__file__).resolve().parents[2]
    script = (repo_root / "infra" / "lakehouse" / "minio" / "create-buckets.sh").read_text(encoding="utf-8")

    for bucket in REQUIRED_BUCKETS:
        assert f"ALIAS/{bucket}" in script

    assert "ALIAS/checkpoints/spark-events/.keep" in script
    assert "raw" not in script


def test_shared_postgres_init_creates_platform_databases_and_users() -> None:
    repo_root = Path(__file__).resolve().parents[2]
    init_sql = (
        repo_root / "infra" / "lakehouse" / "postgres" / "init" / "01-create-platform-databases.sql"
    ).read_text(encoding="utf-8")

    for database in ["hive_metastore", "airflow", "datahub"]:
        assert f"CREATE DATABASE {database}" in init_sql
        assert f"CREATE USER {database}" in init_sql


def test_trino_iceberg_catalog_uses_hive_metastore_and_minio_only() -> None:
    repo_root = Path(__file__).resolve().parents[2]
    catalog = (
        repo_root / "infra" / "lakehouse" / "trino" / "catalog" / "iceberg.properties"
    ).read_text(encoding="utf-8")

    assert "connector.name=iceberg" in catalog
    assert "iceberg.catalog.type=hive_metastore" in catalog
    assert "hive.metastore.uri=thrift://hive-metastore:9083" in catalog
    assert "s3.endpoint=http://minio:9000" in catalog
    assert "bronze" not in catalog.lower()


def test_hive_site_config_supports_s3a_minio_for_metastore_locations() -> None:
    repo_root = Path(__file__).resolve().parents[2]
    hive_site = (repo_root / "infra" / "lakehouse" / "hive" / "hive-site.xml").read_text(encoding="utf-8")

    for expected_fragment in [
        "<name>hive.metastore.warehouse.dir</name>",
        "<value>s3a://silver/warehouse</value>",
        "<name>fs.s3a.endpoint</name>",
        "<value>http://minio:9000</value>",
        "<name>fs.s3a.impl</name>",
        "<value>org.apache.hadoop.fs.s3a.S3AFileSystem</value>",
        "<name>fs.s3a.aws.credentials.provider</name>",
        "<value>org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider</value>",
    ]:
        assert expected_fragment in hive_site


def test_trino_iceberg_catalog_avoids_unsupported_s3_ssl_property() -> None:
    repo_root = Path(__file__).resolve().parents[2]
    catalog = (
        repo_root / "infra" / "lakehouse" / "trino" / "catalog" / "iceberg.properties"
    ).read_text(encoding="utf-8")

    assert "s3.ssl.enabled" not in catalog


def test_trino_smoke_sql_is_read_only_metadata_check() -> None:
    repo_root = Path(__file__).resolve().parents[2]
    smoke_sql = (repo_root / "infra" / "lakehouse" / "trino" / "sql" / "smoke.sql").read_text(encoding="utf-8")

    assert "SHOW CATALOGS" in smoke_sql
    assert "SHOW SCHEMAS FROM iceberg" in smoke_sql
    assert "iceberg.information_schema.schemata" in smoke_sql
    assert "CREATE TABLE" not in smoke_sql
    assert "INSERT INTO" not in smoke_sql
    assert "DROP TABLE" not in smoke_sql


def test_trino_node_environment_is_valid_identifier() -> None:
    repo_root = Path(__file__).resolve().parents[2]
    node_properties = (
        repo_root / "infra" / "lakehouse" / "trino" / "etc" / "node.properties"
    ).read_text(encoding="utf-8")

    assert "node.environment=vina_bim_shop_local" in node_properties
    assert "node.environment=vina-bim-shop-local" not in node_properties


def test_trino_self_discovery_uses_container_hostname() -> None:
    repo_root = Path(__file__).resolve().parents[2]
    config_properties = (
        repo_root / "infra" / "lakehouse" / "trino" / "etc" / "config.properties"
    ).read_text(encoding="utf-8")

    assert "coordinator=true" in config_properties
    assert "node-scheduler.include-coordinator=true" in config_properties
    assert "discovery.uri=http://trino:8080" in config_properties
    assert "discovery.uri=http://localhost:8080" not in config_properties


def test_trino_worker_config_uses_non_coordinator_role() -> None:
    repo_root = Path(__file__).resolve().parents[2]
    worker_config = (
        repo_root / "infra" / "lakehouse" / "trino" / "etc" / "worker-config.properties"
    ).read_text(encoding="utf-8")
    worker_node = (
        repo_root / "infra" / "lakehouse" / "trino" / "etc" / "worker-node.properties"
    ).read_text(encoding="utf-8")

    assert "coordinator=false" in worker_config
    assert "discovery.uri=http://trino:8080" in worker_config
    assert "node.environment=vina_bim_shop_local" in worker_node
    assert "node.id=vina-bim-shop-trino-worker-1" in worker_node


def test_lakehouse_evidence_defaults_to_adr02_evidence_path() -> None:
    from vina_bim_shop.lakehouse.evidence import DEFAULT_EVIDENCE_ROOT, REQUIRED_BUCKETS

    assert DEFAULT_EVIDENCE_ROOT == Path("evidence/04_lakehouse")
    assert REQUIRED_BUCKETS == ("bronze", "silver", "gold", "checkpoints", "evidence")


def test_env_example_documents_bronze_raw_landing_defaults() -> None:
    repo_root = Path(__file__).resolve().parents[2]
    env_example = (repo_root / ".env.example").read_text(encoding="utf-8")

    expected_lines = [
        "VBS_RAW_ROOT=data/raw",
        "VBS_BRONZE_BATCH_PREFIX=bronze/batch",
        "VBS_BRONZE_EVENTS_PREFIX=bronze/events",
    ]
    for expected_line in expected_lines:
        assert expected_line in env_example
