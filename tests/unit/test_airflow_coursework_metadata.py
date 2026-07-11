from __future__ import annotations

import importlib.util
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]
SEED_PATH = REPO_ROOT / "infra" / "orchestration" / "airflow" / "bootstrap" / "seed_coursework_metadata.py"


class FakeConnection:
    records: dict[str, dict[str, object]] = {}

    @classmethod
    def upsert(cls, **payload: object) -> None:
        cls.records[str(payload["conn_id"])] = payload


class FakeVariable:
    records: dict[str, object] = {}

    @classmethod
    def set(cls, key: str, value: object, **_kwargs: object) -> None:
        cls.records[key] = value


def _load_seed_module():
    assert SEED_PATH.is_file(), "Expected the idempotent Airflow coursework metadata seed script."
    spec = importlib.util.spec_from_file_location("coursework_metadata_seed", SEED_PATH)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_seed_upserts_the_required_connections_and_variables_without_secret_output() -> None:
    module = _load_seed_module()
    FakeConnection.records = {}
    FakeVariable.records = {}
    messages: list[str] = []

    module.seed_coursework_metadata(
        connection_model=FakeConnection,
        variable_model=FakeVariable,
        environ={
            "VBS_MINIO_INTERNAL_ENDPOINT": "http://minio:9000",
            "VBS_MINIO_ROOT_USER": "vina_minio",
            "VBS_MINIO_ROOT_PASSWORD": "secret-value",
            "VBS_MINIO_REGION": "us-east-1",
            "VBS_TRINO_URL": "http://trino:8080",
            "VBS_TRINO_USER": "vina_analyst",
            "VBS_KAFKA_BOOTSTRAP_SERVERS": "kafka:29092",
            "VBS_DATAHUB_GMS_URL": "http://datahub-gms:8080",
            "VBS_RAW_ROOT": "/workspace/data/raw",
            "VBS_BRONZE_BUCKET": "bronze",
            "VBS_SPARK_EVIDENCE_ROOT": "/workspace/evidence/08_airflow_gx/coursework_pipeline",
        },
        log=messages.append,
    )

    assert set(FakeConnection.records) == {
        "vbs_minio",
        "vbs_trino",
        "vbs_kafka",
        "datahub_rest_default",
    }
    assert FakeConnection.records["vbs_minio"]["host"] == "minio"
    assert FakeConnection.records["vbs_minio"]["port"] == 9000
    assert FakeConnection.records["vbs_trino"]["host"] == "trino"
    assert FakeConnection.records["vbs_kafka"]["host"] == "kafka"
    assert FakeConnection.records["datahub_rest_default"]["host"] == "http://datahub-gms:8080"
    assert set(FakeVariable.records) == {
        "vbs_raw_root",
        "vbs_bronze_bucket",
        "vbs_spark_evidence_root",
        "vbs_feature_tables",
    }
    assert FakeVariable.records["vbs_feature_tables"] == [
        "feat_customer_90d",
        "feat_stream_60m",
        "feat_customer_unified",
    ]
    assert "secret-value" not in "\n".join(messages)
