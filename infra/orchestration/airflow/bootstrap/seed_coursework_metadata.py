"""Seed non-secret Airflow metadata for the six-stage coursework DAG."""
from __future__ import annotations

import json
import os
from typing import Any, Callable
from urllib.parse import urlparse


FEATURE_TABLES = ["feat_customer_90d", "feat_stream_60m", "feat_customer_unified"]


def _endpoint(value: str) -> tuple[str, int | None]:
    parsed = urlparse(value if "://" in value else f"//{value}")
    if not parsed.hostname:
        raise ValueError(f"Expected endpoint host in {value!r}.")
    return parsed.hostname, parsed.port


def _connection_specs(environ: dict[str, str]) -> list[dict[str, Any]]:
    minio_host, minio_port = _endpoint(environ["VBS_MINIO_INTERNAL_ENDPOINT"])
    trino_host, trino_port = _endpoint(environ["VBS_TRINO_URL"])
    kafka_host, kafka_port = _endpoint(environ["VBS_KAFKA_BOOTSTRAP_SERVERS"])
    return [
        {
            "conn_id": "vbs_minio",
            "conn_type": "http",
            "host": minio_host,
            "port": minio_port,
            "login": environ["VBS_MINIO_ROOT_USER"],
            "password": environ["VBS_MINIO_ROOT_PASSWORD"],
            "extra": {"region": environ["VBS_MINIO_REGION"]},
        },
        {
            "conn_id": "vbs_trino",
            "conn_type": "trino",
            "host": trino_host,
            "port": trino_port,
            "login": environ["VBS_TRINO_USER"],
            "password": None,
            "extra": {"catalog": "iceberg"},
        },
        {
            "conn_id": "vbs_kafka",
            "conn_type": "kafka",
            "host": kafka_host,
            "port": kafka_port,
            "login": None,
            "password": None,
            "extra": {},
        },
        {
            "conn_id": "datahub_rest_default",
            "conn_type": "datahub_rest",
            "host": environ["VBS_DATAHUB_GMS_URL"],
            "port": None,
            "login": None,
            "password": None,
            "extra": {},
        },
    ]


def _variable_specs(environ: dict[str, str]) -> dict[str, Any]:
    return {
        "vbs_raw_root": environ["VBS_RAW_ROOT"],
        "vbs_bronze_bucket": environ["VBS_BRONZE_BUCKET"],
        "vbs_spark_evidence_root": environ["VBS_SPARK_EVIDENCE_ROOT"],
        "vbs_feature_tables": FEATURE_TABLES,
    }


def _upsert_connection(connection_model: Any, payload: dict[str, Any]) -> None:
    if hasattr(connection_model, "upsert"):
        connection_model.upsert(**payload)
        return

    from airflow import settings

    session = settings.Session()
    try:
        existing = session.query(connection_model).filter_by(conn_id=payload["conn_id"]).one_or_none()
        values = {**payload, "extra": json.dumps(payload["extra"], sort_keys=True)}
        if existing is None:
            session.add(connection_model(**values))
        else:
            for key, value in values.items():
                setattr(existing, key, value)
        session.commit()
    finally:
        session.close()


def seed_coursework_metadata(
    *,
    connection_model: Any,
    variable_model: Any,
    environ: dict[str, str],
    log: Callable[[str], None] = print,
) -> None:
    for connection in _connection_specs(environ):
        _upsert_connection(connection_model, connection)
        log(f"Seeded connection {connection['conn_id']} at {connection['host']}:{connection['port']}.")
    for key, value in _variable_specs(environ).items():
        variable_model.set(key, value, serialize_json=isinstance(value, list))
        log(f"Seeded variable {key}.")


def main() -> None:
    from airflow.models.connection import Connection
    from airflow.models.variable import Variable

    required = {
        "VBS_MINIO_INTERNAL_ENDPOINT": "http://minio:9000",
        "VBS_MINIO_ROOT_USER": "vina_minio",
        "VBS_MINIO_ROOT_PASSWORD": "vina_minio_password",
        "VBS_MINIO_REGION": "us-east-1",
        "VBS_TRINO_URL": "http://trino:8080",
        "VBS_TRINO_USER": "vina_analyst",
        "VBS_KAFKA_BOOTSTRAP_SERVERS": "kafka:29092",
        "VBS_DATAHUB_GMS_URL": "http://datahub-gms:8080",
        "VBS_RAW_ROOT": "/workspace/data/raw",
        "VBS_BRONZE_BUCKET": "bronze",
        "VBS_SPARK_EVIDENCE_ROOT": "/workspace/evidence/08_airflow_gx/coursework_pipeline",
    }
    seed_coursework_metadata(
        connection_model=Connection,
        variable_model=Variable,
        environ={key: os.getenv(key, default) for key, default in required.items()},
    )


if __name__ == "__main__":
    main()
