"""Pure runtime functions for the rubric-facing DP1, DP2, and DP3 DAG.

The Airflow adapter resolves connection metadata and forwards it here.  Keeping
the work in this module makes the six stages directly testable without an
Airflow installation.
"""
from __future__ import annotations

import json
import shlex
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable

import pandas as pd

from vina_bim_shop.lakehouse.bronze import render_batch_snapshot_key, render_event_key
from vina_bim_shop.lakehouse.spark.constants import (
    BRONZE_BATCH_DATASETS,
    BRONZE_EVENT_TOPICS,
    REQUIRED_GOLD_TABLES,
    SILVER_TABLES,
)
from vina_bim_shop.lakehouse.spark.runner import run_core_transform, run_feature_compute
from vina_bim_shop.lakehouse.spark.sql import ordered_core_gold_queries, ordered_feature_queries
from vina_bim_shop.lakehouse.spark.trino import execute_trino_query
from vina_bim_shop.lakehouse.spark.window import BatchWindow
from vina_bim_shop.quality.reports import ValidationReport

from .paths import DOCS_ROOT, REPO_ROOT, _slugify_run_id, _utc_now, _write_json
from .quality_helpers import _render_docs, _validate_pandas_dataframe, _window_payload
from .subprocess_helpers import _get_json, _run_command, _working_directory


FEATURE_TABLES = (
    "feat_customer_90d",
    "feat_stream_60m",
    "feat_customer_unified",
)
CORE_GOLD_TABLES = tuple(table_name for table_name, _query in ordered_core_gold_queries())
STAGE_FUNCTION_NAMES = (
    "ingest_raw_to_bronze",
    "validate_bronze",
    "transform_bronze_to_silver_gold",
    "validate_silver_gold",
    "compute_offline_features",
    "validate_offline_features",
)
STAGE_DETAILS = {
    "ingest_raw_to_bronze": ("dp1_ingest", "dp1_raw_to_bronze.ingest_raw_to_bronze"),
    "validate_bronze": ("dp1_validate", "dp1_raw_to_bronze.validate_bronze"),
    "transform_bronze_to_silver_gold": (
        "dp2_transform",
        "dp2_bronze_to_silver_gold.transform_bronze_to_silver_gold",
    ),
    "validate_silver_gold": (
        "dp2_validate",
        "dp2_bronze_to_silver_gold.validate_silver_gold",
    ),
    "compute_offline_features": ("dp3_compute", "dp3_offline_features.compute_offline_features"),
    "validate_offline_features": (
        "dp3_validate",
        "dp3_offline_features.validate_offline_features",
    ),
}


@dataclass(frozen=True)
class CourseworkPipelineSettings:
    minio_endpoint: str
    minio_access_key: str
    minio_secret_key: str
    minio_region: str
    trino_url: str
    trino_user: str
    kafka_bootstrap_servers: str
    datahub_url: str
    raw_root: str
    bronze_bucket: str
    spark_evidence_root: str
    feature_tables: tuple[str, ...] = FEATURE_TABLES

    @classmethod
    def for_tests(cls) -> "CourseworkPipelineSettings":
        return cls(
            minio_endpoint="http://minio:9000",
            minio_access_key="vina_minio",
            minio_secret_key="vina_minio_password",
            minio_region="us-east-1",
            trino_url="http://trino:8080",
            trino_user="vina_analyst",
            kafka_bootstrap_servers="kafka:29092",
            datahub_url="http://datahub-gms:8080",
            raw_root="/workspace/data/raw",
            bronze_bucket="bronze",
            spark_evidence_root="/workspace/evidence/08_airflow_gx/coursework_pipeline",
        )


def _url_from_connection(connection: Any, *, scheme: str) -> str:
    host = getattr(connection, "host", "")
    port = getattr(connection, "port", None)
    if not host:
        raise ValueError("Airflow connection is missing its host.")
    if host.startswith(("http://", "https://")):
        return host.rstrip("/")
    return f"{scheme}://{host}{f':{port}' if port else ''}"


def settings_from_airflow(
    *,
    get_connection: Callable[[str], Any],
    get_variable: Callable[..., Any],
) -> CourseworkPipelineSettings:
    minio = get_connection("vbs_minio")
    trino = get_connection("vbs_trino")
    kafka = get_connection("vbs_kafka")
    datahub = get_connection("datahub_rest_default")
    features = get_variable("vbs_feature_tables", deserialize_json=True)
    return CourseworkPipelineSettings(
        minio_endpoint=_url_from_connection(minio, scheme="http"),
        minio_access_key=getattr(minio, "login", ""),
        minio_secret_key=getattr(minio, "password", ""),
        minio_region=(getattr(minio, "extra_dejson", {}) or {}).get("region", "us-east-1"),
        trino_url=_url_from_connection(trino, scheme="http"),
        trino_user=getattr(trino, "login", "vina_analyst"),
        kafka_bootstrap_servers=f"{getattr(kafka, 'host', '')}:{getattr(kafka, 'port', 29092)}",
        datahub_url=_url_from_connection(datahub, scheme="http"),
        raw_root=get_variable("vbs_raw_root"),
        bronze_bucket=get_variable("vbs_bronze_bucket"),
        spark_evidence_root=get_variable("vbs_spark_evidence_root"),
        feature_tables=tuple(features),
    )


def settings_from_environment() -> CourseworkPipelineSettings:
    import os

    defaults = CourseworkPipelineSettings.for_tests()
    return CourseworkPipelineSettings(
        minio_endpoint=os.getenv("VBS_MINIO_INTERNAL_ENDPOINT", defaults.minio_endpoint),
        minio_access_key=os.getenv("VBS_MINIO_ROOT_USER", defaults.minio_access_key),
        minio_secret_key=os.getenv("VBS_MINIO_ROOT_PASSWORD", defaults.minio_secret_key),
        minio_region=os.getenv("VBS_MINIO_REGION", defaults.minio_region),
        trino_url=os.getenv("VBS_TRINO_URL", defaults.trino_url),
        trino_user=os.getenv("VBS_TRINO_USER", defaults.trino_user),
        kafka_bootstrap_servers=os.getenv("VBS_KAFKA_BOOTSTRAP_SERVERS", defaults.kafka_bootstrap_servers),
        datahub_url=os.getenv("VBS_DATAHUB_GMS_URL", defaults.datahub_url),
        raw_root=os.getenv("VBS_RAW_ROOT", defaults.raw_root),
        bronze_bucket=os.getenv("VBS_BRONZE_BUCKET", defaults.bronze_bucket),
        spark_evidence_root=os.getenv("VBS_SPARK_EVIDENCE_ROOT", defaults.spark_evidence_root),
        feature_tables=FEATURE_TABLES,
    )


def build_coursework_run_root(*, run_id: str, settings: CourseworkPipelineSettings) -> Path:
    path = Path(settings.spark_evidence_root) / _slugify_run_id(run_id)
    path.mkdir(parents=True, exist_ok=True)
    return path


def _window(start_ts: str, end_ts: str) -> BatchWindow:
    return BatchWindow.from_args(start_ts=start_ts, end_ts=end_ts, mode="hourly")


def _stage_payload(
    *,
    function_name: str,
    run_id: str,
    window: BatchWindow,
    payload: dict[str, Any],
) -> dict[str, Any]:
    artifact_name, task_id = STAGE_DETAILS[function_name]
    return {
        "schema_version": 1,
        "dag_id": "mini_coursework_pipeline",
        "run_id": run_id,
        "task_id": task_id,
        "stage": artifact_name,
        "window": _window_payload(window),
        "artifact": f"{artifact_name}.json",
        **payload,
    }


def _write_stage(
    *,
    function_name: str,
    run_id: str,
    window: BatchWindow,
    run_root: Path,
    payload: dict[str, Any],
) -> dict[str, Any]:
    result = _stage_payload(function_name=function_name, run_id=run_id, window=window, payload=payload)
    _write_json(run_root / result["artifact"], result)

    existing_path = run_root / "run_manifest.json"
    existing = json.loads(existing_path.read_text(encoding="utf-8")) if existing_path.is_file() else {}
    stages = {entry["task_id"]: entry for entry in existing.get("stages", [])}
    stages[result["task_id"]] = {
        "task_id": result["task_id"],
        "state": result.get("state", "success"),
        "artifact": result["artifact"],
    }
    manifest = {
        "schema_version": 1,
        "dag_id": "mini_coursework_pipeline",
        "run_id": run_id,
        "window": _window_payload(window),
        "stages": [
            stages[STAGE_DETAILS[name][1]]
            for name in STAGE_FUNCTION_NAMES
            if STAGE_DETAILS[name][1] in stages
        ],
        "updated_at": _utc_now(),
    }
    _write_json(existing_path, manifest)
    return result


def _raw_files(settings: CourseworkPipelineSettings) -> list[tuple[Path, str]]:
    raw_root = Path(settings.raw_root)
    files: list[tuple[Path, str]] = []
    for dataset in BRONZE_BATCH_DATASETS:
        files.extend((path, dataset) for path in sorted((raw_root / dataset).glob("*.parquet")))
    return files


def _event_files(settings: CourseworkPipelineSettings) -> list[tuple[Path, str]]:
    raw_root = Path(settings.raw_root) / "kafka_topics"
    return [
        (raw_root / topic / "events.jsonl", topic)
        for topic in (*BRONZE_EVENT_TOPICS, "dead_letter_events")
        if (raw_root / topic / "events.jsonl").is_file()
    ]


def _copy_to_bronze(
    *, source: Path, object_key: str, settings: CourseworkPipelineSettings
) -> None:
    """Upload a deterministic bronze object from the Airflow worker."""
    alias = "coursework"
    _run_command(
        [
            "mc",
            "alias",
            "set",
            alias,
            settings.minio_endpoint,
            settings.minio_access_key,
            settings.minio_secret_key,
        ]
    )
    _run_command(["mc", "cp", str(source), f"{alias}/{object_key}"])


def ingest_raw_to_bronze(
    *, run_id: str, start_ts: str, end_ts: str, settings: CourseworkPipelineSettings, run_root: Path
) -> dict[str, Any]:
    window = _window(start_ts, end_ts)
    date = window.start_ts.date().isoformat()
    batch_objects: list[str] = []
    for source, dataset in _raw_files(settings):
        key = render_batch_snapshot_key(dataset, date, source.name)
        _copy_to_bronze(source=source, object_key=key, settings=settings)
        batch_objects.append(key)
    event_objects: list[str] = []
    for source, topic in _event_files(settings):
        key = render_event_key(topic, date, source.name)
        _copy_to_bronze(source=source, object_key=key, settings=settings)
        event_objects.append(key)
    return _write_stage(
        function_name="ingest_raw_to_bronze",
        run_id=run_id,
        window=window,
        run_root=run_root,
        payload={
            "state": "success",
            "raw_batch_file_count": len(batch_objects),
            "raw_event_file_count": len(event_objects),
            "bronze_batch_objects": batch_objects,
            "bronze_event_objects": event_objects,
        },
    )


def _quality_dir(run_root: Path) -> Path:
    path = run_root / "quality"
    path.mkdir(parents=True, exist_ok=True)
    return path


def _render_run_docs(quality_root: Path) -> None:
    reports = [
        ValidationReport(**json.loads(path.read_text(encoding="utf-8")))
        for path in sorted(quality_root.glob("*.json"))
    ]
    if reports:
        _render_docs(reports)


def validate_bronze(
    *, run_id: str, start_ts: str, end_ts: str, settings: CourseworkPipelineSettings, run_root: Path
) -> dict[str, Any]:
    window = _window(start_ts, end_ts)
    ingest = json.loads((run_root / "dp1_ingest.json").read_text(encoding="utf-8"))
    records = [
        {"path": path}
        for path in [*ingest["bronze_batch_objects"], *ingest["bronze_event_objects"]]
    ]
    report = _validate_pandas_dataframe(
        dataframe=pd.DataFrame(records or [{"path": None}]),
        datasource_name="coursework_bronze",
        asset_name="coursework_bronze_asset",
        suite_name="coursework_bronze_contract",
        layer="bronze_raw",
        expectations=[
            __import__("great_expectations").expectations.ExpectColumnValuesToNotBeNull(column="path"),
            __import__("great_expectations").expectations.ExpectTableRowCountToBeBetween(min_value=1),
        ],
        output_root=_quality_dir(run_root),
        window=_window_payload(window),
    )
    _render_run_docs(_quality_dir(run_root))
    return _write_stage(
        function_name="validate_bronze",
        run_id=run_id,
        window=window,
        run_root=run_root,
        payload={
            "state": "success",
            "gate": report.to_dict(),
            "gx_report": "quality/coursework_bronze_contract.json",
        },
    )


def _prepare_spark_evidence_root(run_root: Path) -> None:
    _run_command(
        [
            "docker",
            "compose",
            "exec",
            "-T",
            "--user",
            "root",
            "spark-master",
            "bash",
            "-lc",
            f"mkdir -p {shlex.quote(run_root.as_posix())} && chmod -R 0777 {shlex.quote(run_root.as_posix())}",
        ],
    )


def _latest_spark_application_id() -> str | None:
    applications = _get_json("http://spark-history-server:18080/api/v1/applications")
    if not isinstance(applications, list) or not applications:
        return None
    return str(applications[0].get("id")) if applications[0].get("id") else None


def transform_bronze_to_silver_gold(
    *, run_id: str, start_ts: str, end_ts: str, settings: CourseworkPipelineSettings, run_root: Path
) -> dict[str, Any]:
    window = _window(start_ts, end_ts)
    spark_root = run_root / "spark_core"
    _prepare_spark_evidence_root(spark_root)
    with _working_directory(REPO_ROOT):
        summary = run_core_transform(
            start_ts=start_ts,
            end_ts=end_ts,
            mode="hourly",
            evidence_root=spark_root,
        )
    return _write_stage(
        function_name="transform_bronze_to_silver_gold",
        run_id=run_id,
        window=window,
        run_root=run_root,
        payload={
            "state": "success",
            "spark_application_id": _latest_spark_application_id(),
            "core_gold_tables": list(CORE_GOLD_TABLES),
            "spark_summary": summary,
        },
    )


def _trino_count(table_name: str, settings: CourseworkPipelineSettings) -> int:
    return int(
        execute_trino_query(
            f"select count(*) from iceberg.gold.{table_name}",
            trino_url=settings.trino_url,
            user=settings.trino_user,
        )["rows"][0][0]
    )


def validate_silver_gold(
    *, run_id: str, start_ts: str, end_ts: str, settings: CourseworkPipelineSettings, run_root: Path
) -> dict[str, Any]:
    window = _window(start_ts, end_ts)
    gold_inventory = execute_trino_query(
        "show tables from iceberg.gold", trino_url=settings.trino_url, user=settings.trino_user
    )["rows"]
    observed = {row[0] for row in gold_inventory}
    gold_counts = {table_name: _trino_count(table_name, settings) for table_name in CORE_GOLD_TABLES}
    silver_inventory = execute_trino_query(
        "show tables from iceberg.silver", trino_url=settings.trino_url, user=settings.trino_user
    )["rows"]
    report = _validate_pandas_dataframe(
        dataframe=pd.DataFrame(
            [
                {
                    "core_gold_table_count": len(set(CORE_GOLD_TABLES) & observed),
                    "fact_order_rows": gold_counts.get("fact_order", 0),
                }
            ]
        ),
        datasource_name="coursework_core_gold",
        asset_name="coursework_core_gold_asset",
        suite_name="coursework_core_gold_contract",
        layer="gold_trino",
        expectations=[
            __import__("great_expectations").expectations.ExpectColumnValuesToBeBetween(
                column="core_gold_table_count", min_value=len(CORE_GOLD_TABLES)
            ),
            __import__("great_expectations").expectations.ExpectColumnValuesToBeBetween(
                column="fact_order_rows", min_value=1
            ),
        ],
        output_root=_quality_dir(run_root),
        window=_window_payload(window),
    )
    _render_run_docs(_quality_dir(run_root))
    result = _write_stage(
        function_name="validate_silver_gold",
        run_id=run_id,
        window=window,
        run_root=run_root,
        payload={
            "state": "success" if report.success else "failed",
            "silver_table_count": len(silver_inventory),
            "gold_row_counts": gold_counts,
            "gate": report.to_dict(),
            "gx_report": "quality/coursework_core_gold_contract.json",
        },
    )
    if report.blocks_dag and not report.success:
        raise RuntimeError("Core Silver/Gold validation failed.")
    return result


def compute_offline_features(
    *, run_id: str, start_ts: str, end_ts: str, settings: CourseworkPipelineSettings, run_root: Path
) -> dict[str, Any]:
    window = _window(start_ts, end_ts)
    spark_root = run_root / "spark_features"
    _prepare_spark_evidence_root(spark_root)
    with _working_directory(REPO_ROOT):
        summary = run_feature_compute(
            start_ts=start_ts,
            end_ts=end_ts,
            mode="hourly",
            evidence_root=spark_root,
        )
    columns = {
        table_name: [
            row[0]
            for row in execute_trino_query(
                f"describe iceberg.gold.{table_name}",
                trino_url=settings.trino_url,
                user=settings.trino_user,
            )["rows"]
        ]
        for table_name in settings.feature_tables
    }
    return _write_stage(
        function_name="compute_offline_features",
        run_id=run_id,
        window=window,
        run_root=run_root,
        payload={
            "state": "success",
            "spark_application_id": _latest_spark_application_id(),
            "feature_tables": list(settings.feature_tables),
            "feature_columns": columns,
            "spark_summary": summary,
        },
    )


def validate_offline_features(
    *, run_id: str, start_ts: str, end_ts: str, settings: CourseworkPipelineSettings, run_root: Path
) -> dict[str, Any]:
    window = _window(start_ts, end_ts)
    rows = []
    for table_name in settings.feature_tables:
        columns = {
            row[0]
            for row in execute_trino_query(
                f"describe iceberg.gold.{table_name}",
                trino_url=settings.trino_url,
                user=settings.trino_user,
            )["rows"]
        }
        rows.append(
            {
                "table_name": table_name,
                "row_count": _trino_count(table_name, settings),
                "has_event_timestamp": "event_timestamp" in columns,
                "has_created": "created" in columns,
                "has_created_ts": "created_ts" in columns,
            }
        )
    report = _validate_pandas_dataframe(
        dataframe=pd.DataFrame(rows),
        datasource_name="coursework_features",
        asset_name="coursework_features_asset",
        suite_name="coursework_feature_contract",
        layer="gold_trino",
        expectations=[
            __import__("great_expectations").expectations.ExpectColumnValuesToBeBetween(
                column="row_count", min_value=1
            ),
            __import__("great_expectations").expectations.ExpectColumnValuesToBeInSet(
                column="has_event_timestamp", value_set=[True]
            ),
            __import__("great_expectations").expectations.ExpectColumnValuesToBeInSet(
                column="has_created", value_set=[True]
            ),
            __import__("great_expectations").expectations.ExpectColumnValuesToBeInSet(
                column="has_created_ts", value_set=[False]
            ),
        ],
        output_root=_quality_dir(run_root),
        window=_window_payload(window),
    )
    _render_run_docs(_quality_dir(run_root))
    result = _write_stage(
        function_name="validate_offline_features",
        run_id=run_id,
        window=window,
        run_root=run_root,
        payload={
            "state": "success" if report.success else "failed",
            "feature_results": rows,
            "gate": report.to_dict(),
            "gx_report": "quality/coursework_feature_contract.json",
        },
    )
    if report.blocks_dag and not report.success:
        raise RuntimeError("Offline feature validation failed.")
    return result
