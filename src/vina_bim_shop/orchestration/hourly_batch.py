"""``hourly_batch_lakehouse`` DAG runtime.

Runs one logical hourly batch window:

1. Inventory the Bronze landing objects via ``mc ls --recursive`` and count
   quarantine records.
2. Validate the Bronze and Gold contracts with Great Expectations through
   ``quality_helpers._validate_pandas_dataframe``.
3. Run the Spark Bronze→Silver→Gold pipeline against the window
   (``vina_bim_shop.lakehouse.spark.runner.run_batch_pipeline``).
4. Query the resulting Gold tables through Trino and capture the
   ``gold_table_count`` / ``fact_order_rows`` row counts.
5. Render the static GX Data Docs site and write a per-run manifest.

The DAG blocks only when ``gold_report.blocks_dag and not gold_report.success``
per the ``gate_outcome_for_layer("gold_trino", success=False)`` policy.
"""
from __future__ import annotations

import shlex
from typing import Any

import pandas as pd

from vina_bim_shop.lakehouse.spark.evidence import capture_evidence
from vina_bim_shop.lakehouse.spark.runner import run_batch_pipeline
from vina_bim_shop.lakehouse.spark.trino import execute_trino_query
from vina_bim_shop.lakehouse.spark.window import BatchWindow

from .paths import (
    DOCS_ROOT,
    REPO_ROOT,
    _utc_now,
    _write_json,
    build_run_root,
)
from .mini_coursework_pipeline import (
    STAGE_FUNCTION_NAMES,
    compute_offline_features,
    ingest_raw_to_bronze,
    settings_from_environment,
    transform_bronze_to_silver_gold,
    validate_bronze,
    validate_offline_features,
    validate_silver_gold,
)
from .quality_helpers import _render_docs, _validate_pandas_dataframe, _window_payload
from .subprocess_helpers import _run_command, _working_directory


def _list_bronze_objects() -> list[str]:
    command = [
        "docker",
        "compose",
        "run",
        "--rm",
        "--no-deps",
        "--entrypoint",
        "/bin/sh",
        "minio-init",
        "-c",
        'mc alias set ALIAS http://minio:9000 "${MINIO_ROOT_USER}" "${MINIO_ROOT_PASSWORD}" >/dev/null && mc ls --recursive ALIAS/bronze',
    ]
    output = _run_command(command, cwd=REPO_ROOT)
    bronze_paths: list[str] = []
    for line in output.splitlines():
        stripped = line.strip()
        if not stripped:
            continue
        marker = stripped.find("bronze/")
        if marker == -1:
            continue
        bronze_paths.append(stripped[marker:])
    return bronze_paths


def _count_quarantine_records() -> dict[str, int]:
    candidates = {
        "bad_snapshots": REPO_ROOT / "data" / "raw" / "bad_snapshots" / "bad_snapshots.jsonl",
        "dead_letter_events": REPO_ROOT / "data" / "raw" / "kafka_topics" / "dead_letter_events" / "events.jsonl",
    }
    counts: dict[str, int] = {}
    for name, path in candidates.items():
        if not path.is_file():
            counts[name] = 0
            continue
        counts[name] = sum(1 for line in path.read_text(encoding="utf-8").splitlines() if line.strip())
    return counts


def _prepare_spark_evidence_root(evidence_root) -> None:
    quoted_path = shlex.quote(evidence_root.as_posix())
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
            f"mkdir -p {quoted_path} && chmod -R 0777 {quoted_path}",
        ],
        cwd=REPO_ROOT,
    )


def _prepare_gx_docs_root() -> None:
    _run_command(
        [
            "docker",
            "compose",
            "exec",
            "-T",
            "--user",
            "root",
            "gx-docs",
            "sh",
            "-lc",
            "mkdir -p /usr/share/nginx/html && chmod -R 0777 /usr/share/nginx/html",
        ],
        cwd=REPO_ROOT,
    )


def _capture_airflow_batch_evidence(*, evidence_root: str | Any) -> dict[str, Any]:
    return capture_evidence(
        evidence_root=evidence_root,
        master_url="http://spark-master:8080",
        history_url="http://spark-history-server:18080",
    )


def run_hourly_batch_lakehouse(*, run_id: str, start_ts: str, end_ts: str) -> dict[str, Any]:
    """Run the legacy entry point as a composition of the six DP stages."""
    run_root = build_run_root("hourly_batch_lakehouse", run_id)
    settings = settings_from_environment()
    stage_functions = (
        ingest_raw_to_bronze,
        validate_bronze,
        transform_bronze_to_silver_gold,
        validate_silver_gold,
        compute_offline_features,
        validate_offline_features,
    )
    stage_results = [
        stage_function(
            run_id=run_id,
            start_ts=start_ts,
            end_ts=end_ts,
            settings=settings,
            run_root=run_root,
        )
        for stage_function in stage_functions
    ]
    manifest = {
        "captured_at": _utc_now(),
        "legacy_dag_id": "hourly_batch_lakehouse",
        "stage_artifacts": [result["artifact"] for result in stage_results],
        "data_docs_index": str(DOCS_ROOT.relative_to(REPO_ROOT) / "index.html").replace("\\", "/"),
    }
    _write_json(run_root / "run_manifest.json", manifest)
    spark_summary = {
        "core": stage_results[2].get("spark_summary"),
        "features": stage_results[4].get("spark_summary"),
    }
    return {"manifest": manifest, "spark_summary": spark_summary}
