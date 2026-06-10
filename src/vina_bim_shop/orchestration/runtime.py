from __future__ import annotations

import json
import os
import re
import shlex
import subprocess
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd
import requests

from vina_bim_shop.kafka.bootstrap import bootstrap_topics
from vina_bim_shop.kafka.bronze_sink import register_bronze_sink
from vina_bim_shop.kafka.schema_registry import register_schema_subjects
from vina_bim_shop.lakehouse.spark.evidence import capture_evidence
from vina_bim_shop.lakehouse.spark.runner import run_batch_pipeline
from vina_bim_shop.lakehouse.spark.trino import execute_trino_query
from vina_bim_shop.lakehouse.spark.window import BatchWindow
from vina_bim_shop.pinot.bootstrap import apply_assets
from vina_bim_shop.pinot.query_examples import run_query_examples
from vina_bim_shop.quality.policies import gate_outcome_for_layer, should_fail_reconciliation
from vina_bim_shop.quality.reports import ValidationReport, render_validation_docs, write_validation_report


REPO_ROOT = Path(__file__).resolve().parents[3]
ADR06_EVIDENCE_ROOT = REPO_ROOT / "evidence" / "08_airflow_gx"
RUNS_ROOT = ADR06_EVIDENCE_ROOT / "runs"
DOCS_ROOT = ADR06_EVIDENCE_ROOT / "gx_data_docs"


def _slugify_run_id(run_id: str) -> str:
    return re.sub(r"[^a-zA-Z0-9._-]+", "_", run_id)


def build_run_root(dag_id: str, run_id: str) -> Path:
    path = RUNS_ROOT / dag_id / _slugify_run_id(run_id)
    path.mkdir(parents=True, exist_ok=True)
    return path


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")


def _run_command(command: list[str], *, cwd: Path | None = None) -> str:
    completed = subprocess.run(
        command,
        cwd=str(cwd or REPO_ROOT),
        check=True,
        text=True,
        capture_output=True,
        encoding="utf-8",
        errors="replace",
    )
    return completed.stdout


@contextmanager
def _working_directory(path: Path):
    original = Path.cwd()
    os.chdir(path)
    try:
        yield
    finally:
        os.chdir(original)


def _get_json(url: str) -> Any:
    response = requests.get(url, timeout=30)
    response.raise_for_status()
    if not response.content:
        return {}
    if "application/json" in response.headers.get("content-type", ""):
        return response.json()
    return {"text": response.text}


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


def _validate_pandas_dataframe(
    *,
    dataframe: pd.DataFrame,
    datasource_name: str,
    asset_name: str,
    suite_name: str,
    layer: str,
    expectations: list[Any],
    output_root: Path,
    window: dict[str, str] | None = None,
) -> ValidationReport:
    import great_expectations as gx

    context = gx.get_context(mode="ephemeral")
    datasource = context.data_sources.add_pandas(name=datasource_name)
    asset = datasource.add_dataframe_asset(name=asset_name)
    batch_definition = asset.add_batch_definition_whole_dataframe(f"{asset_name}_whole_dataframe")
    batch = batch_definition.get_batch(batch_parameters={"dataframe": dataframe})
    results = [batch.validate(expectation).to_json_dict() for expectation in expectations]
    success = all(result.get("success", False) for result in results)
    gate = gate_outcome_for_layer(layer, success=success)
    report = ValidationReport(
        layer=layer,
        suite_name=suite_name,
        success=success,
        status=gate.status,
        severity=gate.severity.value,
        blocks_dag=gate.blocks_dag,
        requires_quarantine=gate.requires_quarantine,
        summary=f"{sum(1 for result in results if result.get('success'))}/{len(results)} expectations passed.",
        artifacts=[str(output_root.name + "/" + f"{suite_name}.json").replace("\\", "/")],
        details={"results": results},
        window=window,
    )
    write_validation_report(report=report, output_path=output_root / f"{suite_name}.json")
    return report


def _render_docs(reports: list[ValidationReport]) -> None:
    render_validation_docs(reports=reports, docs_root=DOCS_ROOT)


def _latest_quality_reports() -> list[ValidationReport]:
    reports_by_suite: dict[str, tuple[float, ValidationReport]] = {}
    for path in RUNS_ROOT.rglob("quality/*.json"):
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
            report = ValidationReport(**payload)
        except (OSError, TypeError, ValueError):
            continue
        current = reports_by_suite.get(report.suite_name)
        modified_at = path.stat().st_mtime
        if current is None or modified_at > current[0]:
            reports_by_suite[report.suite_name] = (modified_at, report)
    return [report for _, report in sorted(reports_by_suite.values(), key=lambda item: item[1].suite_name)]


def _docs_reports_with(extra_reports: list[ValidationReport]) -> list[ValidationReport]:
    reports = {report.suite_name: report for report in _latest_quality_reports()}
    for report in extra_reports:
        reports[report.suite_name] = report
    preferred_order = [
        "bronze_raw_minio",
        "gold_trino_contract",
        "pinot_query_contract",
        "datahub_ingestion",
    ]
    return sorted(
        reports.values(),
        key=lambda report: (
            preferred_order.index(report.suite_name) if report.suite_name in preferred_order else len(preferred_order),
            report.suite_name,
        ),
    )


def _window_payload(window: BatchWindow) -> dict[str, str]:
    return {
        "start_ts": window.start_ts.isoformat().replace("+00:00", "Z"),
        "end_ts": window.end_ts.isoformat().replace("+00:00", "Z"),
        "mode": window.mode,
    }


def _prepare_spark_evidence_root(evidence_root: Path) -> None:
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


def _capture_airflow_batch_evidence(*, evidence_root: str | Path) -> dict[str, Any]:
    return capture_evidence(
        evidence_root=evidence_root,
        master_url="http://spark-master:8080",
        history_url="http://spark-history-server:18080",
    )


def run_kafka_topic_bootstrap(*, run_id: str) -> dict[str, Any]:
    run_root = build_run_root("kafka_topic_bootstrap", run_id)
    bootstrap_topics(
        runner=lambda command: subprocess.run(command, check=True, cwd=str(REPO_ROOT)),
        bootstrap_server="kafka:29092",
        topics_file=REPO_ROOT / "infra" / "kafka" / "topics.yaml",
    )

    schema_responses = register_schema_subjects(
        registry_url="http://schema-registry:8081",
        schemas_dir=REPO_ROOT / "infra" / "kafka" / "schemas",
        evidence_root=run_root,
    )
    connector_response = register_bronze_sink(
        connect_url="http://kafka-connect:8083",
        template_path=REPO_ROOT / "infra" / "kafka" / "connect" / "source-events-s3-sink.template.json",
        connector_name="source-events-s3-sink",
        bronze_bucket="bronze",
        minio_endpoint="http://minio:9000",
        minio_region="us-east-1",
        minio_access_key="vina_minio",
        minio_secret_key="vina_minio_password",
    )

    health = {
        "captured_at": _utc_now(),
        "schema_registry": _get_json("http://schema-registry:8081/subjects"),
        "kafka_connect": _get_json("http://kafka-connect:8083/connectors"),
    }
    topic_list = _run_command(
        ["docker", "compose", "exec", "-T", "kafka", "kafka-topics", "--bootstrap-server", "kafka:29092", "--list"],
        cwd=REPO_ROOT,
    )
    _write_json(run_root / "bootstrap_health.json", health)
    _write_json(run_root / "connector_response.json", connector_response)
    (run_root / "topic_list.txt").write_text(topic_list, encoding="utf-8")
    manifest = {
        "captured_at": _utc_now(),
        "topics_file": "infra/kafka/topics.yaml",
        "registered_subject_count": len(schema_responses),
        "artifacts": [
            "bootstrap_health.json",
            "connector_response.json",
            "topic_list.txt",
        ],
    }
    _write_json(run_root / "run_manifest.json", manifest)
    return manifest


def run_pinot_bootstrap(*, run_id: str) -> dict[str, Any]:
    run_root = build_run_root("pinot_bootstrap", run_id)
    manifest = apply_assets(
        controller_url="http://pinot-controller:9000",
        evidence_root=run_root,
    )
    query_manifest = run_query_examples(
        evidence_root=run_root,
        broker_url="http://pinot-broker:8000",
        trino_url="http://trino:8080",
        trino_user="vina_analyst",
    )
    result = {
        "captured_at": _utc_now(),
        "tables": manifest["tables"],
        "artifacts": sorted({"pinot_bootstrap_manifest.json", *query_manifest["artifacts"]}),
    }
    _write_json(run_root / "run_manifest.json", result)
    return result


def run_hourly_batch_lakehouse(*, run_id: str, start_ts: str, end_ts: str) -> dict[str, Any]:
    run_root = build_run_root("hourly_batch_lakehouse", run_id)
    window = BatchWindow.from_args(start_ts=start_ts, end_ts=end_ts, mode="hourly")
    window_payload = _window_payload(window)
    spark_evidence_root = run_root / "spark_batch"

    reports_root = run_root / "quality"
    bronze_paths = _list_bronze_objects()
    quarantine_counts = _count_quarantine_records()
    bronze_records = [
        {"path": path, "quarantine_count": sum(quarantine_counts.values())}
        for path in bronze_paths
    ] or [{"path": None, "quarantine_count": sum(quarantine_counts.values())}]
    bronze_report = _validate_pandas_dataframe(
        dataframe=pd.DataFrame(bronze_records),
        datasource_name="bronze_runtime",
        asset_name="bronze_landing_asset",
        suite_name="bronze_raw_minio",
        layer="bronze_raw",
        expectations=[
            __import__("great_expectations").expectations.ExpectColumnValuesToNotBeNull(column="path"),
            __import__("great_expectations").expectations.ExpectTableRowCountToBeBetween(min_value=1),
        ],
        output_root=reports_root,
        window=window_payload,
    )
    bronze_report.details["quarantine_counts"] = quarantine_counts

    _prepare_spark_evidence_root(spark_evidence_root)
    with _working_directory(REPO_ROOT):
        spark_summary = run_batch_pipeline(
            start_ts=window_payload["start_ts"],
            end_ts=window_payload["end_ts"],
            mode="hourly",
            evidence_root=spark_evidence_root,
            capture_evidence_fn=_capture_airflow_batch_evidence,
        )

    trino_validation_frame = pd.DataFrame(
        [
            {
                "gold_table_count": len(
                    execute_trino_query("show tables from iceberg.gold", trino_url="http://trino:8080")["rows"]
                ),
                "fact_order_rows": execute_trino_query(
                    "select count(*) as fact_order_count from iceberg.gold.fact_order",
                    trino_url="http://trino:8080",
                )["rows"][0][0],
            }
        ]
    )
    gold_report = _validate_pandas_dataframe(
        dataframe=trino_validation_frame,
        datasource_name="trino_runtime",
        asset_name="gold_validation_asset",
        suite_name="gold_trino_contract",
        layer="gold_trino",
        expectations=[
            __import__("great_expectations").expectations.ExpectColumnValuesToBeBetween(
                column="gold_table_count",
                min_value=1,
            ),
            __import__("great_expectations").expectations.ExpectColumnValuesToBeBetween(
                column="fact_order_rows",
                min_value=1,
            ),
        ],
        output_root=reports_root,
        window=window_payload,
    )

    reports = [bronze_report, gold_report]
    _prepare_gx_docs_root()
    _render_docs(reports)
    manifest = {
        "captured_at": _utc_now(),
        "window": window_payload,
        "spark_summary_path": "spark_batch/run_batch_summary.json",
        "quality_reports": [f"quality/{report.suite_name}.json" for report in reports],
        "data_docs_index": str(DOCS_ROOT.relative_to(REPO_ROOT) / "index.html").replace("\\", "/"),
        "artifacts": [
            "quality/bronze_raw_minio.json",
            "quality/gold_trino_contract.json",
            "spark_batch/run_batch_summary.json",
        ],
    }
    _write_json(run_root / "run_manifest.json", manifest)
    if gold_report.blocks_dag and not gold_report.success:
        raise RuntimeError("Gold Trino validation failed.")
    return {"manifest": manifest, "spark_summary": spark_summary}


def run_reconciliation_report(*, run_id: str, start_ts: str, end_ts: str) -> dict[str, Any]:
    run_root = build_run_root("reconciliation_report", run_id)
    window = BatchWindow.from_args(start_ts=start_ts, end_ts=end_ts, mode="hourly")
    window_payload = _window_payload(window)

    query_manifest = run_query_examples(
        evidence_root=run_root,
        broker_url="http://pinot-broker:8000",
        trino_url="http://trino:8080",
        trino_user="vina_analyst",
        start_ts=window_payload["start_ts"],
        end_ts=window_payload["end_ts"],
    )
    pinot_result = json.loads((run_root / "query_outputs" / "pinot_dashboard_results.json").read_text(encoding="utf-8"))
    dashboard_rows = len(pinot_result["dashboard_contract"].get("resultTable", {}).get("rows", []) or [])
    pinot_report = _validate_pandas_dataframe(
        dataframe=pd.DataFrame([{"dashboard_rows": dashboard_rows}]),
        datasource_name="pinot_runtime",
        asset_name="pinot_dashboard_asset",
        suite_name="pinot_query_contract",
        layer="pinot_queries",
        expectations=[
            __import__("great_expectations").expectations.ExpectColumnValuesToBeBetween(
                column="dashboard_rows",
                min_value=0,
            )
        ],
        output_root=run_root / "quality",
        window=window_payload,
    )
    reconciliation_success = dashboard_rows > 0
    manifest = {
        "captured_at": _utc_now(),
        "window": window_payload,
        "quality_reports": [f"quality/{pinot_report.suite_name}.json"],
        "query_manifest": "query_examples_manifest.json",
        "artifacts": sorted({*query_manifest["artifacts"], "quality/pinot_query_contract.json"}),
    }
    _write_json(run_root / "run_manifest.json", manifest)
    _render_docs([pinot_report])
    if should_fail_reconciliation(pinot_success=pinot_report.success, reconciliation_success=reconciliation_success):
        raise RuntimeError("Reconciliation report did not find Pinot rows for the selected hourly window.")
    return manifest


def run_datahub_ingestion(*, run_id: str) -> dict[str, Any]:
    run_root = build_run_root("datahub_ingestion", run_id)
    recipes_dir = Path("/opt/airflow/recipes")
    if not recipes_dir.exists():
        recipes_dir = REPO_ROOT / "infra" / "governance" / "recipes"
    recipes = [
        ("kafka_topics", recipes_dir / "kafka_topics.yml"),
        ("minio_storage", recipes_dir / "minio_storage.yml"),
        ("trino_tables", recipes_dir / "trino_tables.yml"),
        ("dbt_legacy", recipes_dir / "dbt_legacy.yml"),
    ]
    recipe_results: dict[str, dict[str, Any]] = {}
    all_success = True
    for name, path in recipes:
        if not path.exists():
            recipe_results[name] = {"status": "skipped", "reason": f"recipe file not found: {path}"}
            continue
        try:
            result = _run_command(["datahub", "ingest", "run", "-c", str(path)])
            recipe_results[name] = {"status": "success", "output": result[:2000]}
        except subprocess.CalledProcessError as exc:
            recipe_results[name] = {"status": "warning", "output": str(exc)[:2000]}
            all_success = False

    lineage_results = _run_custom_lineage_emission()
    recipe_results["custom_lineage"] = lineage_results

    manifest = {
        "captured_at": _utc_now(),
        "status": "success" if all_success else "warning",
        "ingestion_results": recipe_results,
    }
    _write_json(run_root / "run_manifest.json", manifest)
    datahub_report = ValidationReport(
        layer="datahub",
        suite_name="datahub_ingestion",
        success=all_success,
        status="success" if all_success else "warning",
        severity="warning",
        blocks_dag=False,
        requires_quarantine=False,
        summary="ADR 07 ingestion completed." if all_success else "Some recipes emitted warnings.",
        artifacts=["run_manifest.json"],
    )
    _render_docs(_docs_reports_with([datahub_report]))
    return manifest


def _run_custom_lineage_emission() -> dict[str, Any]:
    from vina_bim_shop.datahub_lineage.spark_lineage import emit_spark_batch_lineage
    from vina_bim_shop.datahub_lineage.flink_lineage import emit_flink_streaming_lineage
    from vina_bim_shop.datahub_lineage.emitter import DataHubLineageEmitter

    results: dict[str, Any] = {}
    gms_url = "http://datahub-gms:8080"

    try:
        spark_result = emit_spark_batch_lineage(gms_url)
        results["spark"] = spark_result
    except Exception as exc:
        results["spark"] = {"status": "warning", "reason": str(exc)}

    try:
        flink_result = emit_flink_streaming_lineage(gms_url)
        results["flink"] = flink_result
    except Exception as exc:
        results["flink"] = {"status": "warning", "reason": str(exc)}

    try:
        emitter = DataHubLineageEmitter(gms_url)
        _bootstrap_governance_vocabulary(emitter)
        results["vocabulary"] = "success"
    except Exception as exc:
        results["vocabulary"] = {"status": "warning", "reason": str(exc)}

    try:
        from vina_bim_shop.datahub_lineage.gx_assertions import emit_gx_assertions_to_datahub

        gx_results = emit_gx_assertions_to_datahub(gms_url)
        results["gx_assertions"] = gx_results
    except Exception as exc:
        results["gx_assertions"] = {"status": "warning", "reason": str(exc)}

    return results


def _bootstrap_governance_vocabulary(emitter: Any) -> None:
    tags = {
        "bronze": "Raw source-fidelity data",
        "silver": "Cleaned and standardized data",
        "gold": "Business-ready canonical data",
        "official": "Approved source for historical KPI reporting",
        "provisional": "Fresh operational view subject to reconciliation",
        "pii_safe": "Synthetic or non-sensitive local coursework data",
        "regression_oracle": "dbt-DuckDB compatibility artifact used for parity checks",
        "quality_gate": "Dataset or job has GX validation attached",
    }
    for tag_name, _description in tags.items():
        try:
            emitter.emit_tag("urn:li:tag:" + tag_name, tag_name)
        except Exception:
            pass

    owners = {
        "data_engineer": "TECHNICAL_OWNER",
        "airflow": "TECHNICAL_OWNER",
    }
    for owner_name, owner_type in owners.items():
        try:
            emitter.emit_ownership(
                "urn:li:corpuser:" + owner_name,
                "urn:li:corpuser:" + owner_name,
                owner_type,
            )
        except Exception:
            pass


def run_local_evidence_build(*, run_id: str) -> dict[str, Any]:
    run_root = build_run_root("local_evidence_build", run_id)
    airflow_health = _get_json("http://airflow-webserver:8080/health")
    docs_index = DOCS_ROOT / "index.html"
    latest_manifests = sorted(str(path.relative_to(REPO_ROOT)).replace("\\", "/") for path in RUNS_ROOT.rglob("run_manifest.json"))
    manifest = {
        "captured_at": _utc_now(),
        "airflow_health": airflow_health,
        "docs_index_exists": docs_index.is_file(),
        "latest_run_manifests": latest_manifests,
        "artifacts": [
            "gx_data_docs/index.html",
        ],
    }
    _write_json(run_root / "run_manifest.json", manifest)
    _write_json(ADR06_EVIDENCE_ROOT / "airflow_health.json", airflow_health)
    _write_json(ADR06_EVIDENCE_ROOT / "run_manifest.json", manifest)
    return manifest
