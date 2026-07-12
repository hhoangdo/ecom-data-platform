"""``datahub_ingestion`` DAG runtime.

Publishes metadata, tags, ownership, lineage, and GX assertions into DataHub
GMS. The DAG is the only one with ``is_paused_upon_creation=False`` so
governance ingestion can run on demand without manual un-pausing.

Boundary: this runtime publishes metadata and lineage. It does not validate
business data quality; that is owned by the Spark / Flink / GX layers.
"""
from __future__ import annotations

import json
import subprocess
from pathlib import Path
from typing import Any

from vina_bim_shop.quality.reports import ValidationReport

from .paths import REPO_ROOT, RUNS_ROOT, _utc_now, _write_json, build_run_root
from .quality_helpers import _render_docs
from .subprocess_helpers import _run_command


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

    try:
        from vina_bim_shop.datahub_lineage.coursework_pipelines import emit_coursework_pipeline

        results["coursework_pipelines"] = emit_coursework_pipeline(gms_url=gms_url)
    except Exception as exc:
        results["coursework_pipelines"] = {"status": "failed", "reason": str(exc)}

    try:
        from vina_bim_shop.datahub_lineage.gx_assertions import emit_coursework_assertions_to_datahub

        results["coursework_assertions"] = emit_coursework_assertions_to_datahub(gms_url)
    except Exception as exc:
        results["coursework_assertions"] = {"status": "failed", "reason": str(exc)}

    return results


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
    coursework_results = [
        lineage_results.get("coursework_pipelines", {}),
        lineage_results.get("coursework_assertions", {}),
    ]
    if any(result.get("status") != "success" for result in coursework_results):
        all_success = False

    manifest = {
        "captured_at": _utc_now(),
        "status": "success" if all_success else "failed",
        "ingestion_results": recipe_results,
    }
    _write_json(run_root / "run_manifest.json", manifest)
    datahub_report = ValidationReport(
        layer="datahub",
        suite_name="datahub_ingestion",
        success=all_success,
        status="success" if all_success else "failed",
        severity="warning" if all_success else "error",
        blocks_dag=not all_success,
        requires_quarantine=False,
        summary="ADR 07 ingestion completed." if all_success else "Required coursework metadata emission failed.",
        artifacts=["run_manifest.json"],
    )
    _render_docs(_docs_reports_with([datahub_report]))
    return manifest
