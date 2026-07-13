"""Build and verify the fail-closed Mini-Coursework rubric evidence manifest."""

from __future__ import annotations

import argparse
import hashlib
import json
from collections.abc import Callable, Iterable
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


Gate = Callable[[Path], None]


@dataclass(frozen=True)
class RowRequirement:
    """Immutable evidence contract for one rubric row."""

    row: int
    points: int
    implementation_paths: tuple[str, ...]
    evidence_paths: tuple[str, ...]
    notes: str
    gate: Gate


def _read_json(root: Path, relative_path: str) -> dict[str, Any]:
    return json.loads((root / relative_path).read_text(encoding="utf-8"))


def _require(condition: bool, message: str) -> None:
    if not condition:
        raise ValueError(message)


def _require_text(root: Path, relative_path: str, phrase: str) -> None:
    _require(phrase in (root / relative_path).read_text(encoding="utf-8"), f"{relative_path} lacks {phrase!r}")


def _gate_row_2(root: Path) -> None:
    coverage = _read_json(root, "evidence/final_integration/public_documentation_coverage.json")
    _require(coverage.get("success") is True, "public API coverage is not successful")
    _require(coverage.get("summary", {}).get("coverage_percent") == 100.0, "public API coverage is not 100%")
    for relative_path, phrase in [
        ("README.md", "Rubric Evidence and Navigation"),
        ("README.md", "mini_coursework_rubric_manifest.json"),
        ("deliverables/13_mini_coursework_rubric_evidence.md", "| 46 |"),
        ("architecture/diagrams/README.md", "Arrow conventions"),
    ]:
        _require_text(root, relative_path, phrase)


def _gate_docker_size(root: Path) -> None:
    comparison = _read_json(root, "evidence/00_engineering_fundamentals/kafka_connect_image_comparison.json")
    _require(comparison["optimized_size_bytes"] < comparison["baseline_size_bytes"], "optimized image is not smaller")
    _require(comparison["reduction_bytes"] > 0, "image reduction is not positive")


def _gate_multistage_connect(root: Path) -> None:
    _gate_docker_size(root)
    dockerfile = (root / "infra/kafka/connect/Dockerfile").read_text(encoding="utf-8")
    from_lines = [line for line in dockerfile.splitlines() if line.strip().upper().startswith("FROM ")]
    _require(len(from_lines) == 2, "Kafka Connect Dockerfile is not a two-stage build")
    _require(" AS plugin-builder" in from_lines[0] and "COPY --from=plugin-builder" in dockerfile, "Kafka Connect builder output is not copied into the runtime stage")
    smoke = _read_json(root, "evidence/00_engineering_fundamentals/kafka_connect_plugin_smoke.json")
    _require(smoke["connector_status"]["connector"]["state"] == "RUNNING", "Kafka Connect is not running")
    _require(smoke["plugin"]["class"] == "io.confluent.connect.s3.S3SinkConnector", "S3 plugin proof is absent")


def _gate_generator(row: int) -> Gate:
    def gate(root: Path) -> None:
        _require_text(root, "evidence/01_data_generator/rubric_evidence_summary.md", f"## Row {row} -")
        manifest = _read_json(root, "evidence/01_data_generator/run_manifest.json")
        _require(manifest.get("scale") == "medium", "generator evidence is not the medium submission run")

    return gate


def _gate_spark(root: Path) -> None:
    manifest = _read_json(root, "evidence/05_spark_batch/optimization/run_manifest.json")
    variants = manifest.get("variants", {})
    _require(set(variants) == {"skew-baseline", "skew-optimized", "high-cardinality-baseline", "high-cardinality-optimized"}, "Spark variants are incomplete")
    _require(len({details["application_id"] for details in variants.values()}) == 4, "Spark application IDs are not distinct")
    _require(manifest.get("canonical_batch_semantics_changed") is False, "canonical Spark semantics changed")


def _gate_flink(root: Path) -> None:
    comparison = _read_json(root, "evidence/06_flink_streaming/optimization/comparison.json")
    _require(comparison.get("passed") is True, "Flink comparison did not pass")
    _require(comparison.get("on_time_aggregates_equal") is True, "Flink aggregates differ")
    _require(comparison.get("correction_counts", {}).get("optimized") == 1, "optimized late correction proof is absent")


def _gate_storage_compaction(root: Path) -> None:
    result = _read_json(root, "evidence/04_lakehouse/optimization/compaction_results.json")
    rewrites = {item["table"]: item for item in result.get("rewrites", [])}
    _require(result.get("success") is True, "compaction result is unsuccessful")
    for table_name in ("gold.fact_order", "gold.fact_order_item"):
        rewrite = rewrites.get(table_name, {})
        _require(rewrite.get("status") == "rewritten" and rewrite.get("rewritten_data_files_count") == 12, f"{table_name} rewrite proof is absent")


def _gate_storage_benchmark(root: Path) -> None:
    benchmark = _read_json(root, "evidence/04_lakehouse/optimization/query_benchmark.json")
    _require(benchmark.get("before") and benchmark.get("after"), "before/after warehouse benchmark is incomplete")


def _gate_airflow(root: Path) -> None:
    base = "evidence/08_airflow_gx/coursework_pipeline/topic07_20260711T124521Z"
    manifest = _read_json(root, f"{base}/run_manifest.json")
    expected_tasks = [
        "dp1_raw_to_bronze.ingest_raw_to_bronze",
        "dp1_raw_to_bronze.validate_bronze",
        "dp2_bronze_to_silver_gold.transform_bronze_to_silver_gold",
        "dp2_bronze_to_silver_gold.validate_silver_gold",
        "dp3_offline_features.compute_offline_features",
        "dp3_offline_features.validate_offline_features",
    ]
    _require([stage["task_id"] for stage in manifest["stages"]] == expected_tasks, "Airflow task order changed")
    _require(all(stage["state"] == "success" for stage in manifest["stages"]), "Airflow stage is not successful")
    dp1 = _read_json(root, f"{base}/dp1_ingest.json")
    dp2 = _read_json(root, f"{base}/dp2_transform.json")
    dp3 = _read_json(root, f"{base}/dp3_validate.json")
    _require(len(dp1["bronze_batch_objects"]) == 10 and len(dp1["bronze_event_objects"]) == 5, "DP1 object proof is incomplete")
    _require(dp2["spark_application_id"] == "app-20260711124824-0000" and len(dp2["core_gold_tables"]) == 19, "DP2 proof changed")
    _require(all(item["row_count"] > 0 and item["has_event_timestamp"] and item["has_created"] and not item["has_created_ts"] for item in dp3["feature_results"]), "DP3 feature contract is invalid")


def _gate_datahub(stage: str, proof: str) -> Gate:
    def gate(root: Path) -> None:
        recovery = _read_json(root, "evidence/09_datahub_governance/runtime_recovery/run_manifest.json")
        coursework = _read_json(root, "evidence/09_datahub_governance/coursework_pipeline/run_manifest.json")
        screenshots = _read_json(root, "evidence/09_datahub_governance/coursework_pipeline/ui_screenshot_manifest.json")["screenshots"]
        _require(recovery["search"]["status"] == "success", "DataHub search recovery is unsuccessful")
        _require(coursework["status"] == "success", "DataHub coursework capture is unsuccessful")
        item = next((shot for shot in screenshots if shot["id"] == f"{stage}_{proof}"), None)
        _require(item is not None and bool(item["sha256"]) and item["width"] > 0 and item["height"] > 0, f"DataHub {stage} {proof} screenshot is invalid")

    return gate


def _gate_schema(root: Path) -> None:
    manifest = _read_json(root, "evidence/02_schema_design/run_manifest.json")
    _require(manifest["schema_design_model_counts"] == {"bronze": 16, "silver": 14, "gold": 22}, "all-zone ERD inventory changed")
    _require(manifest["dbt_model_count"] == 52 and manifest["dbt_test_count"] == 66, "dbt evidence is incomplete")


def _gate_features(root: Path) -> None:
    _gate_schema(root)
    catalog = (root / "evidence/02_schema_design/dbt_catalog_summary.csv").read_text(encoding="utf-8")
    for name in ("feat_customer_90d", "feat_stream_60m", "feat_customer_unified"):
        _require(name in catalog and "event_timestamp" in catalog and "created" in catalog, "feature-column evidence is incomplete")


def _gate_novel_ideas(root: Path) -> None:
    manifest = _read_json(root, "evidence/10_novel_ideas/run_manifest.json")
    _require(manifest.get("status") == "success", "novel-ideas manifest is unsuccessful")
    _require(manifest.get("success_gates") == {"idea_1": True, "idea_2": True}, "novel-ideas gates are incomplete")
    _require(all(item.get("present") and item.get("sha256") for item in manifest.get("screenshots", [])), "novel-ideas screenshots are invalid")


def _row(
    row: int,
    points: int,
    implementation: tuple[str, ...],
    evidence: tuple[str, ...],
    notes: str,
    gate: Gate,
) -> RowRequirement:
    return RowRequirement(row, points, implementation, evidence, notes, gate)


_GENERATOR_IMPLEMENTATION = ("configs/generator/base.yaml", "src/vina_bim_shop/generators/runner.py")
_GENERATOR_EVIDENCE = ("evidence/01_data_generator/rubric_evidence_summary.md", "evidence/01_data_generator/run_manifest.json")
_SPARK_IMPLEMENTATION = ("scripts/spark/run_optimization_experiments.py", "src/vina_bim_shop/lakehouse/spark/runner.py")
_SPARK_EVIDENCE = ("evidence/05_spark_batch/optimization/run_manifest.json", "evidence/05_spark_batch/optimization/optimization_report.md")
_FLINK_IMPLEMENTATION = ("src/vina_bim_shop/flink/baseline_experiment.py", "configs/pipelines/flink_baseline_experiment.yaml")
_FLINK_EVIDENCE = ("evidence/06_flink_streaming/optimization/run_manifest.json", "evidence/06_flink_streaming/optimization/comparison.json", "evidence/06_flink_streaming/optimization/challenge_samples.json")
_AIRFLOW_BASE = "evidence/08_airflow_gx/coursework_pipeline/topic07_20260711T124521Z"
_AIRFLOW_IMPLEMENTATION = ("src/vina_bim_shop/orchestration/mini_coursework_pipeline.py", "infra/orchestration/airflow/dags/mini_coursework_pipeline.py")
_DATAHUB_IMPLEMENTATION = ("src/vina_bim_shop/datahub_lineage/emitter.py", "scripts/datahub/capture_evidence.py")


ROW_REQUIREMENTS = (
    _row(2, 10, ("README.md", "architecture/diagrams/README.md", "deliverables/13_mini_coursework_rubric_evidence.md", "scripts/qa/audit_public_documentation.py"), ("evidence/final_integration/public_documentation_coverage.json",), "Central reviewer navigation and declared API coverage.", _gate_row_2),
    _row(3, 1, ("infra/kafka/connect/Dockerfile",), ("evidence/00_engineering_fundamentals/kafka_connect_image_comparison.json",), "Measured Kafka Connect image reduction.", _gate_docker_size),
    _row(4, 1, ("infra/kafka/connect/Dockerfile",), ("evidence/00_engineering_fundamentals/kafka_connect_plugin_smoke.json",), "Multi-stage Kafka Connect build and runtime smoke proof.", _gate_multistage_connect),
    *(_row(row, 2, _GENERATOR_IMPLEMENTATION, _GENERATOR_EVIDENCE, f"Generator Row {row} evidence.", _gate_generator(row)) for row in range(5, 15)),
    *(_row(row, 2, _SPARK_IMPLEMENTATION, _SPARK_EVIDENCE, f"Spark optimization Row {row} evidence.", _gate_spark) for row in range(15, 21)),
    *(_row(row, 2, _FLINK_IMPLEMENTATION, _FLINK_EVIDENCE, f"Flink optimization Row {row} evidence.", _gate_flink) for row in range(21, 26)),
    _row(26, 2, ("scripts/lakehouse/optimize_iceberg.py",), ("evidence/04_lakehouse/optimization/compaction_results.json", "evidence/04_lakehouse/optimization/before_file_stats.csv", "evidence/04_lakehouse/optimization/after_file_stats.csv"), "Controlled compaction proof; not a general performance claim.", _gate_storage_compaction),
    _row(27, 2, ("scripts/analytics/benchmark_duckdb_index.py",), ("evidence/04_lakehouse/optimization/query_benchmark.json", "evidence/10_duckdb_dbt_local_analytics/index_optimization/index_benchmark.json"), "Reproducible warehouse/index benchmark evidence.", _gate_storage_benchmark),
    *(_row(row, 2, _AIRFLOW_IMPLEMENTATION, (f"{_AIRFLOW_BASE}/run_manifest.json", f"{_AIRFLOW_BASE}/{artifact}", "evidence/08_airflow_gx/screenshots/mini_coursework_pipeline_graph.png", "evidence/08_airflow_gx/screenshots/mini_coursework_pipeline_grid.png"), f"Airflow coursework stage Row {row}.", _gate_airflow) for row, artifact in zip(range(28, 34), ("dp1_ingest.json", "dp1_validate.json", "dp2_transform.json", "dp2_validate.json", "dp3_compute.json", "dp3_validate.json"), strict=True)),
    _row(34, 2, _DATAHUB_IMPLEMENTATION, ("evidence/09_datahub_governance/coursework_pipeline/dp1_lineage_capture.json", "evidence/09_datahub_governance/screenshots/datahub_dp1_lineage.png", "evidence/09_datahub_governance/coursework_pipeline/ui_screenshot_manifest.json"), "DP1 lineage UI proof.", _gate_datahub("dp1", "lineage")),
    _row(35, 2, _DATAHUB_IMPLEMENTATION, ("evidence/09_datahub_governance/coursework_pipeline/dp1_contract_capture.json", "evidence/09_datahub_governance/screenshots/datahub_dp1_contract.png", "evidence/09_datahub_governance/coursework_pipeline/ui_screenshot_manifest.json"), "DP1 contract UI proof.", _gate_datahub("dp1", "contract")),
    _row(36, 2, _DATAHUB_IMPLEMENTATION, ("evidence/09_datahub_governance/coursework_pipeline/dp2_lineage_capture.json", "evidence/09_datahub_governance/screenshots/datahub_dp2_lineage.png", "evidence/09_datahub_governance/coursework_pipeline/ui_screenshot_manifest.json"), "DP2 lineage UI proof.", _gate_datahub("dp2", "lineage")),
    _row(37, 2, _DATAHUB_IMPLEMENTATION, ("evidence/09_datahub_governance/coursework_pipeline/dp2_contract_capture.json", "evidence/09_datahub_governance/screenshots/datahub_dp2_contract.png", "evidence/09_datahub_governance/coursework_pipeline/ui_screenshot_manifest.json"), "DP2 contract UI proof.", _gate_datahub("dp2", "contract")),
    _row(38, 2, _DATAHUB_IMPLEMENTATION, ("evidence/09_datahub_governance/coursework_pipeline/dp3_lineage_capture.json", "evidence/09_datahub_governance/screenshots/datahub_dp3_lineage.png", "evidence/09_datahub_governance/coursework_pipeline/ui_screenshot_manifest.json"), "DP3 lineage UI proof.", _gate_datahub("dp3", "lineage")),
    _row(39, 2, _DATAHUB_IMPLEMENTATION, ("evidence/09_datahub_governance/coursework_pipeline/dp3_contract_capture.json", "evidence/09_datahub_governance/screenshots/datahub_dp3_contract.png", "evidence/09_datahub_governance/coursework_pipeline/ui_screenshot_manifest.json"), "DP3 contract UI proof.", _gate_datahub("dp3", "contract")),
    _row(40, 2, ("architecture/diagrams/schema_design.puml",), ("evidence/02_schema_design/run_manifest.json", "evidence/02_schema_design/screenshots/schema_design.png"), "Generated all-zone ERD proof.", _gate_schema),
    _row(41, 1, ("infra/analytics/dbt/models/gold/dim_customer.sql",), ("evidence/02_schema_design/schema_inventory.csv",), "SCD2-compatible dimension columns.", _gate_schema),
    _row(42, 1, ("infra/analytics/dbt/models/gold/_features.yml", "src/vina_bim_shop/lakehouse/spark/sql.py"), ("evidence/02_schema_design/dbt_catalog_summary.csv", "evidence/02_schema_design/schema_inventory.csv"), "Exact feature created/event_timestamp contract.", _gate_features),
    _row(43, 2, ("architecture/diagrams/erd/physical_gold_model.puml",), ("architecture/diagrams/erd/physical_gold_model.png",), "Dimension/fact relationship proof.", _gate_schema),
    _row(44, 2, ("deliverables/02_schema_design.md",), ("evidence/02_schema_design/run_manifest.json",), "Bronze/Silver/Gold naming-convention proof.", _gate_schema),
    _row(45, 5, ("scripts/qa/capture_novel_ideas.py", "deliverables/12_novel_ideas.md"), ("evidence/10_novel_ideas/idea_1_duckdb_dbt.json", "evidence/10_novel_ideas/run_manifest.json", "evidence/10_novel_ideas/screenshots/idea_1_duckdb_dbt_lineage.png"), "Novel Idea 1: DuckDB/dbt local analytics.", _gate_novel_ideas),
    _row(46, 5, ("scripts/qa/capture_novel_ideas.py", "deliverables/12_novel_ideas.md"), ("evidence/10_novel_ideas/idea_2_pinot_realtime.json", "evidence/10_novel_ideas/run_manifest.json", "evidence/10_novel_ideas/screenshots/idea_2_pinot_realtime_query.png"), "Novel Idea 2: Pinot realtime serving.", _gate_novel_ideas),
)


def _resolve_required_path(root: Path, relative_path: str) -> Path:
    candidate = (root / relative_path).resolve()
    try:
        candidate.relative_to(root.resolve())
    except ValueError as error:
        raise ValueError(f"Requirement path is outside the repository: {relative_path}") from error
    return candidate


def _validate_requirement(requirement: RowRequirement) -> None:
    paths = requirement.implementation_paths + requirement.evidence_paths
    if len(set(paths)) != len(paths):
        raise ValueError(f"Row {requirement.row} has duplicate required paths")
    for path in paths:
        if not path or Path(path).is_absolute() or ".." in Path(path).parts:
            raise ValueError(f"Row {requirement.row} has an outside or invalid path: {path}")


def _hash_paths(root: Path, paths: Iterable[str], failures: list[str]) -> dict[str, str]:
    hashes: dict[str, str] = {}
    for relative_path in paths:
        path = _resolve_required_path(root, relative_path)
        if not path.is_file():
            failures.append(f"missing required artifact: {relative_path}")
        elif path.stat().st_size == 0:
            failures.append(f"empty required artifact: {relative_path}")
        else:
            hashes[relative_path] = hashlib.sha256(path.read_bytes()).hexdigest()
    return hashes


def build_manifest(repo_root: Path, *, requirements: tuple[RowRequirement, ...] = ROW_REQUIREMENTS) -> dict[str, object]:
    """Build a truthful, hash-bound rubric manifest without mutating upstream evidence."""

    rows: list[dict[str, object]] = []
    for requirement in requirements:
        _validate_requirement(requirement)
        failures: list[str] = []
        implementation_hashes = _hash_paths(repo_root, requirement.implementation_paths, failures)
        evidence_hashes = _hash_paths(repo_root, requirement.evidence_paths, failures)
        if not failures:
            try:
                requirement.gate(repo_root)
            except (KeyError, TypeError, ValueError, json.JSONDecodeError) as error:
                failures.append(f"unsupported evidence: {error}")
        required_path_count = len(requirement.implementation_paths + requirement.evidence_paths)
        captured_path_count = len(implementation_hashes) + len(evidence_hashes)
        status = "Satisfied" if not failures else "Missing" if captured_path_count == 0 else "Partial"
        rows.append(
            {
                "row": requirement.row,
                "points": requirement.points,
                "status": status,
                "implementation_paths": list(requirement.implementation_paths),
                "implementation_sha256": implementation_hashes,
                "evidence_paths": list(requirement.evidence_paths),
                "evidence_sha256": evidence_hashes,
                "verification_commands": [
                    "rtk uv run python scripts/qa/build_mini_coursework_rubric_manifest.py --verify --manifest evidence/final_integration/mini_coursework_rubric_manifest.json"
                ],
                "notes": requirement.notes if not failures else f"{requirement.notes} Failure: {'; '.join(failures)}",
                "captured_path_count": captured_path_count,
                "required_path_count": required_path_count,
            }
        )
    summary = {status: sum(row["status"] == status for row in rows) for status in ("Satisfied", "Partial", "Missing")}
    return {
        "schema_version": 1,
        "verified_at": datetime.now(timezone.utc).isoformat(),
        "rows": rows,
        "summary": summary,
    }


def write_manifest(output_path: Path, manifest: dict[str, object]) -> None:
    """Persist a generated manifest, including truthful Partial or Missing rows."""

    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _normalized(manifest: dict[str, object]) -> dict[str, object]:
    normalized = dict(manifest)
    normalized.pop("verified_at", None)
    return normalized


def verify_manifest(repo_root: Path, manifest_path: Path, *, requirements: tuple[RowRequirement, ...] = ROW_REQUIREMENTS) -> None:
    """Fail when the persisted manifest or any hash-bound artifact changed."""

    persisted = json.loads(manifest_path.read_text(encoding="utf-8"))
    expected = build_manifest(repo_root, requirements=requirements)
    if _normalized(persisted) != _normalized(expected):
        raise ValueError("Persisted rubric manifest does not match current artifacts and gates")
    if expected["summary"]["Partial"] or expected["summary"]["Missing"]:
        raise ValueError("Persisted rubric manifest contains unsupported Partial or Missing rows")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--output", type=Path)
    mode.add_argument("--verify", action="store_true")
    parser.add_argument("--manifest", type=Path)
    args = parser.parse_args()
    repo_root = Path(__file__).resolve().parents[2]
    if args.verify:
        if args.manifest is None:
            parser.error("--verify requires --manifest")
        verify_manifest(repo_root, args.manifest)
        print("Mini-Coursework rubric manifest verification: success")
        return 0
    manifest = build_manifest(repo_root)
    write_manifest(args.output, manifest)
    print(f"Mini-Coursework rubric manifest: {manifest['summary']}")
    return 0 if manifest["summary"]["Partial"] == 0 and manifest["summary"]["Missing"] == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
