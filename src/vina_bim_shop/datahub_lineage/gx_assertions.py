from __future__ import annotations

import json
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from vina_bim_shop.datahub_lineage.emitter import DataHubLineageEmitter, ice_urn
from vina_bim_shop.datahub_lineage.coursework_pipelines import (
    COURSEWORK_SCHEMA_TARGETS,
    coursework_assertion_specs,
)

GX_ARTIFACTS_GLOB = "evidence/08_airflow_gx/runs/hourly_batch_lakehouse/**/quality/*.json"
REPO_ROOT = Path(__file__).resolve().parents[3]
COURSEWORK_PIPELINE_ROOT = REPO_ROOT / "evidence" / "08_airflow_gx" / "coursework_pipeline"


def _find_gx_quality_artifacts() -> list[Path]:
    matches = sorted(REPO_ROOT.glob("evidence/08_airflow_gx/runs/*/quality/*.json"))
    matches += sorted(REPO_ROOT.glob("evidence/05_spark_batch/gx/*.json"))
    return matches


def _load_gx_results(path: Path) -> dict[str, Any] | None:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None


def _latest_successful_coursework_run() -> Path | None:
    manifests = sorted(
        COURSEWORK_PIPELINE_ROOT.glob("*/run_manifest.json"),
        key=lambda path: path.stat().st_mtime,
        reverse=True,
    )
    for manifest_path in manifests:
        data = _load_gx_results(manifest_path)
        stages = data.get("stages", []) if data else []
        if len(stages) == 6 and all(stage.get("state") == "success" for stage in stages):
            return manifest_path.parent
    return None


def _run_timestamp_ms(run_id: str) -> int:
    compact = re.search(r"(\d{8}T\d{6}Z)", run_id)
    if compact:
        return int(datetime.strptime(compact.group(1), "%Y%m%dT%H%M%SZ").replace(tzinfo=timezone.utc).timestamp() * 1000)
    iso = re.search(r"(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2})(?:\.\d+)?([+-]\d{2}:\d{2}|Z)", run_id)
    if iso:
        return int(datetime.fromisoformat(f"{iso.group(1)}{'+00:00' if iso.group(2) == 'Z' else iso.group(2)}").timestamp() * 1000)
    raise ValueError(f"Could not derive a deterministic timestamp from coursework run ID {run_id!r}")


def emit_coursework_assertions_to_datahub(gms_url: str = "http://datahub-gms:8080") -> dict[str, Any]:
    run_root = _latest_successful_coursework_run()
    if run_root is None:
        return {"status": "failed", "reason": "No successful six-stage coursework pipeline run found"}

    try:
        specs = coursework_assertion_specs(run_root)
        emitter = DataHubLineageEmitter(gms_url)
        for dataset_urn, field_names in COURSEWORK_SCHEMA_TARGETS.items():
            platform_urn = "urn:li:dataPlatform:s3" if ":s3," in dataset_urn else "urn:li:dataPlatform:iceberg"
            schema_name = dataset_urn.split(",")[1].removeprefix("vina_bim_shop.")
            emitter.emit_string_schema(
                entity_urn=dataset_urn,
                platform_urn=platform_urn,
                schema_name=schema_name,
                field_names=field_names,
            )
        for spec in specs:
            emitter.emit_assertion(
                assertion_urn=f"urn:li:assertion:{spec['assertion_id']}",
                dataset_urn=str(spec["dataset_urn"]),
                assertion_type=str(spec["assertion_type"]),
                success=bool(spec["success"]),
                column=str(spec["column"]),
                run_id=str(spec["run_id"]),
                timestamp_ms=_run_timestamp_ms(str(spec["run_id"])),
            )
    except Exception as exc:
        return {"status": "failed", "reason": str(exc), "source_run_root": str(run_root)}

    return {
        "status": "success",
        "source_run_id": str(specs[0]["run_id"]),
        "source_run_root": str(run_root),
        "assertions_emitted": len(specs),
        "assertion_urns": [f"urn:li:assertion:{spec['assertion_id']}" for spec in specs],
    }


def emit_gx_assertions_to_datahub(gms_url: str = "http://datahub-gms:8080") -> dict[str, Any]:
    emitter = DataHubLineageEmitter(gms_url)
    artifacts = _find_gx_quality_artifacts()
    results: dict[str, Any] = {"files_found": len(artifacts), "assertions_emitted": 0}

    for path in artifacts:
        try:
            data = _load_gx_results(path)
            if not data:
                continue
            _emit_from_gx_data(emitter, data, results, path.name)
        except Exception as exc:
            results[f"error_{path.name}"] = str(exc)

    return results


def _emit_from_gx_data(
    emitter: DataHubLineageEmitter,
    data: dict[str, Any],
    results: dict[str, Any],
    source_name: str,
) -> None:
    for table_name, validation_results in data.get("validations", {}).items():
        dataset_urn = ice_urn(table_name)
        if not validation_results:
            continue
        for idx, result in enumerate(validation_results):
            if not isinstance(result, dict):
                continue
            expectation_info = result.get("expectation_config", {})
            exp_type = expectation_info.get("expectation_type", "unknown")
            if isinstance(exp_type, dict):
                exp_type = exp_type.get("expectation_type", "unknown")
            col = expectation_info.get("kwargs", {}).get("column", "")
            success = result.get("success", False)
            assertion_urn = f"urn:li:assertion:gx_{table_name}.{exp_type}.{col}.{idx}"
            try:
                emitter.emit_assertion(
                    assertion_urn=assertion_urn,
                    dataset_urn=dataset_urn,
                    assertion_type=exp_type,
                    success=success,
                    column=col,
                )
                results["assertions_emitted"] += 1
            except Exception:
                pass
        try:
            emitter.emit_tag(dataset_urn, "quality_gate")
        except Exception:
            pass
