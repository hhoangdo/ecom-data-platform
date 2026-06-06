from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from vina_bim_shop.datahub_lineage.emitter import DataHubLineageEmitter, ice_urn

GX_ARTIFACTS_GLOB = "evidence/08_airflow_gx/runs/hourly_batch_lakehouse/**/quality/*.json"
REPO_ROOT = Path(__file__).resolve().parents[3]


def _find_gx_quality_artifacts() -> list[Path]:
    matches = sorted(REPO_ROOT.glob("evidence/08_airflow_gx/runs/*/quality/*.json"))
    matches += sorted(REPO_ROOT.glob("evidence/05_spark_batch/gx/*.json"))
    return matches


def _load_gx_results(path: Path) -> dict[str, Any] | None:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None


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
