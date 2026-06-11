"""Shared Great Expectations and window-payload helpers.

These are used by more than one DAG runtime:

- ``_validate_pandas_dataframe`` runs an ephemeral GX context against an
  in-memory ``pandas.DataFrame`` and returns a ``ValidationReport`` that the
  policy module can interpret. Used by ``hourly_batch`` and ``reconciliation``.
- ``_render_docs`` publishes the rendered reports into the local GX Data Docs
  site. Used by ``hourly_batch``, ``reconciliation``, and ``datahub_ingestion``.
- ``_window_payload`` serialises a ``BatchWindow`` into the JSON shape that
  manifests expect. Used by ``hourly_batch`` and ``reconciliation``.

The heavy ``great_expectations`` import is kept inside
``_validate_pandas_dataframe`` to preserve the existing lazy-load contract.
"""
from __future__ import annotations

from pathlib import Path
from typing import Any

import pandas as pd

from vina_bim_shop.lakehouse.spark.window import BatchWindow
from vina_bim_shop.quality.policies import gate_outcome_for_layer
from vina_bim_shop.quality.reports import (
    ValidationReport,
    render_validation_docs,
    write_validation_report,
)

from .paths import DOCS_ROOT


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


def _window_payload(window: BatchWindow) -> dict[str, str]:
    return {
        "start_ts": window.start_ts.isoformat().replace("+00:00", "Z"),
        "end_ts": window.end_ts.isoformat().replace("+00:00", "Z"),
        "mode": window.mode,
    }
